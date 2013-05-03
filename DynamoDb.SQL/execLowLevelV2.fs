// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDbV2.SQL.Execution

open System.Collections.Generic
open System.Linq
open System.Runtime.CompilerServices
open DynamoDb.SQL
open DynamoDb.SQL.Extensions
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.DynamoDBv2.DataModel

[<AutoOpen>]
module LowLevel =
    let (|GetQueryReq|) (query : DynamoQuery) = 
        match query with
        | { From    = From table
            Where   = Where(QueryV2Condition keyConditions)
            Action  = ActionParams(isCount, attributes)
            Order   = order
            Options = opts }
            -> let req = new QueryRequest(TableName = table, AttributesToGet = attributes)

               let allAttributes =  
                    match tryGetQueryIndex opts with
                    | Some(idxName, allAttributes) 
                        -> req.IndexName <- idxName
                           allAttributes
                    | _ -> true

               // you cannot specify both AttributesToGet and SPECIFIC_ATTRIBUTES in Select
               // for more details, see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
               req.Select <- match isCount, attributes, allAttributes with
                             | true, _, _         -> "COUNT"
                             | false, null, false -> "ALL_PROJECTED_ATTRIBUTES"
                             | false, null, true  -> "ALL_ATTRIBUTES"
                             | false, _, _        -> "SPECIFIC_ATTRIBUTES"

               keyConditions |> List.iter (fun (attrName, keyCond) -> req.KeyConditions.Add(attrName, keyCond.ToConditionV2()))

               match order with 
               | Some(Asc)  -> req.ScanIndexForward <- true
               | Some(Desc) -> req.ScanIndexForward <- false
               | _          -> ()

               req.ConsistentRead <- isConsistentRead opts
               req.ReturnConsumedCapacity <- match returnConsumedCapacity opts with 
                                             | true -> "TOTAL"
                                             | _    -> "NONE"
               match tryGetQueryPageSize opts with | Some n -> req.Limit <- n | _ -> ()

               req

    let (|GetScanReq|) (scan : DynamoScan) = 
        match scan with
        | { From    = From table
            Where   = where
            Action  = ActionParams(isCount, attributes)
            Options = opts }
            -> let req = new ScanRequest(TableName = table, AttributesToGet = attributes)

               // you cannot specify both AttributesToGet and SPECIFIC_ATTRIBUTES in Select
               // for more details, see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
               req.Select <- match isCount, attributes with
                             | true, _         -> "COUNT"
                             | false, null     -> "ALL_ATTRIBUTES"
                             | false, _        -> "SPECIFIC_ATTRIBUTES"

               // optionally set the scan filters and limit
               match where with 
               | Some(Where(ScanCondition scanFilters)) -> 
                    let scanFilters = scanFilters |> Seq.map (fun (attr, cond) -> attr, cond.ToConditionV2())
                    req.ScanFilter <- new Dictionary<string, Condition>(dict scanFilters)
               | _ -> ()

               match tryGetScanPageSize opts with | Some n -> req.Limit <- n | _ -> ()
               
               req

    /// Merges a QueryResponse into an aggregate QueryResponse
    let mergeQueryResponses (res : QueryResponse) (aggrRes : QueryResponse option) = 
        match aggrRes with
        | Some aggr ->
            aggr.QueryResult.Items.AddRange(res.QueryResult.Items)
            aggr.QueryResult.Count                          <- aggr.QueryResult.Count + res.QueryResult.Count
            aggr.QueryResult.ConsumedCapacity.CapacityUnits <- aggr.QueryResult.ConsumedCapacity.CapacityUnits + res.QueryResult.ConsumedCapacity.CapacityUnits
            aggr.QueryResult.LastEvaluatedKey               <- res.QueryResult.LastEvaluatedKey
            aggr.ResponseMetadata.RequestId                 <- res.ResponseMetadata.RequestId
            aggr
        | _ -> res
        
    /// Merges a ScanResponse into an aggregate ScanResponse
    let mergeScanResponses (res : ScanResponse) maxResults (aggrRes : ScanResponse option) = 
        match aggrRes with
        | Some aggr ->
            // respect the max number of results we want to return from this new response
            // IEnumerable.Take is used here instead of Seq.take because Seq.take excepts
            // when there are insufficient number of items which is not desirable here
            let newItems = res.ScanResult.Items.Take maxResults |> Seq.toArray

            aggr.ScanResult.Items.AddRange(newItems)
            aggr.ScanResult.Count                 <- aggr.ScanResult.Count + newItems.Length
            aggr.ScanResult.ConsumedCapacity.CapacityUnits <- aggr.ScanResult.ConsumedCapacity.CapacityUnits + res.ScanResult.ConsumedCapacity.CapacityUnits
            aggr.ScanResult.ScannedCount          <- aggr.ScanResult.ScannedCount + res.ScanResult.ScannedCount
            aggr.ScanResult.LastEvaluatedKey      <- res.ScanResult.LastEvaluatedKey
            aggr.ResponseMetadata.RequestId       <- res.ResponseMetadata.RequestId
            aggr
        | _ -> res

    /// Recursively make query requests and merge results into an aggregate response
    let rec queryLoop (client : AmazonDynamoDBClient) maxResults (req : QueryRequest) (aggrRes : QueryResponse option) =
        async {
            // the request limit (max number of items to return) should not exceed the max number of results we want
            // to return in total
            req.Limit <- min maxResults req.Limit
            let! res = client.QueryAsync req

            let aggrRes = mergeQueryResponses res aggrRes

            match res.QueryResult.LastEvaluatedKey with
            | null -> return aggrRes
            | _    -> req.ExclusiveStartKey <- res.QueryResult.LastEvaluatedKey
                      return! queryLoop client (maxResults - res.QueryResult.Count) req (Some aggrRes)
        }

    /// Recursively make scan requests and merge results into an aggregate response
    /// NOTE: there are subtle differences with the way 'Limit' works in Scan and Query operations, hence
    /// why the two separate loop functions.
    /// In short, 'Limit' in Query terms defines the max number of items to return, but in Scan terms it
    /// defines the max number of items to evaluate (page size)
    /// For more details, please refer to the official API doc:
    /// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/API_Query.html
    /// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/API_Scan.html
    let rec scanLoop (client : AmazonDynamoDBClient) maxResults (req : ScanRequest) (aggrRes : ScanResponse option) =
        async {
            let! res = client.ScanAsync req

            let aggrRes = mergeScanResponses res maxResults aggrRes

            match res.ScanResult.LastEvaluatedKey with
            | null -> return aggrRes
            // short circuit if we have managed to find as many results as we wanted
            | _ when res.ScanResult.Count >= maxResults
                   -> return aggrRes
            | _    -> req.ExclusiveStartKey <- res.ScanResult.LastEvaluatedKey
                      return! scanLoop client (maxResults - res.ScanResult.Count) req (Some aggrRes)
        }

[<AutoOpen>]
module ClientExt = 
    type AmazonDynamoDBClient with
        member this.QueryAsync (query : string) =
            let dynamoQuery = parseDynamoQueryV2 query

            match dynamoQuery with
            | { Limit = Some(Limit n) } & GetQueryReq req 
                -> queryLoop this n req None
            | GetQueryReq req 
                -> async { return! this.QueryAsync req }
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query request : %s" query)

        member this.Query (query : string) = this.QueryAsync(query) |> Async.RunSynchronously

        member this.ScanAsync (query : string) =
            let dynamoScan = parseDynamoScanV2 query
    
            match dynamoScan with
            | { Limit = Some(Limit n) } & GetScanReq req
                -> scanLoop this n req None
            | GetScanReq req 
                -> async { return! this.ScanAsync req }
            | _ -> raise <| InvalidScan (sprintf "Not a valid scan request : %s" query)

        member this.Scan (query : string) = this.ScanAsync(query) |> Async.RunSynchronously

[<Extension>]
[<AbstractClass>]
[<Sealed>]
type AmazonDynamoDBClientExt =
    [<Extension>]
    static member QueryAsyncAsTask (clt : AmazonDynamoDBClient, query : string) = clt.QueryAsync(query) |> Async.StartAsTask

    [<Extension>]
    static member Query (clt : AmazonDynamoDBClient, query : string) = clt.Query(query)

    [<Extension>]
    static member ScanAsyncAsTask (clt : AmazonDynamoDBClient, query : string) = clt.ScanAsync(query) |> Async.StartAsTask
    
    [<Extension>]
    static member Scan (clt : AmazonDynamoDBClient, query : string) = clt.Scan(query)