// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System
open System.Collections.Generic
open System.Linq
open System.Runtime.CompilerServices
open DynamoDb.SQL
open DynamoDb.SQL.Extensions
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open Amazon.DynamoDB.DataModel

[<AutoOpen>]
module LowLevel =
    let (|GetQueryReq|) (query : DynamoQuery) = 
        match query with
        | { From    = From table
            Where   = Where(QueryV1Condition(hKey, rngKeyCondition))
            Action  = ActionParams(isCount, attributes)
            Order   = order
            Options = opts }
            -> let req = new QueryRequest(TableName       = table, 
                                          HashKeyValue    = hKey.ToAttributeValueV1(),
                                          AttributesToGet = attributes,
                                          Count           = isCount)

               // optionally set the range key condition if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> req.RangeKeyCondition <- rndCond.ToConditionV1()
               | _ -> ()

               match order with 
               | Some(Asc)  -> req.ScanIndexForward <- true
               | Some(Desc) -> req.ScanIndexForward <- false
               | _          -> ()

               req.ConsistentRead <- isConsistentRead opts
               req.Limit <- match tryGetQueryPageSize opts with 
                            | Some n -> n 
                            | _ -> Int32.MaxValue

               req

    let (|GetScanReq|) (scan : DynamoScan) = 
        match scan with
        | { From    = From table
            Where   = where
            Action  = ActionParams(isCount, attributes)
            Options = opts }
            -> let req = new ScanRequest(TableName       = table, 
                                         AttributesToGet = attributes,
                                         Count           = isCount)

               // optionally set the scan filters and limit
               match where with 
               | Some(Where(ScanCondition scanFilters)) -> 
                    let scanFilters = scanFilters |> Seq.map (fun (attr, cond) -> attr, cond.ToConditionV1())
                    req.ScanFilter <- new Dictionary<string, Condition>(dict scanFilters)
               | _ -> ()

               match tryGetScanPageSize opts with | Some n -> req.Limit <- n | _ -> ()
               
               req

    /// Merges a QueryResponse into an aggregate QueryResponse
    let mergeQueryResponses (res : QueryResponse) (aggrRes : QueryResponse option) = 
        match aggrRes with
        | Some aggr ->
            aggr.QueryResult.Items.AddRange(res.QueryResult.Items)
            aggr.QueryResult.Count                 <- aggr.QueryResult.Count + res.QueryResult.Count
            aggr.QueryResult.ConsumedCapacityUnits <- aggr.QueryResult.ConsumedCapacityUnits + res.QueryResult.ConsumedCapacityUnits
            aggr.QueryResult.LastEvaluatedKey      <- res.QueryResult.LastEvaluatedKey
            aggr.ResponseMetadata                  <- res.ResponseMetadata
            aggr
        | _ -> res
        
    /// Merges a ScanResponse into an aggregate ScanResponse
    let mergeScanResponses (res : ScanResponse) maxResults (aggrRes : ScanResponse option) = 
        let aggr = defaultArg aggrRes (new ScanResponse())
        
        match res.ScanResult.Items with
        | null -> ()
        | lst  -> 
            // respect the max number of results we want to return from this new response
            // IEnumerable.Take is used here instead of Seq.take because Seq.take excepts
            // when there are insufficient number of items which is not desirable here
            let newItems = lst.Take maxResults |> Seq.toArray
            aggr.ScanResult.Items.AddRange(newItems)
        
        let newCount = min maxResults res.ScanResult.Count
        aggr.ScanResult.Count                 <- aggr.ScanResult.Count + newCount

        aggr.ScanResult.ConsumedCapacityUnits <- aggr.ScanResult.ConsumedCapacityUnits + res.ScanResult.ConsumedCapacityUnits
        aggr.ScanResult.ScannedCount          <- aggr.ScanResult.ScannedCount + res.ScanResult.ScannedCount
        aggr.ScanResult.LastEvaluatedKey      <- res.ScanResult.LastEvaluatedKey
        aggr.ResponseMetadata                 <- res.ResponseMetadata
        aggr

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
            // short circuit if we have managed to find as many results as we wanted
            | _ when res.QueryResult.Count >= maxResults
                   -> return aggrRes
            | key  -> req.ExclusiveStartKey <- key
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
            // don't set the limit using the maxResults for a scan, because unlike a query, limit is the max number of
            // items scanned rather than max number of items to return
            let! res = client.ScanAsync req

            let aggrRes = mergeScanResponses res maxResults aggrRes

            match res.ScanResult.LastEvaluatedKey with
            | null -> return aggrRes
            // short circuit if we have managed to find as many results as we wanted
            | _ when res.ScanResult.Count >= maxResults
                   -> return aggrRes
            | key  -> req.ExclusiveStartKey <- key
                      return! scanLoop client (maxResults - res.ScanResult.Count) req (Some aggrRes)
        }

[<AutoOpen>]
module ClientExt = 
    type AmazonDynamoDBClient with
        member this.QueryAsync (query : string) =
            let dynamoQuery = parseDynamoQueryV1 query

            match dynamoQuery with
            | { Limit = Some(Limit n) } & GetQueryReq req 
                -> queryLoop this n req None
            | GetQueryReq req 
                -> queryLoop this Int32.MaxValue req None
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query request : %s" query)

        member this.Query (query : string) = this.QueryAsync(query) |> Async.RunSynchronously

        member this.ScanAsync (query : string) =
            let dynamoScan = parseDynamoScanV1 query
    
            match dynamoScan with
            | { Limit = Some(Limit n) } & GetScanReq req
                -> scanLoop this n req None
            | GetScanReq req 
                -> scanLoop this Int32.MaxValue req None
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