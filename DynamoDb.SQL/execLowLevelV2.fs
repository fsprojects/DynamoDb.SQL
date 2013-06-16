// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDbV2.SQL.Execution

open System
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
               req.ReturnConsumedCapacity <- match returnQueryConsumedCapacity opts with 
                                             | true -> "TOTAL"
                                             | _    -> "NONE"
               req.Limit <- match tryGetQueryPageSize opts with 
                            | Some n -> n 
                            | _ -> Int32.MaxValue

               req

    type ScanRequest with
        member this.SplitIntoSegments () =
            let makeSegment n =
                new ScanRequest(TableName               = this.TableName, 
                                AttributesToGet         = this.AttributesToGet,
                                Select                  = this.Select,
                                ScanFilter              = this.ScanFilter,
                                ReturnConsumedCapacity  = this.ReturnConsumedCapacity,
                                Limit                   = this.Limit,
                                TotalSegments           = this.TotalSegments,
                                Segment                 = n)

            [| 1..this.TotalSegments |] |> Array.map (fun n -> makeSegment n)

    let (|GetScanReqs|) (scan : DynamoScan) = 
        let getReq table attributes select scanFilter returnConsumedCapacity = ()

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

               req.ReturnConsumedCapacity <- match returnScanConsumedCapacity opts with 
                                             | true -> "TOTAL"
                                             | _    -> "NONE"
               match tryGetScanPageSize opts with | Some n -> req.Limit <- n | _ -> ()
               req.TotalSegments <- getScanSegments opts

               req.SplitIntoSegments()

    /// Merges a QueryResponse into an aggregate QueryResponse
    let mergeQueryResponses (res : QueryResponse) (aggrRes : QueryResponse option) = 
        match aggrRes with
        | Some aggr ->
            aggr.QueryResult.Items.AddRange(res.QueryResult.Items)
            aggr.QueryResult.Count                          <- aggr.QueryResult.Count + res.QueryResult.Count

            // in the V2 API, we MIGHT NOT get any consumed capacity back if the NoReturnedCapacity 
            // query option is specified, so need to handle that case
            match aggr.QueryResult.ConsumedCapacity, res.QueryResult.ConsumedCapacity with
            | null, null | _, null -> ()
            | null, x -> aggr.QueryResult.ConsumedCapacity  <- x
            | x, y    -> x.CapacityUnits                    <- x.CapacityUnits + y.CapacityUnits

            aggr.QueryResult.LastEvaluatedKey               <- res.QueryResult.LastEvaluatedKey
            aggr.ResponseMetadata                           <- res.ResponseMetadata
            aggr
        | _ -> res
        
    /// Merges a ScanResponse into an aggregate ScanResponse
    let mergeScanResponses maxResults (aggrRes : ScanResponse) (res : ScanResponse) = 
        match res.ScanResult.Items with
        | null -> ()
        | lst  -> 
            // respect the max number of results we want to return from this new response
            // IEnumerable.Take is used here instead of Seq.take because Seq.take excepts
            // when there are insufficient number of items which is not desirable here
            let newItems = lst.Take maxResults |> Seq.toArray
            aggrRes.ScanResult.Items.AddRange(newItems)

        let newCount = min maxResults res.ScanResult.Count
        aggrRes.ScanResult.Count                            <- aggrRes.ScanResult.Count + newCount

        // in the V2 API, we MIGHT NOT get any consumed capacity back if the NoReturnedCapacity 
        // query option is specified, so need to handle that case
        match aggrRes.ScanResult.ConsumedCapacity, res.ScanResult.ConsumedCapacity with
        | null, null | _, null -> ()
        | null, x -> aggrRes.ScanResult.ConsumedCapacity    <- x
        | x, y    -> x.CapacityUnits                        <- x.CapacityUnits + y.CapacityUnits

        aggrRes.ScanResult.ScannedCount                    <- aggrRes.ScanResult.ScannedCount + res.ScanResult.ScannedCount
        aggrRes.ScanResult.LastEvaluatedKey                <- res.ScanResult.LastEvaluatedKey
        aggrRes.ResponseMetadata                           <- res.ResponseMetadata
        aggrRes

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
    let rec scanLoop (client : AmazonDynamoDBClient) maxResults (aggrRes : ScanResponse option) (req : ScanRequest) =
        async {
            // don't set the limit using the maxResults for a scan, because unlike a query, limit is the max number of
            // items scanned rather than max number of items to return
            let! res = client.ScanAsync req

            let aggrRes = match aggrRes with 
                          | Some aggrRes -> mergeScanResponses maxResults aggrRes res
                          | _            -> res

            match res.ScanResult.LastEvaluatedKey with
            | null -> return aggrRes
            // short circuit if we have managed to find as many results as we wanted
            | _ when res.ScanResult.Count >= maxResults
                   -> return aggrRes
            | key  -> req.ExclusiveStartKey <- key
                      return! scanLoop client (maxResults - res.ScanResult.Count) (Some aggrRes) req 
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
                -> queryLoop this Int32.MaxValue req None
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query request : %s" query)

        member this.Query (query : string) = this.QueryAsync(query) |> Async.RunSynchronously

        member this.ScanAsync (query : string) =
            let dynamoScan  = parseDynamoScanV2 query
            let maxResults  = match dynamoScan.Limit with | Some(Limit n) -> n | _ -> Int32.MaxValue
    
            let scanReqs    = match dynamoScan with 
                              | GetScanReqs reqs -> reqs 
                              | _ -> raise <| InvalidScan (sprintf "Not a valid scan request : %s" query)
            
            async {
                let! scanResponses = scanReqs |> Array.map (scanLoop this maxResults None) |> Async.Parallel
                return scanResponses |> Array.reduce (mergeScanResponses maxResults)
            }
            
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