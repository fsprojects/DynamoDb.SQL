// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open System.Runtime.CompilerServices
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Extensions
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open Amazon.DynamoDB.DataModel

[<AutoOpen>]
module LowLevel =
    let (|GetQueryReq|) (query : DynamoQuery) = 
        match query with
        | { From    = From table
            Where   = Where(QueryCondition(hKey, rngKeyCondition))
            Action  = ActionParams(isCount, attributes)
            Order   = order
            Options = opts }
            -> let req = new QueryRequest(TableName       = table, 
                                          HashKeyValue    = hKey.ToAttributeValue(),
                                          AttributesToGet = attributes,
                                          Count           = isCount)

               // optionally set the range key condition if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> req.RangeKeyCondition <- rndCond.ToCondition()
               | _ -> ()

               match order with 
               | Some(Asc)  -> req.ScanIndexForward <- true
               | Some(Desc) -> req.ScanIndexForward <- false
               | _          -> ()

               req.ConsistentRead <- isConsistentRead opts
               match tryGetQueryPageSize opts with | Some n -> req.Limit <- n | _ -> ()

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
                    let scanFilters = scanFilters |> Seq.map (fun (attr, cond) -> attr, cond.ToCondition())
                    req.ScanFilter <- new Dictionary<string, Condition>(dict scanFilters)
               | _ -> ()

               match tryGetScanPageSize opts with | Some n -> req.Limit <- n | _ -> ()
               
               req

[<AutoOpen>]
module ClientExt = 
    type AmazonDynamoDBClient with
        member this.QueryAsync (query : string) =
            let dynamoQuery = parseDynamoQuery query

            match dynamoQuery with
            | GetQueryReq req -> async { return! this.QueryAsync req }
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query request : %s" query)

        member this.Query (query : string) = this.QueryAsync(query) |> Async.RunSynchronously

        member this.ScanAsync (query : string) =
            let dynamoScan = parseDynamoScan query
    
            match dynamoScan with
            | GetScanReq req -> async { return! this.ScanAsync req }
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