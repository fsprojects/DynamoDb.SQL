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
            Select  = Select(SelectAttributes attributes)
            Limit   = limit
            Order   = order }
            -> let req = new QueryRequest(ConsistentRead = true, // TODO
                                          TableName = table, HashKeyValue = hKey.ToAttributeValue(),
                                          AttributesToGet = attributes)

               // optionally set the range key condition and limit if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> req.RangeKeyCondition <- rndCond.ToCondition()
               | _ -> ()

               match limit with | Some(Limit n) -> req.Limit <- n | _ -> ()
               match order with 
               | Some(Asc)  -> req.ScanIndexForward <- true
               | Some(Desc) -> req.ScanIndexForward <- false
               | _          -> ()
               
               req

    let (|GetScanReq|) (scan : DynamoScan) = 
        match scan with
        | { From    = From table
            Where   = where
            Select  = Select(SelectAttributes attributes) 
            Limit   = limit }
            -> let req = new ScanRequest(TableName = table, AttributesToGet = attributes)

               // optionally set the scan filters and limit
               match where with 
               | Some(Where(ScanCondition scanFilters)) -> 
                    let scanFilters = scanFilters |> Seq.map (fun (attr, cond) -> attr, cond.ToCondition())
                    req.ScanFilter <- new Dictionary<string, Condition>(dict scanFilters)
               | _ -> ()

               match limit with | Some(Limit n) -> req.Limit <- n | _ -> ()
               
               req

[<Extension>]
[<AbstractClass>]
[<Sealed>]
type AmazonDynamoDBClientExt =
    [<Extension>]
    static member QueryAsync (clt : AmazonDynamoDBClient, query : string) =
        let dynamoQuery = parseDynamoQuery query

        match dynamoQuery with
        | GetQueryReq req -> async { return! clt.QueryAsync req }
        | _ -> raise <| InvalidQuery (sprintf "Not a valid query request : %s" query)            

    [<Extension>]
    static member QueryAsyncAsTask (clt : AmazonDynamoDBClient, query : string) = 
        AmazonDynamoDBClientExt.QueryAsync(clt, query) |> Async.StartAsTask

    [<Extension>]
    static member Query (clt : AmazonDynamoDBClient, query : string) = 
        AmazonDynamoDBClientExt.QueryAsync(clt, query) |> Async.RunSynchronously

    [<Extension>]
    static member ScanAsync (clt : AmazonDynamoDBClient, query : string) =
        let dynamoScan = parseDynamoScan query
    
        match dynamoScan with
        | GetScanReq req -> async { return! clt.ScanAsync req }
        | _ -> raise <| InvalidScan (sprintf "Not a valid scan request : %s" query)

    [<Extension>]
    static member ScanAsyncAsTask (clt : AmazonDynamoDBClient, query : string) = 
        AmazonDynamoDBClientExt.ScanAsync(clt, query) |> Async.StartAsTask
    
    [<Extension>]
    static member Scan (clt : AmazonDynamoDBClient, query : string) = 
        AmazonDynamoDBClientExt.ScanAsync(clt, query) |> Async.RunSynchronously