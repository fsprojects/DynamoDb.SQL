// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Extensions
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open Amazon.DynamoDB.DataModel

[<AutoOpen>]
module LowLevel =
    let (|IsQueryReq|IsScanReq|) = function
        // handle invalid requests
        | { Select = Select([]) }   -> raise EmptySelect
        | { From = From("") }       -> raise EmptyFrom        
        // handle Query requests
        | { From = From(table); Where = Some(Where(Query(hKey, rngKeyCondition)));
            Select = Select(SelectAttributes attributes); Limit = limit }
            -> let req = new QueryRequest(ConsistentRead = true, // TODO
                                          TableName = table, HashKeyValue = hKey.ToAttributeValue(),
                                          AttributesToGet = attributes)

               // optionally set the range key condition and limit if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> req.RangeKeyCondition <- rndCond.ToCondition()
               | _ -> ()

               match limit with | Some(Limit n) -> req.Limit <- n | _ -> ()
               
               IsQueryReq req
        // handle Scan requests
        | { From = From(table); Where = Some(Where(Scan(scanFilters)));
            Select = Select(SelectAttributes attributes); Limit = limit }
            -> let scanFilters = scanFilters |> Seq.map (fun (attr, cond) -> attr, cond.ToCondition())
               let req = new ScanRequest(TableName = table, AttributesToGet = attributes,
                                         ScanFilter = new Dictionary<string, Condition>(dict scanFilters))

               match limit with | Some(Limit n) -> req.Limit <- n | _ -> ()
               IsScanReq req

    type AmazonDynamoDBClient with
        member this.QueryAsync (query : string) =
            let dynamoQuery = parseDynamoQuery query

            match dynamoQuery with
            | IsQueryReq req -> async { return! this.QueryAsync req }
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query request : %s" query)            

        member this.QueryAsyncAsTask query = this.QueryAsync query |> Async.StartAsTask
        member this.Query query = this.QueryAsync query |> Async.RunSynchronously

        member this.ScanAsync (query : string) =
            let dynamoQuery = parseDynamoQuery query
    
            match dynamoQuery with
            | IsScanReq req -> async { return! this.ScanAsync req }
            | _ -> raise <| InvalidQuery (sprintf "Not a valid scan request : %s" query)

        member this.ScanAsyncAsTask query = this.ScanAsync query |> Async.StartAsTask
        member this.Scan query = this.ScanAsync query |> Async.RunSynchronously