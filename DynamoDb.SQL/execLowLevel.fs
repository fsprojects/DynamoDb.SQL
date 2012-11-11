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

exception EmptySelect
exception EmptyFrom

[<AutoOpen>]
module LowLevel =
    /// Active pattern to get a GetItem/BatchGetItem/Query/Scan request object for a DynamoQuery
    let (|IsGetItemReq|IsQueryReq|IsScanReq|) = function
        | { Select = Select([]) }   -> raise EmptySelect
        | { From = From("") }       -> raise EmptyFrom
        | { From = From(table); Where = Some(Where(GetByKey(key)));
            Select = Select(SelectAttributes attributes) }
            -> let req = new GetItemRequest(ConsistentRead = true, // TODO
                                            Key = key, TableName = table,
                                            AttributesToGet = attributes)
               IsGetItemReq req
        | { From = From(table); Where = Some(Where(Query(hKey, rKeyCondition)));
            Select = Select(SelectAttributes attributes); Limit = None }
            -> let req = new QueryRequest(ConsistentRead = true, // TODO
                                          TableName = table, HashKeyValue = hKey,
                                          RangeKeyCondition = rKeyCondition.ToCondition())
               IsQueryReq req
        | { From = From(table); Where = Some(Where(Query(hKey, rKeyCondition)));
            Select = Select(SelectAttributes attributes); Limit = Some(Limit(limit)) }
            -> let req = new QueryRequest(ConsistentRead = true, // TODO
                                          TableName = table, HashKeyValue = hKey, Limit = limit,
                                          RangeKeyCondition = rKeyCondition.ToCondition())
               IsQueryReq req
        | { From = From(table); Where = Some(Where(Scan));
            Select = Select(SelectAttributes attributes); Limit = None }
            -> let req = new ScanRequest()
               IsScanReq req
        | { From = From(table); Where = Some(Where(Scan));
            Select = Select(SelectAttributes attributes); Limit = Some(Limit(limit)) }
            -> let req = new ScanRequest(Limit = limit)
               IsScanReq req

    type AmazonDynamoDBClient with
        member this.ExecQuery (query : string) =
            let dynamoQuery = parseDynamoQuery query
    
            async {
                match dynamoQuery with
                | IsGetItemReq req
                    -> let! res = this.GetItemAsync req
                       let getItemResult = res.GetItemResult
                       return seq { yield getItemResult.Item }
                | IsQueryReq req
                    -> let! res = this.QueryAsync req
                       let queryRes = res.QueryResult
                       return seq { yield! queryRes.Items }
                | IsScanReq req
                    -> let! res = this.ScanAsync req
                       let scanRes = res.ScanResult
                       return seq { yield! scanRes.Items }
            } |> Async.RunSynchronously