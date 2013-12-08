// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Extensions

open Amazon.DynamoDBv2.Model

[<AutoOpen>]
module AsyncExtensions =
    type Amazon.DynamoDBv2.AmazonDynamoDBClient with
        member this.GetItemAsync (req : GetItemRequest) = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)
        member this.BatchGetItemAsync (req : BatchGetItemRequest) = Async.FromBeginEnd(req, this.BeginBatchGetItem, this.EndBatchGetItem)    
        member this.QueryAsync (req : QueryRequest) = Async.FromBeginEnd(req, this.BeginQuery, this.EndQuery)
        member this.ScanAsync (req : ScanRequest) = Async.FromBeginEnd(req, this.BeginScan, this.EndScan)