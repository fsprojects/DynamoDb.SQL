namespace DynamoDb.SQL.Extensions

open Amazon.DynamoDB
open Amazon.DynamoDB.Model

[<AutoOpen>]
module AsyncExtensions =
    type AmazonDynamoDBClient with
        member this.GetItemAsync (req : GetItemRequest) = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)
        member this.BatchGetItemAsync (req : BatchGetItemRequest) = Async.FromBeginEnd(req, this.BeginBatchGetItem, this.EndBatchGetItem)    
        member this.QueryAsync (req : QueryRequest) = Async.FromBeginEnd(req, this.BeginQuery, this.EndQuery)
        member this.ScanAsync (req : ScanRequest) = Async.FromBeginEnd(req, this.BeginScan, this.EndScan)