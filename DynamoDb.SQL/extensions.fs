namespace DynamoDb.SQL.Extensions

type GetItemRequestV1 = Amazon.DynamoDB.Model.GetItemRequest
type GetItemRequestV2 = Amazon.DynamoDBv2.Model.GetItemRequest

type BatchGetItemRequestV1 = Amazon.DynamoDB.Model.BatchGetItemRequest
type BatchGetItemRequestV2 = Amazon.DynamoDBv2.Model.BatchGetItemRequest

type QueryRequestV1 = Amazon.DynamoDB.Model.QueryRequest
type QueryRequestV2 = Amazon.DynamoDBv2.Model.QueryRequest

type ScanRequestV1 = Amazon.DynamoDB.Model.ScanRequest
type ScanRequestV2 = Amazon.DynamoDBv2.Model.ScanRequest

[<AutoOpen>]
module AsyncExtensions =
    type Amazon.DynamoDB.AmazonDynamoDBClient with
        member this.GetItemAsync (req : GetItemRequestV1) = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)
        member this.BatchGetItemAsync (req : BatchGetItemRequestV1) = Async.FromBeginEnd(req, this.BeginBatchGetItem, this.EndBatchGetItem)    
        member this.QueryAsync (req : QueryRequestV1) = Async.FromBeginEnd(req, this.BeginQuery, this.EndQuery)
        member this.ScanAsync (req : ScanRequestV1) = Async.FromBeginEnd(req, this.BeginScan, this.EndScan)

    type Amazon.DynamoDBv2.AmazonDynamoDBClient with
        member this.GetItemAsync (req : GetItemRequestV2) = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)
        member this.BatchGetItemAsync (req : BatchGetItemRequestV2) = Async.FromBeginEnd(req, this.BeginBatchGetItem, this.EndBatchGetItem)    
        member this.QueryAsync (req : QueryRequestV2) = Async.FromBeginEnd(req, this.BeginQuery, this.EndQuery)
        member this.ScanAsync (req : ScanRequestV2) = Async.FromBeginEnd(req, this.BeginScan, this.EndScan)