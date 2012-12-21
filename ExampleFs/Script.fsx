
#r @"bin\Release\AWSSDK.dll"
#r @"bin\Release\DynamoDb.SQL.dll"

open System
open System.Linq
open Amazon.DynamoDB
open Amazon.DynamoDB.DataModel
open DynamoDb.SQL.Execution

[<DynamoDBTable("Reply")>]
type Reply () =
    [<DynamoDBHashKey>]
    member val Id  = "" with get, set

    [<DynamoDBRangeKey>]
    member val ReplyDateTime = DateTime.MinValue with get, set
            
    member val Message = "" with get, set

    member val PostedBy = "" with get, set

let client = new AmazonDynamoDBClient("PUT_YOUR_AWS_KEY_HERE", "PUT_YOUR_AWS_SECRET_HERE")
let cxt = new DynamoDBContext(client)

let getTestData () =
    [| 
        new Reply(Id = "Amazon DynamoDB#DynamoDB Thread 1",
                 ReplyDateTime = DateTime.Parse("2012-06-03T13:29:39.054Z"),
                 Message = "DynamoDB Thread 1 Reply 1 text",
                 PostedBy = "User A")
        new Reply(Id = "Amazon DynamoDB#DynamoDB Thread 1",
                  ReplyDateTime = DateTime.Parse("2012-06-10T13:29:39.317Z"),
                  Message = "DynamoDB Thread 1 Reply 2 text",
                  PostedBy = "User B")
        new Reply(Id = "Amazon DynamoDB#DynamoDB Thread 1",
                  ReplyDateTime = DateTime.Parse("2012-06-17T13:29:39.578Z"),
                  Message = "DynamoDB Thread 1 Reply 3 text",
                  PostedBy = "User B")
        new Reply(Id = "Amazon DynamoDB#DynamoDB Thread 2",
                  ReplyDateTime = DateTime.Parse("2012-06-17T13:29:39.843Z"),
                  Message = "DynamoDB Thread 2 Reply 1 text",
                  PostedBy = "User A")
        new Reply(Id = "Amazon DynamoDB#DynamoDB Thread 2",
                  ReplyDateTime = DateTime.Parse("2012-06-23T13:29:40.114Z"),
                  Message = "DynamoDB Thread 2 Reply 2 text",
                  PostedBy = "User A") 
    |]

let batchWrite = cxt.CreateBatchWrite<Reply>()
do getTestData() |> batchWrite.AddPutItems
do cxt.ExecuteBatchWrite(batchWrite)

let hashKey = "\"Amazon DynamoDB#DynamoDB Thread 1\""
let queryAllRes = client.QueryAsync("SELECT * from Reply where @HashKey = " + hashKey)
let queryCountAllRes = client.QueryAsync("COUNT * from Reply where @HashKey = " + hashKey)

// query subset
let querySubsetRes1 = Async.RunSynchronously(client.QueryAsync("select * from Reply where @HashKey = " + hashKey + " and @RangeKey >= \"2012-06-10\""))
let querySubsetRes2 = Async.RunSynchronously(client.QueryAsync("select * from Reply where @HashKey = " + hashKey + " and @RangeKey < \"2012-06-10\""))
let querySubsetRes3 = Async.RunSynchronously(client.QueryAsync("select * from Reply where @HashKey = " + hashKey + " ORDER DESC LIMIT 2"))
let querySubsetRes4 = Async.RunSynchronously(client.QueryAsync("select * from Reply where @HashKey = " + hashKey + " ORDER ASC LIMIT 2"))
let querySubsetRes5 = Async.RunSynchronously(client.QueryAsync("select * from Reply where @HashKey = " + hashKey + " LIMIT 2"))

let querySubsetRes1' = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " and @RangeKey >= \"2012-06-10\"").ToArray()
let querySubsetRes2' = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " and @RangeKey < \"2012-06-10\"").ToArray()
let querySubsetRes3' = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " ORDER DESC LIMIT 2").ToArray()
let querySubsetRes4' = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " ORDER ASC LIMIT 2").ToArray()
let querySubsetRes5' = cxt.ExecQuery<Reply>("select * from Reply where @HashKey = " + hashKey + " LIMIT 2").ToArray()

let scanRes1        = Async.RunSynchronously(client.ScanAsync("Select * from Reply where PostedBy contains \"A\""))
let scanCountRes1   = Async.RunSynchronously(client.ScanAsync("Count * from Reply where PostedBy contains \"A\""))
let scanRes2        = Async.RunSynchronously(client.ScanAsync("Select * from Reply where ReplyDateTime between \"2012-06-10\" and \"2012-06-20\""))
let scanCountRes2   = Async.RunSynchronously(client.ScanAsync("Count * from Reply where ReplyDateTime between \"2012-06-10\" and \"2012-06-20\""))

let scanRes3 = cxt.ExecScan<Reply>("Select * from Reply where PostedBy contains \"A\"").ToArray()
let scanRes4 = cxt.ExecScan<Reply>("Select * from Reply where ReplyDateTime between \"2012-06-10\" and \"2012-06-20\"").ToArray()