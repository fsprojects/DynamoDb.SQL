
#r @"bin\Release\AWSSDK.dll"
#r @"bin\Release\DynamoDb.SQL.dll"

open Amazon.DynamoDB
open Amazon.DynamoDB.DataModel
open DynamoDb.SQL.Execution

let client = new AmazonDynamoDBClient("key", "secret")
let cxt = new DynamoDBContext(client)

let hashKey = "\"Amazon DynamoDB#DynamoDB Thread 1\""
let queryAllRes = client.QueryAsync("SELECT * from Reply where @HashKey = " + hashKey)
let queryCountAllRes = client.QueryAsync("COUNT * from Reply where @HashKey = " + hashKey)
