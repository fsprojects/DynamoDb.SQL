// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module Common

open System
open System.Diagnostics
open Amazon.DynamoDB
open Amazon.DynamoDB.DataModel
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open DynamoDb.SQL.Execution
open DynamoDbV2.SQL.Execution

type AmazonDynamoDBClientV1 = Amazon.DynamoDB.AmazonDynamoDBClient
type DynamoDBContextV1      = Amazon.DynamoDB.DataModel.DynamoDBContext

type AmazonDynamoDBClientV2 = Amazon.DynamoDBv2.AmazonDynamoDBClient
type DynamoDBContextV2      = Amazon.DynamoDBv2.DataModel.DynamoDBContext

type DynamoDBTableV1        = Amazon.DynamoDB.DataModel.DynamoDBTableAttribute
type DynamoDBHashKeyV1      = Amazon.DynamoDB.DataModel.DynamoDBHashKeyAttribute
type DynamoDBRangeKeyV1     = Amazon.DynamoDB.DataModel.DynamoDBRangeKeyAttribute

type DynamoDBTableV2        = Amazon.DynamoDBv2.DataModel.DynamoDBTableAttribute
type DynamoDBHashKeyV2      = Amazon.DynamoDBv2.DataModel.DynamoDBHashKeyAttribute
type DynamoDBRangeKeyV2     = Amazon.DynamoDBv2.DataModel.DynamoDBRangeKeyAttribute

let awsKey, awsSecret = "PUT_YOUR_AWS_KEY_HERE", "PUT_YOUR_AWS_SECRET_HERE"

let clientV1 = new AmazonDynamoDBClientV1(awsKey, awsSecret)
let cxtV1 = new DynamoDBContextV1(clientV1)

let clientV2 = new AmazonDynamoDBClientV2(awsKey, awsSecret)
let cxtV2 = new DynamoDBContextV2(clientV2)

[<DynamoDBTableV1("Reply")>]
[<DynamoDBTableV2("Reply")>]
type Reply () =
    [<DynamoDBHashKeyV1>]
    [<DynamoDBHashKeyV2>]
    member val Id = 0 with get, set

    [<DynamoDBRangeKeyV1>]
    [<DynamoDBRangeKeyV2>]
    member val ReplyDateTime = DateTime.MinValue with get, set
            
    member val Message = "" with get, set

    /// Local Secondary Index (PostedByIndex) is created against this property
    member val PostedBy = "" with get, set

let createTable () = 
    let mutable req = new Amazon.DynamoDBv2.Model.CreateTableRequest()
    req <- req.WithTableName "Reply"

    let hashKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    hashKey.KeyType         <- "HASH"
    hashKey.AttributeName   <- "Id"
    let rangeKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    rangeKey.KeyType        <- "RANGE"
    rangeKey.AttributeName  <- "ReplyDateTime"
    req <- req.WithKeySchema(hashKey, rangeKey)
    
    let hashKeyDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    hashKeyDef.AttributeName    <- "Id"
    hashKeyDef.AttributeType    <- "N"
    let rangeKeyDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    rangeKeyDef.AttributeName   <- "ReplyDateTime"
    rangeKeyDef.AttributeType   <- "S"
    let postedByDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    postedByDef.AttributeName   <- "PostedBy"
    postedByDef.AttributeType   <- "S"
    req <- req.WithAttributeDefinitions(hashKeyDef, rangeKeyDef, postedByDef)

    let throughput = new Amazon.DynamoDBv2.Model.ProvisionedThroughput()
    throughput.ReadCapacityUnits    <- 50L
    throughput.WriteCapacityUnits   <- 50L
    req <- req.WithProvisionedThroughput(throughput)

    let mutable index = new Amazon.DynamoDBv2.Model.LocalSecondaryIndex()
    index.IndexName <- "PosterIndex"
    
    let postedByKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    postedByKey.KeyType        <- "RANGE"
    postedByKey.AttributeName  <- "PostedBy"
    index <- index.WithKeySchema(hashKey, postedByKey)

    let projection = new Amazon.DynamoDBv2.Model.Projection()
    projection.ProjectionType  <- "KEYS_ONLY"
    index <- index.WithProjection projection

    req <- req.WithLocalSecondaryIndexes(index)

    clientV2.CreateTable(req) |> ignore

let seedData () =
    let getPoster =
        let rand = new Random(int DateTime.UtcNow.Ticks)
        let posters = [| "John"; "Yan"; "Sean"; "Ben"; "James"; "Michael"; "Rita" |]
        printfn "Created posters"

        fun () -> posters.[rand.Next(posters.Length)]

    let replies = seq { 1..1000 } |> Seq.collect (fun id ->
        seq { 1..10 } |> Seq.map (fun offset ->
            new Reply(Id = id, 
                      ReplyDateTime = DateTime.UtcNow.AddDays(-1.0 * float offset), 
                      Message = "Test", 
                      PostedBy = getPoster())))

    let batchWrite = cxtV2.CreateBatchWrite()

    printfn "Adding 10,000 replies..."

    batchWrite.AddPutItems(replies)

    printfn "Executing batch write..."

    batchWrite.Execute()

    printfn "Batch write completed..."

let assertThat conditionMet errorMsg =
    if not conditionMet then failwithf "%s" errorMsg

let time f =
    let stopwatch = new Stopwatch()
    stopwatch.Start()
    f()
    stopwatch.Stop()
    printfn "Execution took %A" stopwatch.Elapsed