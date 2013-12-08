// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module Common

open System
open System.Diagnostics
open System.Collections.Generic
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model
open DynamoDbV2.SQL.Execution
open DynamoDbV2.SQL.Execution

let awsKey, awsSecret = "PUT_YOUR_AWS_KEY_HERE", "PUT_YOUR_AWS_SECRET_HERE"

let client = new AmazonDynamoDBClient(awsKey, awsSecret)
let cxt = new DynamoDBContext(client)

[<DynamoDBTable("Reply")>]
type Reply () =
    [<DynamoDBHashKey>]
    member val Id = 0 with get, set

    [<DynamoDBRangeKey>]
    member val ReplyDateTime = DateTime.MinValue with get, set
            
    member val Message = "" with get, set

    /// Local Secondary Index (PostedByIndex) is created against this property
    member val PostedBy = "" with get, set

let createTable () = 
    let mutable req = new Amazon.DynamoDBv2.Model.CreateTableRequest()
    req.TableName <- "Reply"

    let hashKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    hashKey.KeyType         <- KeyType.HASH
    hashKey.AttributeName   <- "Id"
    let rangeKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    rangeKey.KeyType        <- KeyType.RANGE
    rangeKey.AttributeName  <- "ReplyDateTime"
    req.KeySchema           <- new List<KeySchemaElement>([| hashKey; rangeKey |])
    
    let hashKeyDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    hashKeyDef.AttributeName    <- "Id"
    hashKeyDef.AttributeType    <- ScalarAttributeType.N
    let rangeKeyDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    rangeKeyDef.AttributeName   <- "ReplyDateTime"
    rangeKeyDef.AttributeType   <- ScalarAttributeType.S
    let postedByDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    postedByDef.AttributeName   <- "PostedBy"
    postedByDef.AttributeType   <- ScalarAttributeType.S
    req.AttributeDefinitions    <- new List<AttributeDefinition>([| hashKeyDef; rangeKeyDef; postedByDef |])

    let throughput = new Amazon.DynamoDBv2.Model.ProvisionedThroughput()
    throughput.ReadCapacityUnits    <- 50L
    throughput.WriteCapacityUnits   <- 50L
    req.ProvisionedThroughput       <- throughput

    let mutable index = new Amazon.DynamoDBv2.Model.LocalSecondaryIndex()
    index.IndexName <- "PosterIndex"
    
    let postedByKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    postedByKey.KeyType         <- KeyType.RANGE
    postedByKey.AttributeName   <- "PostedBy"
    index.KeySchema             <-new List<KeySchemaElement>([| hashKey; postedByKey |])

    let projection = new Amazon.DynamoDBv2.Model.Projection()
    projection.ProjectionType   <- ProjectionType.KEYS_ONLY
    index.Projection            <-projection

    req.LocalSecondaryIndexes   <- new List<LocalSecondaryIndex>([| index |])

    client.CreateTable(req) |> ignore

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

    let batchWrite = cxt.CreateBatchWrite()

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