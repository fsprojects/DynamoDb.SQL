// Author : Yan Cui (twitter @theburningmonk)

// Email  : theburningmonk@gmail.com
// Blog   : http://theburningmonk.com

module Common

open System
open System.Diagnostics
open System.Collections.Generic
open Amazon
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model
open DynamoDbV2.SQL.Execution

let awsKey      = "AWS-KEY"
let awsSecret   = "AWS-SECRET"
let region      = RegionEndpoint.USEast1

let client = new AmazonDynamoDBClient(awsKey, awsSecret, region)
let cxt = new DynamoDBContext(client)

[<DynamoDBTable("GameScores")>]
type GameScore () =
    [<DynamoDBHashKey>]  member val UserId    = "" with get, set
    [<DynamoDBRangeKey>] member val GameTitle = "" with get, set
            
    member val TopScore         = 0 with get, set
    member val TopScoreDateTime = DateTime.MinValue with get, set
    member val Wins             = 0 with get, set
    member val Losses           = 0 with get, set

let createTable () = 
    let mutable req = new Amazon.DynamoDBv2.Model.CreateTableRequest()
    req.TableName <- "GameScores"

    let hashKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    hashKey.KeyType         <- KeyType.HASH
    hashKey.AttributeName   <- "UserId"
    let rangeKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    rangeKey.KeyType        <- KeyType.RANGE
    rangeKey.AttributeName  <- "GameTitle"
    req.KeySchema           <- new List<KeySchemaElement>([| hashKey; rangeKey |])
    
    let hashKeyDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    hashKeyDef.AttributeName    <- "UserId"
    hashKeyDef.AttributeType    <- ScalarAttributeType.S
    let rangeKeyDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    rangeKeyDef.AttributeName   <- "GameTitle"
    rangeKeyDef.AttributeType   <- ScalarAttributeType.S
    let topScoreDef = new Amazon.DynamoDBv2.Model.AttributeDefinition()
    topScoreDef.AttributeName   <- "TopScore"
    topScoreDef.AttributeType   <- ScalarAttributeType.N

    req.AttributeDefinitions    <- new List<AttributeDefinition>([| hashKeyDef; rangeKeyDef; topScoreDef |])

    let throughput = new Amazon.DynamoDBv2.Model.ProvisionedThroughput()
    throughput.ReadCapacityUnits    <- 25L
    throughput.WriteCapacityUnits   <- 25L
    req.ProvisionedThroughput       <- throughput

    let mutable lsi = new Amazon.DynamoDBv2.Model.LocalSecondaryIndex()
    lsi.IndexName                   <- "TopScoreIndex"
    
    let topScoreKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    topScoreKey.KeyType             <- KeyType.RANGE
    topScoreKey.AttributeName       <- "TopScore"
    lsi.KeySchema                   <- new List<KeySchemaElement>([| hashKey; topScoreKey |])

    let lsiProjection = new Amazon.DynamoDBv2.Model.Projection()
    lsiProjection.ProjectionType    <- ProjectionType.KEYS_ONLY
    lsi.Projection                  <- lsiProjection

    req.LocalSecondaryIndexes.Add(lsi)

    let mutable gsi = new Amazon.DynamoDBv2.Model.GlobalSecondaryIndex()
    gsi.IndexName                   <- "GameTitleIndex"

    let gameTitleKey = new Amazon.DynamoDBv2.Model.KeySchemaElement()
    gameTitleKey.KeyType            <- KeyType.HASH
    gameTitleKey.AttributeName      <- "GameTitle"
    gsi.KeySchema                   <- new List<KeySchemaElement>([| gameTitleKey; topScoreKey |])
    
    let gsiProjection = new Amazon.DynamoDBv2.Model.Projection()
    gsiProjection.ProjectionType    <- ProjectionType.KEYS_ONLY
    gsi.Projection                  <- gsiProjection

    gsi.ProvisionedThroughput       <- throughput

    req.GlobalSecondaryIndexes.Add(gsi)
    client.CreateTable(req) |> ignore

let alienAdventure = "Alien Adventure"
let attackShips    = "Attack Ships"
let galaxyInvaders = "Galaxy Invaders"
let meteorBlasters = "Meteor Blasters"
let starshipX      = "Starship X"

let gameTitles = [| alienAdventure; attackShips; galaxyInvaders; meteorBlasters; starshipX |]

let seedData () =
    let rand       = new Random(int DateTime.UtcNow.Ticks)

    let gameScores = [| 1..1000 |] |> Array.collect (fun id ->
        let userId = sprintf "theburningmonk-%d" id

        gameTitles |> Array.map (fun title ->
            let gs = new GameScore(UserId = userId, GameTitle = title)
            gs.TopScore         <- rand.Next(5000)

            let dayOffset = float <| rand.Next(365)
            gs.TopScoreDateTime <- DateTime.UtcNow.AddDays(-dayOffset)

            gs.Wins             <- rand.Next(128)
            gs.Losses           <- rand.Next(128)
            
            gs))

    let batchWrite = cxt.CreateBatchWrite()

    printfn "Adding %d replies..." gameScores.Length

    batchWrite.AddPutItems(gameScores)

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