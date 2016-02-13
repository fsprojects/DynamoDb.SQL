namespace DynamoDb.SQL.DynamoDBLocal.Tests

open System
open System.Collections.Generic
open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model

[<DynamoDBTable("GameScores")>]
type GameScore () =
    [<DynamoDBHashKey>]  member val UserId    = "" with get, set
    [<DynamoDBRangeKey>] member val GameTitle = "" with get, set
            
    member val TopScore         = 0 with get, set
    member val TopScoreDateTime = DateTime.MinValue with get, set
    member val Wins             = 0 with get, set
    member val Losses           = 0 with get, set

[<AutoOpen>]
module GameScoresTableVars = 
    let awsKey      = "my-aws-key"
    let awsSecret   = "my-aws-secret"
    let config = new AmazonDynamoDBConfig(ServiceURL = "http://localhost:8000")

    let client = new AmazonDynamoDBClient(awsKey, awsSecret, config)
    let ctx    = new DynamoDBContext(client)

    let userId         = "theburningmonk-1"
            
    let alienAdventure = "Alien Adventure"
    let attackShips    = "Attack Ships"
    let galaxyInvaders = "Galaxy Invaders"
    let meteorBlasters = "Meteor Blasters"
    let starshipX      = "Starship X"

    let gameTitles = [| alienAdventure; attackShips; galaxyInvaders; meteorBlasters; starshipX |]

[<AutoOpen>]
module Helper =
    let inline nullOrWs str        = String.IsNullOrWhiteSpace str
    let inline notNullNotWs str    = not <| nullOrWs str
    let inline notDefault (x : 'T) = x <> Unchecked.defaultof<'T>
    let inline isDefault (x : 'T)  = not <| notDefault x

module ``Integration tests against DynamoDBLocal`` =
    let awsKey    = "my-aws-key"
    let awsSecret = "my-aws-secret"
    let config = new AmazonDynamoDBConfig(ServiceURL = "http://localhost:8000")
    let client = new AmazonDynamoDBClient(awsKey, awsSecret, config)
    let ctx    = new DynamoDBContext(client)

    let tableName = "GameScores"

    let createTable () = 
        let mutable req = new Amazon.DynamoDBv2.Model.CreateTableRequest()
        req.TableName <- tableName

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

    let seedData =
        let rand = new Random(int DateTime.UtcNow.Ticks)

        [| 1..1000 |] |> Array.collect (fun id ->
            let userId = sprintf "theburningmonk-%d" id

            gameTitles |> Array.map (fun title ->
                let gs = new GameScore(UserId = userId, GameTitle = title)
                gs.TopScore <- rand.Next(5000)

                let dayOffset = float <| rand.Next(365)
                gs.TopScoreDateTime <- DateTime.UtcNow.AddDays(-dayOffset)

                gs.Wins   <- rand.Next(128)
                gs.Losses <- rand.Next(128)
            
                gs))

    let insertGameScores (gameScores : GameScore[]) =
        printfn "Adding %d game scores..." gameScores.Length
        
        let batchWrite = ctx.CreateBatchWrite()
        batchWrite.AddPutItems(gameScores)
        batchWrite.Execute()

        printfn "Adding %d game scores...DONE" gameScores.Length

    [<TestFixtureSetUp>]
    let ``seed data in DynamoDBLocal`` () =
        printfn "Seeding data into DynamoDBLocal..."

        createTable ()
        insertGameScores seedData

        printfn "Seeding data into DynamoDBLocal...DONE"

    [<Test>]
    let ``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" should return 5 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 5
        response.Items.Count |> should equal 5
        response.Items
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items 
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 5
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameTitles
        |> Seq.forall (fun title -> gameScores |> Seq.exists (fun gs -> gs.GameTitle = title))
        |> should equal true

    [<Test>]
    let ``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" should return 5`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\"" userId

        let response = client.Query(selectQuery)
        response.Count |> should equal 5
        response.Items |> should haveCount 0

    [<Test>]
    let ``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" and GameTitle BEGINS WITH \"A\" should return 2 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND GameTitle BEGINS WITH \"A\"" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 2
        response.Items.Count |> should equal 2
        
        response.Items       
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S.StartsWith "A")
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 2
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle.StartsWith "A")
        |> should equal true

        gameTitles
        |> Seq.filter (fun title -> title.StartsWith "A")
        |> Seq.forall (fun title -> 
            gameScores 
            |> Seq.exists (fun gs -> gs.GameTitle = title))
        |> should equal true

    [<Test>]
    let ``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND GameTitle BEGINS WITH \"A\" should return 2`` () =
        let selectQuery = 
            sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\" and GameTitle BEGINS WITH \"A\"" userId

        let response = client.Query(selectQuery)
        response.Count |> should equal 2
        response.Items |> should haveCount 0

    [<Test>]
    let ``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER ASC LIMIT 3 should return top 3 results`` () =
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" ORDER ASC LIMIT 3" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 3
        response.Items.Count |> should equal 3

        response.Items
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 3
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        let top3Scores = 
            ctx.ExecQuery<GameScore>(
                sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId) 
            |> Seq.sortBy (fun gs -> gs.GameTitle)
            |> Seq.take 3
            |> Seq.toArray

        top3Scores
        |> Seq.forall (fun gs -> 
            gameScores 
            |> Seq.exists (fun gs' -> gs.GameTitle = gs'.GameTitle))
        |> should equal true

    [<Test>]
    let ``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER ASC LIMIT 3 should return 3`` () =
        let selectQuery = 
            sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\" ORDER ASC LIMIT 3" userId

        let response = client.Query(selectQuery)
        response.Count |> should equal 3
        response.Items |> should haveCount 0

    [<Test>]
    let ``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER DESC LIMIT 3 should return bottom 3 results`` () =
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" ORDER DESC LIMIT 3" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 3
        response.Items.Count |> should equal 3

        response.Items
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 3
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        let top3Scores = 
            ctx.ExecQuery<GameScore>(
                sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId) 
            |> Seq.sortBy (fun gs -> gs.GameTitle)
            |> Seq.skip 2
            |> Seq.toArray
        top3Scores
        |> Seq.forall (fun gs -> 
            gameScores 
            |> Seq.exists (fun gs' -> gs.GameTitle = gs'.GameTitle))
        |> should equal true

    [<Test>]
    let ``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER DESC LIMIT 3 should return 3`` () =
        let selectQuery = 
            sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\" ORDER DESC LIMIT 3" userId

        let response = client.Query(selectQuery)
        response.Count |> should equal 3
        response.Items |> should haveCount 0
    
    [<Test>]
    let ``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\ WITH (NoConsistentRead) should return 5 results`` () =
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" WITH (NoConsistentRead)" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 5
        response.Items.Count |> should equal 5

        response.Items  
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 5
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameTitles
        |> Seq.forall (fun title -> 
            gameScores 
            |> Seq.exists (fun gs -> gs.GameTitle = title))
        |> should equal true

    [<Test>]
    let ``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\ WITH (PageSize(1)) should return 5 results`` () =
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" WITH (PageSize(1))" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 5
        response.Items.Count |> should equal 5

        response.Items
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 5
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameTitles
        |> Seq.forall (fun title -> 
            gameScores 
            |> Seq.exists (fun gs -> gs.GameTitle = title))
        |> should equal true

    [<Test>]
    let ``SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"theburningmonk-1\" should return only those attributes`` () =
        let selectQuery = 
            sprintf "SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"%s\"" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal 5
        response.Items.Count |> should equal 5
        response.Items  
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 3)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 5
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> 
            notNullNotWs gs.GameTitle && notDefault gs.Wins)
        |> should equal true
        
        gameScores
        |> Seq.forall (fun gs -> 
            isDefault gs.TopScore && 
            isDefault gs.TopScoreDateTime && 
            isDefault gs.Losses)
        |> should equal true
        
        gameTitles
        |> Seq.forall (fun title -> 
            gameScores 
            |> Seq.exists (fun gs -> gs.GameTitle = title))
        |> should equal true

    [<Test>]
    let ``(LSI) SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, true)) should return 4 results`` () =
        let expectedCount =
            seedData
            |> Seq.filter (fun score ->
                score.UserId = userId &&
                score.TopScore >= 1000)
            |> Seq.length
        
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, true))" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal expectedCount
        response.Items.Count |> should equal expectedCount

        response.Items
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal expectedCount
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> gs.TopScore >= 1000)
        |> should equal true

    [<Test>]
    let ``(LSI) SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, false)) should return only projected attributes`` () =
        let expectedCount =
            seedData
            |> Seq.filter (fun score ->
                score.UserId = userId &&
                score.TopScore >= 1000)
            |> Seq.length
        
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, false))" userId

        let response = client.Query(selectQuery)
        response.Count       |> should equal expectedCount
        response.Items.Count |> should equal expectedCount

        response.Items  
        |> Seq.forall (fun item -> item.["UserId"].S = userId)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> int item.["TopScore"].N >= 1000)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 3)
        |> should equal true

        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal expectedCount
        gameScores
        |> Seq.forall (fun gs -> gs.UserId = userId)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> gs.TopScore >= 1000)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> 
            notNullNotWs gs.GameTitle && notDefault gs.TopScore)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs ->
            isDefault gs.TopScoreDateTime && 
            isDefault gs.Wins && 
            isDefault gs.Losses)
        |> should equal true

    [<Test>]
    let ``(GSI) SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" AND TopScore >= 1000 WITH (Index(GameTitleIndex, false), NoConsistentRead) should return all matching results with only projected attributes`` () =
        let expectedCount = 
            seedData
            |> Seq.filter (fun score ->
                score.GameTitle = starshipX && score.TopScore >= 1000)
            |> Seq.length
        
        let selectQuery = 
            "SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" AND TopScore >= 1000 WITH (Index(GameTitleIndex, false), NoConsistentRead)"

        let response = client.Query(selectQuery)
        response.Count       |> should equal expectedCount
        response.Items.Count |> should equal expectedCount
        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> int item.["TopScore"].N >= 1000)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 3)
        |> should equal true

        let gameScores = 
            ctx.ExecQuery<GameScore>(selectQuery) 
            |> Seq.toArray

        gameScores.Length |> should equal expectedCount
        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> gs.TopScore >= 1000)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> 
            notNullNotWs gs.UserId && notDefault gs.TopScore)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> 
            isDefault gs.TopScoreDateTime && 
            isDefault gs.Wins && 
            isDefault gs.Losses)
        |> should equal true

    [<Test>]
    let ``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" should return 1000 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        let response = client.Scan(selectQuery)
        response.Count       |> should equal 1000
        response.Items.Count |> should equal 1000
        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = 
            ctx.ExecScan<GameScore>(selectQuery) 
            |> Seq.toArray
        gameScores.Length |> should equal 1000
        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
        |> should equal true
    
    [<Test>]
    let ``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" should return 1000`` () =
        let selectQuery = 
            sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        let response = client.Scan(selectQuery)
        response.Count |> should equal 1000
        response.Items |> should haveCount 0

    [<Test>]
    let ``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" LIMIT 10 should return 10 results`` () =
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" LIMIT 10" starshipX

        let response = client.Scan(selectQuery)
        response.Count       |> should equal 10
        response.Items.Count |> should equal 10
        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length |> should equal 10
        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
        |> should equal true
                                
    [<Test>]
    let ``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" LIMIT 10 should return 10`` () =
        let selectQuery = 
            sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\" LIMIT 10" starshipX

        let response = client.Scan(selectQuery)
        response.Count |> should equal 10
        response.Items |> should haveCount 0
        
    [<Test>]
    let ``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (PageSize(20)) should return 1000 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (PageSize(20))" starshipX

        let response = client.Scan(selectQuery)
        response.Count       |> should equal 1000
        response.Items.Count |> should equal 1000
        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = 
            ctx.ExecScan<GameScore>(selectQuery) 
            |> Seq.toArray
        gameScores.Length |> should equal 1000
        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
        |> should equal true
                                
    [<Test>]
    let ``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (PageSize(20)) should return 1000`` () =
        let selectQuery = 
            sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\" WITH (PageSize(20))" starshipX

        let response = client.Scan(selectQuery)
        response.Count |> should equal 1000
        response.Items |> should haveCount 0
        
    [<Test>]
    let ``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (Segments(10)) should return 1000 results`` () =
        let selectQuery = 
            sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (Segments(10))" starshipX

        let response = client.Scan(selectQuery)
        response.Count       |> should equal 1000
        response.Items.Count |> should equal 1000
        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
        |> should equal true

        response.Items
        |> Seq.forall (fun item -> item.Count = 6)
        |> should equal true

        let gameScores = 
            ctx.ExecScan<GameScore>(selectQuery) 
            |> Seq.toArray
        gameScores.Length |> should equal 1000
        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
        |> should equal true
                                
    [<Test>]
    let ``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (Segments(10)) should return 1000`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\" WITH (Segments(10))" starshipX

        let response = client.Scan(selectQuery)
        response.Count |> should equal 1000
        response.Items |> should haveCount 0

    [<Test>]
    let ``SELECT GameTitle, Wins, Losses FROM GameScores WHERE GameTitle = \"Starship X\" should return only those attributes`` () =
        let selectQuery = 
            sprintf "SELECT GameTitle, TopScoreDateTime FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        let response = client.Scan(selectQuery)
        response.Count       |> should equal 1000
        response.Items.Count |> should equal 1000
        response.Items
        |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
        |> should equal true
        response.Items
        |> Seq.forall (fun item -> item.Count = 2)
        |> should equal true

        let gameScores = 
            ctx.ExecScan<GameScore>(selectQuery)
            |> Seq.toArray
        gameScores.Length |> should equal 1000
        gameScores
        |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
        |> should equal true

        gameScores
        |> Seq.forall (fun gs -> 
            nullOrWs gs.UserId && 
            notDefault gs.TopScoreDateTime && 
            isDefault gs.TopScore && 
            isDefault gs.Wins && 
            isDefault gs.Losses)
        |> should equal true