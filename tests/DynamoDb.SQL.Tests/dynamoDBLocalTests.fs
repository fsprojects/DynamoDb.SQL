namespace DynamoDb.SQL.dynamoDBLocal.Tests

open System
open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel

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

[<TestFixture>]
type ``Given a query against the GameScores table in DynamoDBLocal`` () = 
    [<Test>]
    member this.``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" should return 5 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 5
        response.Items.Count    |> should equal 5
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 5
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameTitles              |> Seq.forall (fun title -> gameScores |> Seq.exists (fun gs -> gs.GameTitle = title))
                                |> should equal true

    [<Test>]
    member this.``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" should return 5`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\"" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 5
        response.Items          |> should haveCount 0

    [<Test>]
    member this.``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" and GameTitle BEGINS WITH \"A\" should return 2 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND GameTitle BEGINS WITH \"A\"" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 2
        response.Items.Count    |> should equal 2
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S.StartsWith "A")
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 2
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle.StartsWith "A")
                                |> should equal true
        gameTitles              |> Seq.filter (fun title -> title.StartsWith "A")
                                |> Seq.forall (fun title -> gameScores |> Seq.exists (fun gs -> gs.GameTitle = title))
                                |> should equal true

    [<Test>]
    member this.``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND GameTitle BEGINS WITH \"A\" should return 2`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\" and GameTitle BEGINS WITH \"A\"" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 2
        response.Items          |> should haveCount 0

    [<Test>]
    member this.``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER ASC LIMIT 3 should return top 3 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" ORDER ASC LIMIT 3" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 3
        response.Items.Count    |> should equal 3
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 3
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true

        let top3Scores  = ctx.ExecQuery<GameScore>(sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId) 
                          |> Seq.sortBy (fun gs -> gs.GameTitle)
                          |> Seq.take 3
                          |> Seq.toArray
        top3Scores              |> Seq.forall (fun gs -> gameScores |> Seq.exists (fun gs' -> gs.GameTitle = gs'.GameTitle))
                                |> should equal true

    [<Test>]
    member this.``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER ASC LIMIT 3 should return 3`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\" ORDER ASC LIMIT 3" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 3
        response.Items          |> should haveCount 0

    [<Test>]
    member this.``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER DESC LIMIT 3 should return bottom 3 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" ORDER DESC LIMIT 3" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 3
        response.Items.Count    |> should equal 3
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 3
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true

        let top3Scores  = ctx.ExecQuery<GameScore>(sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId) 
                          |> Seq.sortBy (fun gs -> gs.GameTitle)
                          |> Seq.skip 2
                          |> Seq.toArray
        top3Scores              |> Seq.forall (fun gs -> gameScores |> Seq.exists (fun gs' -> gs.GameTitle = gs'.GameTitle))
                                |> should equal true

    [<Test>]
    member this.``COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER DESC LIMIT 3 should return 3`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE UserId = \"%s\" ORDER DESC LIMIT 3" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 3
        response.Items          |> should haveCount 0
    
    [<Test>]
    member this.``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\ WITH (NoConsistentRead) should return 5 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" WITH (NoConsistentRead)" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 5
        response.Items.Count    |> should equal 5
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 5
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameTitles              |> Seq.forall (fun title -> gameScores |> Seq.exists (fun gs -> gs.GameTitle = title))
                                |> should equal true

    [<Test>]
    member this.``SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\ WITH (PageSize(1)) should return 5 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" WITH (PageSize(1))" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 5
        response.Items.Count    |> should equal 5
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 5
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameTitles              |> Seq.forall (fun title -> gameScores |> Seq.exists (fun gs -> gs.GameTitle = title))
                                |> should equal true

    [<Test>]
    member this.``SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"theburningmonk-1\" should return only those attributes`` () =
        let selectQuery = sprintf "SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"%s\"" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 5
        response.Items.Count    |> should equal 5
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 3)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 5
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> notNullNotWs gs.GameTitle && notDefault gs.Wins)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> isDefault gs.TopScore && isDefault gs.TopScoreDateTime && isDefault gs.Losses)
                                |> should equal true
        gameTitles              |> Seq.forall (fun title -> gameScores |> Seq.exists (fun gs -> gs.GameTitle = title))
                                |> should equal true

    [<Test>]
    member this.``(LSI) SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, true)) should return 4 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, true))" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 4
        response.Items.Count    |> should equal 4
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 4
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> gs.TopScore >= 1000)
                                |> should equal true

    [<Test>]
    member this.``(LSI) SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, false)) should return only projected attributes`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND TopScore >= 1000 WITH (Index(TopScoreIndex, false))" userId

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 4
        response.Items.Count    |> should equal 4
        response.Items          |> Seq.forall (fun item -> item.["UserId"].S = userId)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> int item.["TopScore"].N >= 1000)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 3)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 4
        gameScores              |> Seq.forall (fun gs -> gs.UserId = userId)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> gs.TopScore >= 1000)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> notNullNotWs gs.GameTitle && notDefault gs.TopScore)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> isDefault gs.TopScoreDateTime && isDefault gs.Wins && isDefault gs.Losses)
                                |> should equal true

    [<Test>]
    member this.``(GSI) SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" AND TopScore >= 1000 WITH (Index(GameTitleIndex, false), NoConsistentRead) should return 805 results with only projected attributes`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" AND TopScore >= 1000 WITH (Index(GameTitleIndex, false), NoConsistentRead)" starshipX

        let response    = client.Query(selectQuery)
        response.Count          |> should equal 805
        response.Items.Count    |> should equal 805
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> int item.["TopScore"].N >= 1000)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 3)
                                |> should equal true

        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 805
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> gs.TopScore >= 1000)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> notNullNotWs gs.UserId && notDefault gs.TopScore)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> isDefault gs.TopScoreDateTime && isDefault gs.Wins && isDefault gs.Losses)
                                |> should equal true

[<TestFixture>]
type ``Given a scan against the GameScores table in DynamoDBLocal`` () = 
    [<Test>]
    member this.``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" should return 1000 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items.Count    |> should equal 1000
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 1000
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
                                |> should equal true
    
    [<Test>]
    member this.``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" should return 1000`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items          |> should haveCount 0

    [<Test>]
    member this.``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" LIMIT 10 should return 10 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" LIMIT 10" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 10
        response.Items.Count    |> should equal 10
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 10
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
                                |> should equal true
                                
    [<Test>]
    member this.``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" LIMIT 10 should return 10`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\" LIMIT 10" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 10
        response.Items          |> should haveCount 0
        
    [<Test>]
    member this.``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (PageSize(20)) should return 1000 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (PageSize(20))" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items.Count    |> should equal 1000
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 1000
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
                                |> should equal true
                                
    [<Test>]
    member this.``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (PageSize(20)) should return 1000`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\" WITH (PageSize(20))" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items          |> should haveCount 0
        
    [<Test>]
    member this.``SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (Segments(10)) should return 1000 results`` () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (Segments(10))" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items.Count    |> should equal 1000
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 6)
                                |> should equal true

        let gameScores  = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 1000
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
                                |> should equal true
                                
    [<Test>]
    member this.``COUNT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (Segments(10)) should return 1000`` () =
        let selectQuery = sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\" WITH (Segments(10))" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items          |> should haveCount 0

    [<Test>]
    member this.``SELECT GameTitle, Wins, Losses FROM GameScores WHERE GameTitle = \"Starship X\" should return only those attributes`` () =
        let selectQuery = sprintf "SELECT GameTitle, TopScoreDateTime FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        let response    = client.Scan(selectQuery)
        response.Count          |> should equal 1000
        response.Items.Count    |> should equal 1000
        response.Items          |> Seq.forall (fun item -> item.["GameTitle"].S = starshipX)
                                |> should equal true
        response.Items          |> Seq.forall (fun item -> item.Count = 2)
                                |> should equal true

        let gameScores  = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores.Length       |> should equal 1000
        gameScores              |> Seq.forall (fun gs -> gs.GameTitle = starshipX)
                                |> should equal true
        gameScores              |> Seq.forall (fun gs -> nullOrWs gs.UserId && notDefault gs.TopScoreDateTime && isDefault gs.TopScore && isDefault gs.Wins && isDefault gs.Losses)
                                |> should equal true