// Author : Yan Cui (twitter @theburningmonk)

// Email  : theburningmonk@gmail.com
// Blog   : http://theburningmonk.com

(*
    This script contains query and scan examples using the V2 API (with Local and Global
    Secondary Index support).
    For more details on the query index, please check the Wiki page:
        https://github.com/theburningmonk/DynamoDb.SQL/wiki

    This script does not execute against Amazon DynamoDB but executes against the
    DynamoDBLocal instead. For more detais, please see:
        http://aws.typepad.com/aws/2013/09/dynamodb-local-for-desktop-development.html
        http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html    
*)

(*

RUNNING INSTRUCTIONS

In order for this script to run, you need to have version 7 of the Java Runtime Environement 
installed, which you can download from: 
    http://java.com/en/

Before you start running the scripts, please double click the 
    lib/start_dynamodb_local.bat
file to start an instance of the DynamoDBLocal database for testing.

*)

#r @"bin\AWSSDK.dll"
#r @"bin\DynamoDb.SQL.dll"

#load "Common.fs"
open Common

open System
open System.Linq
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open DynamoDbV2.SQL.Execution

module QueryExamples = 
    let userId = "theburningmonk-1"
    
    //#region Hepers

    let assertCount n (response : Amazon.DynamoDBv2.Model.QueryResponse) = 
        assertThat (response.Count = n) 
                   (sprintf "Response has incorrect Count : %d %d" response.Count n)
        response

    let assertItemsCount n (response : Amazon.DynamoDBv2.Model.QueryResponse) = 
        assertThat (response.Items.Count = n)
                   (sprintf "Response has incorrect Items count : %d %d" response.Items.Count n)
        response |> assertCount n

    let assertUserId userId (response : Amazon.DynamoDBv2.Model.QueryResponse) = 
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["UserId"].S = userId))
                   (sprintf "UserId should all be \"%s\"" userId)
        response

    let assertGameScores n userId (gameScores : GameScore[]) =
        assertThat (gameScores.Length = n) 
                   (sprintf "Game scores has incorrect length : %d %d" gameScores.Length n)
        assertThat (gameScores |> Seq.forall (fun gs -> gs.UserId = userId))
                   (sprintf "GameScore.UserId should all be \"%s\"" userId)
        gameScores

    //#endregion

    /// Basic query using only key conditions
    /// NOTE: in the DynamoDBv2 API you no longer have to specify hash and range conditions explicitly, but
    /// instead all conditions now go into a single array.
    /// So the query syntax has also changed accordingly to remove the need to use the @HashKey and @RangeKey
    /// special keywords to specify hash and range key conditions. For more details, please check the wiki page:
    ///     https://github.com/theburningmonk/DynamoDb.SQL/wiki
    let queryByHashKey () = 
        // this query should return all 5 results
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\"" userId

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic hash key query :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertItemsCount 5 |> assertUserId userId |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic hash key query :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 5 userId |> ignore

        (* --------------------------------------------------------------- *)        
        let countQuery = "COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\""

        printfn "(AmazonDynamoDBClient) Running basic hash key count :\n\t\t%s" countQuery
        let countResponse = client.Query(countQuery)
        countResponse |> assertCount 5 |> ignore

    /// Basic query using both hash and range key
    /// NOTE: only a subset of the operators are allowed in a query (by DynamoDB)
    /// For the list of allowed operators, please check the Wiki
    ///     https://github.com/theburningmonk/DynamoDb.SQL/wiki
    let queryByHashAndRangeKey () =
        // this query should return 2 result
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" AND GameTitle BEGINS WITH \"A\"" userId
    
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running hash key and range key query:\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertItemsCount 2 |> assertUserId userId |> ignore

        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["GameTitle"].S.StartsWith "A"))
                   (sprintf "GameScore.GameTitle should all start with \"A\"")

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running hash key and range key query:\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 2 userId |> ignore

        assertThat (gameScores |> Seq.forall (fun gameScore -> gameScore.GameTitle.StartsWith "A"))
                   (sprintf "GameScore.GameTitle should all start with \"A\"")

        (* --------------------------------------------------------------- *)
        let countQuery = "COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\" AND GameTitle BEGINS WITH \"A\""
        printfn "(AmazonDynamoDBClient) Running hash key and range key count:\n\t\t%s" countQuery

        let countResponse = client.Query(countQuery)
        countResponse |> assertCount 2 |> ignore

    /// You can use ORDER ASC/DESC and LIMIT to get the first/last X number of items that
    /// matches the Hash/Range key filters
    let queryWithOrderByAndLimit () =
        // this query should return the first 3 scores
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" ORDER ASC LIMIT 3" userId
    
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with ORDER ASC and LIMIT :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertItemsCount 3 |> assertUserId userId |> ignore

        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["GameTitle"].S < meteorBlasters))
                   (sprintf "GameTitle should all be before %s" meteorBlasters)

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with ORDER ASC and LIMIT :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 3 userId |> ignore

        assertThat (gameScores |> Seq.forall (fun gameScore -> gameScore.GameTitle < meteorBlasters))
                   (sprintf "GameScore.GameTitle should all be before %s" meteorBlasters)

        // this query should return the last 3 scores
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" ORDER DESC LIMIT 3" userId
    
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with ORDER DESC and LIMIT :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertItemsCount 3 |> assertUserId userId |> ignore

        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["GameTitle"].S > attackShips))
                   (sprintf "GameTitle should all be after %s" attackShips)

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with ORDER DESC and LIMIT :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 3 userId |> ignore

        assertThat (gameScores |> Seq.forall (fun gameScore -> gameScore.GameTitle > attackShips))
                   (sprintf "GameScore.GameTitle should all be after %s" attackShips)

    /// By defaut, queries are executed with ConsistentRead switched on.
    /// To disable consistent read, use the NoConsistentRead query option
    let queryWithNoConsistentRead () =
        // this query should return all 5 game scores
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" WITH (NoConsistentRead)" userId

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with NoConsistentRead :\n\t\t%s" selectQuery    
        let response = client.Query(selectQuery)
        response |> assertItemsCount 5 |> assertUserId userId |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with NoConsistentRead :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 5 userId |> ignore

    /// AWS Guideline says that you should avoid sudden bursts of read activity:
    ///     http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html
    /// To accomplish this you can specify the page size for each request by using the PageSize query option
    let throttlingWithQueryPageSize () =
        // this query should return 3 game scores, but 3 requests are made (behind the scene) with each returning
        // only one item before the results are aggregated
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" LIMIT 3 WITH (PageSize(1))" userId

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with PageSize :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery) 
        response |> assertItemsCount 3 |> assertUserId userId |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with PageSize :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 3 userId |> ignore

    /// Rather than always getting all the attributes back, you can also choose a specific subset of 
    /// the available attributes
    let selectSpecificAttributes () =
        // should return all 5 scores with only a subset of attributes
        let selectQuery = sprintf "SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"%s\"" userId

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with speicifc attributes :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertItemsCount 5 |> assertUserId userId |> ignore

        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.Count = 3 && attrs.ContainsKey "UserId" && attrs.ContainsKey "GameTitle" && attrs.ContainsKey "Wins"))
                   "QueryResult items should only contain UserId, GameTitle and Wins attributes"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with speicifc attributes :\n\t\t%s" selectQuery
        let gameScores  = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 5 userId |> ignore

        assertThat (gameScores |> Seq.forall (fun gs -> gs.UserId.Length > 0 && gs.GameTitle.Length > 0 && gs.Wins > 0 && 
                                                        gs.Losses = 0 && gs.TopScore = 0 && gs.TopScoreDateTime = DateTime.MinValue))
                   "GameScore.Losses, GameScore.TopScore and GameScore.TopScoreDateTime should not be set"

    /// Whilst I'm not sure of the rationale behind this decision, but you can now optionally set the flag in
    /// the query request to tell DynamoDB to not report the consumed capacity units.
    /// To accomplish this, simply include the NoReturnedCapacity option in the WITH clause
    let queryWithNoReturnedConsumedCapacity () =
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE UserId = \"%s\" WITH (NoReturnedCapacity)" userId

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic query with NoReturnedCapacity :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertItemsCount 5 |> assertUserId userId |> ignore

        assertThat (response.ConsumedCapacity = null)
                   "ConsumedCapacity should not be returned"
    
    /// You can additionally query using a Local Secondary Index (DynamoDB V2 functionality) by specifying 
    /// the name of the index, and whether or not to fetch ALL attributes in the WITH clause.
    /// For the attributes that are not part of the index, DynamoDB will automatically fetch those from the
    /// table, consuming additional capacity unit in the process. For more details, please refer to docs:
    ///     http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
    let queryWithLocalSecondaryIndexAllAttributes () =
        // this query uses the TopScoreIndex to return all game scores with top score >= 1000
        // because the Indx query option specifies that all attributes are returned, attributes that are NOT part of 
        // the index - i.e. TopScoreDateTime - will be fetched from the table
        let selectQuery = sprintf "SELECT * FROM GameScores 
                                   WHERE UserId = \"%s\" 
                                   AND TopScore >= 1000 
                                   WITH(Index(TopScoreIndex, true))"
                                 userId
                
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with Index (all attributes) :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertUserId userId |> ignore

        assertThat (response.Count > 0) 
                   "Response should have a non-zero count"
        assertThat (response.Items.Count > 0)
                   "Response should have contained some items"
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["TopScore"].N >= "1000"))
                   "TopScore should all be >= 1000"
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.Count = 6))
                   "All 6 attributes should be returned"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with Index (all attributes) :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray

        assertThat (gameScores.Length > 0) 
                   "Game scores should have some items"
        assertThat (gameScores |> Seq.forall (fun gs -> gs.UserId = userId))
                   (sprintf "GameScore.UserId should all be %s" userId)
        assertThat (gameScores |> Seq.forall (fun gs -> gs.TopScore >= 1000))
                   "GameScore.TopScore should all >= 1000"
        assertThat (gameScores |> Seq.forall (fun gs -> gs.TopScoreDateTime > DateTime.MinValue))
                   "GameScore.TopScoreDateTime should all be populated"

        (* --------------------------------------------------------------- *)        
        let countQuery = sprintf "COUNT * FROM GameScores 
                                  WHERE UserId = \"%s\" 
                                  AND TopScore >= 1000 
                                  WITH(Index(TopScoreIndex, true))"
                                 userId

        printfn "(AmazonDynamoDBClient) Running count query with index :\n\t\t%s" countQuery
        let countResponse = client.Query(countQuery)

        assertThat (countResponse.Count >  0)
                   "Count response should have some count"

    /// You can additionally query using a Local Secondary Index (DynamoDB V2 functionality) by specifying 
    /// the name of the index, and whether or not to fetch ALL attributes in the WITH clause.
    /// For the attributes that are not part of the index, DynamoDB will automatically fetch those from the
    /// table, consuming additional capacity unit in the process. For more details, please refer to docs:
    ///     http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html
    let queryWithLocalSecondaryIndexProjectedAttributes () =
        // this query uses the TopScoreIndex to return all game scores with top score >= 1000
        // because the Indx query option specifies that NOT all attributes are returned, attributes that are NOT part 
        // of the index - i.e. TopScoreDateTime - will NOT be fetched from the table
        let selectQuery = sprintf "SELECT * FROM GameScores 
                                   WHERE UserId = \"%s\" 
                                   AND TopScore >= 1000 
                                   WITH(Index(TopScoreIndex, false))"
                                  userId
                
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with Index (index attributes only) :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)
        response |> assertUserId userId |> ignore

        assertThat (response.Count > 0) 
                   "Response should have a non-zero count"
        assertThat (response.Items.Count > 0)
                   "Response should have contained some items"
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["TopScore"].N >= "1000"))
                   "TopScore should all be >= 1000"
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.Count = 3 && (not <| attrs.ContainsKey "TopScoreDateTime")))
                   "GameScore.TopScoreDateTim should not be returned"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with Index (index attributes only) :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray

        assertThat (gameScores.Length > 0) 
                   "Game scores should have some items"
        assertThat (gameScores |> Seq.forall (fun gs -> gs.UserId = userId))
                   (sprintf "GameScore.UserId should all be %s" userId)
        assertThat (gameScores |> Seq.forall (fun gs -> gs.TopScore >= 1000))
                   "GameScore.TopScore should all >= 1000"
        assertThat (gameScores |> Seq.forall (fun gs -> gs.TopScoreDateTime = DateTime.MinValue))
                   "GameScore.TopScoreDateTime should not be populated"
    
    /// You can query using a Global Secondary Index (DynamoDB V2 functionality) by specifying 
    /// the name of the index, and whether or not to fetch ALL attributes in the WITH clause.
    /// If the projection type of the index is not ALL_ATTRIBUTES then you have to specify the
    /// attributes explicitly and to set the `AllAttributes` flag in the `Index` option to false.
    /// For more details, please refer to docs:
    ///     http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
    let queryWithGlobalSecondaryIndexProjectedAttributes () =
        // this query uses the TopScoreIndex to return all game scores with top score >= 1000
        // because the Indx query option specifies that all attributes are returned, attributes that are NOT part of 
        // the index - i.e. TopScoreDateTime - will be fetched from the table
        let selectQuery = sprintf "SELECT * FROM GameScores 
                                   WHERE GameTitle = \"%s\" 
                                   AND TopScore >= 1000
                                   WITH(Index(GameTitleIndex, false), NoConsistentRead)"
                                 starshipX
                
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with Index (all attributes) :\n\t\t%s" selectQuery
        let response = client.Query(selectQuery)

        assertThat (response.Count > 0) 
                   "Response should have a non-zero count"
        assertThat (response.Items.Count > 0)
                   "Response should have contained some items"
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["GameTitle"].S = starshipX))
                   (sprintf "GameTitle should all be %s" starshipX)
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["TopScore"].N >= "1000"))
                   "TopScore should all be >= 1000"
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.Count = 3 && (not <| attrs.ContainsKey "TopScoreDateTime")))
                   "GameScore.TopScoreDateTim should not be returned"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with Index (all attributes) :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecQuery<GameScore>(selectQuery) |> Seq.toArray

        assertThat (gameScores.Length > 0) 
                   "Game scores should have some items"
        assertThat (gameScores |> Seq.forall (fun gs -> gs.GameTitle = starshipX))
                   (sprintf "GameScore.GameTitle should all be %s" starshipX)
        assertThat (gameScores |> Seq.forall (fun gs -> gs.TopScore >= 1000))
                   "GameScore.TopScore should all >= 1000"
        assertThat (gameScores |> Seq.forall (fun gs -> gs.TopScoreDateTime = DateTime.MinValue))
                   "GameScore.TopScoreDateTime should not be populated"

        (* --------------------------------------------------------------- *)        
        let countQuery = sprintf "COUNT * FROM GameScores 
                                  WHERE GameTitle = \"%s\" 
                                  AND TopScore >= 1000 
                                  WITH(Index(GameTitleIndex, false), NoConsistentRead)"
                                 starshipX

        printfn "(AmazonDynamoDBClient) Running count query with index :\n\t\t%s" countQuery
        let countResponse = client.Query(countQuery)

        assertThat (countResponse.Count >  0)
                   "Count response should have some count"

module ScanExamples =
    //#region Hepers

    let assertCount n (response : Amazon.DynamoDBv2.Model.ScanResponse) = 
        assertThat (response.Count = n) 
                   (sprintf "Response has incorrect Count : %d %d" response.Count n)
        response

    let assertItemsCount n (response : Amazon.DynamoDBv2.Model.ScanResponse) = 
        assertThat (response.Items.Count = n)
                   (sprintf "Response has incorrect Items count : %d %d" response.Items.Count n)
        response |> assertCount n

    let assertGameTitle title (response : Amazon.DynamoDBv2.Model.ScanResponse) = 
        assertThat (response.Items |> Seq.forall (fun attrs -> attrs.["GameTitle"].S = title))
                   (sprintf "GameTitle should all be %s" title)
        response
    
    let assertGameScores n title (gameScores : GameScore[]) =
        assertThat (gameScores.Length = n) 
                   (sprintf "Game scores has incorrect length : %d %d" gameScores.Length n)                   
        assertThat (gameScores |> Seq.forall (fun gs -> gs.GameTitle = title))
                   (sprintf "GameScore.GameTitle should all be %s" title)
        gameScores

    //#endregion

    /// Basic scan example
    let basicScan () =
        // this scan should return quite a number of items... all the game scores should be for starship x
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic scan :\n\t\t%s" selectQuery
        let response = client.Scan(selectQuery)
        response |> assertItemsCount 1000 |> assertGameTitle starshipX |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic scan :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 1000 starshipX |> ignore

        (* --------------------------------------------------------------- *)
        let countQuery = sprintf "COUNT * FROM GameScores WHERE GameTitle = \"%s\"" starshipX

        printfn "(AmazonDynamoDBClient) Running basic scan count :\n\t\t%s" countQuery
        let countResponse = client.Scan(countQuery)
        countResponse |> assertCount 1000 |> ignore

    /// You can use ORDER ASC/DESC and LIMIT to get the first/last X number of items that
    /// matches the scan filters
    let scanWithLimit () =
        // this scan should return 10 replies, posted by John or James
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" LIMIT 10" starshipX
        
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running scan with LIMIT :\n\t\t%s" selectQuery
        let response = client.Scan(selectQuery)
        response |> assertItemsCount 10 |> assertGameTitle starshipX |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running scan with LIMIT :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 10 starshipX |> ignore

        (* --------------------------------------------------------------- *)
        let countQuery = sprintf "Count * FROM GameScores WHERE GameTitle = \"%s\" LIMIT 10" starshipX
        
        printfn "(AmazonDynamoDBClient) Running scan count with LIMIT :\n\t\t%s" countQuery
        let countResponse = client.Scan(countQuery)
        countResponse |> assertCount 10 |> ignore

    /// Similar to throttling queries, you can do the same with scans too, using the PageSize scan option
    let throttlingWithScanPageSize () =
        // this scan should return the same results as the basic example, but 
        //  - requires more requests
        //  - less capacity consumed per request
        //  - takes longer
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (PageSize(20))" starshipX

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic scan with PageSize :\n\t\t%s" selectQuery
        let response = client.Scan(selectQuery)
        response |> assertItemsCount 1000 |> assertGameTitle starshipX |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic scan with PageSize :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 1000 starshipX |> ignore
    
    /// You can use Scan segments to carry out scans in multiple segments simultaneously
    let scanWithScanPageSizeAndSegments () =
        // this scan should return the same results as the basic example, but 
        //  - requires more requests
        //  - less capacity consumed per request
        //  - takes longer
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (PageSize(20), Segments(2))" starshipX

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic scan with PageSize and 2 segments :\n\t\t%s" selectQuery
        let response = client.Scan(selectQuery)
        response |> assertItemsCount 1000 |> assertGameTitle starshipX |> ignore

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic scan with PageSize and 2 segments :\n\t\t%s" selectQuery
        let gameScores = ctx.ExecScan<GameScore>(selectQuery) |> Seq.toArray
        gameScores |> assertGameScores 1000 starshipX |> ignore
    
    /// Whilst I'm not sure of the rationale behind this decision, but you can now optionally set the flag in
    /// the scan request to tell DynamoDB to not report the consumed capacity units.
    /// To accomplish this, simply include the NoReturnedCapacity option in the WITH clause
    let scanWithNoReturnedConsumedCapacity () =
        // this scan should return quite a number of items... all the game scores should be for starship x
        let selectQuery = sprintf "SELECT * FROM GameScores WHERE GameTitle = \"%s\" WITH (NoReturnedCapacity)" starshipX

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic scan with NoReturnedCapacity :\n\t\t%s" selectQuery
        let response = client.Scan(selectQuery)
        response |> assertItemsCount 1000 |> assertGameTitle starshipX |> ignore

        assertThat (response.ConsumedCapacity = null)
                   "ConsumedCapacity should not be returned"

// you don't need to run these.
//let cmdFile = System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..\..\lib\start_dynamodb_local.bat")
//let jarFile = System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..\..\lib\dynamodb_local_2013-12-12/DynamoDBLocal.jar")
//let dynamoDbLocal = startDynamoDBLocal cmdFile jarFile

// uncomment these to create the table and seed the test data if you need to build the local DynamoDB table from scratch
//deleteTable()
//createTable()
//seedData()

time <| QueryExamples.queryByHashKey
time <| QueryExamples.queryByHashAndRangeKey
time <| QueryExamples.queryWithOrderByAndLimit
time <| QueryExamples.queryWithNoConsistentRead
time <| QueryExamples.throttlingWithQueryPageSize
time <| QueryExamples.selectSpecificAttributes
time <| QueryExamples.queryWithNoReturnedConsumedCapacity
time <| QueryExamples.queryWithLocalSecondaryIndexAllAttributes
time <| QueryExamples.queryWithLocalSecondaryIndexProjectedAttributes
time <| QueryExamples.queryWithGlobalSecondaryIndexProjectedAttributes

time <| ScanExamples.basicScan
time <| ScanExamples.scanWithLimit
time <| ScanExamples.throttlingWithScanPageSize
time <| ScanExamples.scanWithScanPageSizeAndSegments
time <| ScanExamples.scanWithNoReturnedConsumedCapacity