// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

(*
    This script contains query and scan examples using the V1 API (without Index support).
    For more details on the query index, please check the Wiki page:
        https://github.com/theburningmonk/DynamoDb.SQL/wiki

    If you want to run these examples, please provide the AWS key and secret for your AWS 
    account and run the following functions from the 'Common'
    module first:
        createTable()   - creates a new table with 50 read and 50 write capacity
        seedData()      - seed the table with 10k items

    PLEASE DON'T FORGET TO DELETE THE TABLE AFTER RUNNING THE EXAMPLES. 
    I WILL NOT BE LIABLE FOR ANY AWS COSTS YOU INCUR WHILE RUNNING THESE EXAMPLES.
*)

#r @"bin\Release\AWSSDK.dll"
#r @"bin\Release\DynamoDb.SQL.dll"

#load "Common.fs"
open Common

open System
open System.Linq
open Amazon.DynamoDB
open Amazon.DynamoDB.DataModel
open DynamoDb.SQL.Execution

module QueryExamples = 
    /// Basic query using only the hash key
    let queryByHashKey () = 
        // this query should return all 10 results
        let selectQuery = "SELECT * FROM Reply WHERE @HashKey = 250"    

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic hash key query:\n\t\t%s" selectQuery
        let response = clientV1.Query(selectQuery)
        assertThat (response.QueryResult.Count = 10) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 10)
        assertThat (response.QueryResult.Items.Count = 10)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 10)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic hash key query:\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 10) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 10)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"

        (* --------------------------------------------------------------- *)        
        let countQuery = "COUNT * FROM Reply WHERE @HashKey = 250"

        printfn "(AmazonDynamoDBClient) Running basic hash key count:\n\t\t%s" countQuery
        let countResponse = clientV1.Query(countQuery)
        assertThat (countResponse.QueryResult.Count = 10)
                   (sprintf "Count response has incorrect Count : %d %d" countResponse.QueryResult.Count 10)

    /// Basic query using both hash and range key
    /// NOTE: only a subset of the operators are allowed in a query (by DynamoDB)
    /// For the list of allowed operators, please check the Wiki
    ///     https://github.com/theburningmonk/DynamoDb.SQL/wiki
    let queryByHashAndRangeKey () =
        let twoDaysAgo = DateTime.Now.AddDays(-2.0).Date
        let twoDaysAgoStr = twoDaysAgo.ToString("yyyy-MM-dd")

        // this query should return 2 results - the replies in the two days
        let selectQuery = "SELECT * FROM Reply WHERE @HashKey = 250 AND @RangeKey >= \"" + twoDaysAgoStr + "\""
    
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running hash key and range key query:\n\t\t%s" selectQuery
        let response = clientV1.Query(selectQuery)
        assertThat (response.QueryResult.Count = 2) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 2)
        assertThat (response.QueryResult.Items.Count = 2)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 2)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["ReplyDateTime"].S >= twoDaysAgoStr))
                   (sprintf "Reply.ReplyDateTime should all be earlier than %A" twoDaysAgo)

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running hash key and range key query:\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 2) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 2)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"
        assertThat (replies |> Seq.forall (fun reply -> reply.ReplyDateTime >= twoDaysAgo))
                   (sprintf "Reply.ReplyDateTime should all be earlier than %A" twoDaysAgo)

        (* --------------------------------------------------------------- *)
        let countQuery = "COUNT * FROM Reply WHERE @HashKey = 250 AND @RangeKey >= \"" + twoDaysAgoStr + "\""
        printfn "(AmazonDynamoDBClient) Running hash key and range key count:\n\t\t%s" countQuery

        let countResponse = clientV1.Query(countQuery)
        assertThat (countResponse.QueryResult.Count = 2)
                   (sprintf "Count response has incorrect Count : %d %d" countResponse.QueryResult.Count 2)

    /// You can use ORDER ASC/DESC and LIMIT to get the first/last X number of items that
    /// matches the Hash/Range key filters
    let queryWithOrderByAndLimit () =
        let sevenDaysAgo    = DateTime.Now.AddDays(-7.0).Date
        let sevenDaysAgoStr = sevenDaysAgo.ToString("yyyy-MM-dd")
        let threeDaysAgo    = DateTime.Now.AddDays(-3.0).Date
        let threeDaysAgoStr = threeDaysAgo.ToString("yyyy-MM-dd")

        // this query should return the first 3 replies - from 10 - 8 days ago
        let selectQuery = "SELECT * FROM Reply WHERE @HashKey = 250 ORDER ASC LIMIT 3"    
    
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with ORDER ASC and LIMIT :\n\t\t%s" selectQuery
        let response = clientV1.Query(selectQuery)
        assertThat (response.QueryResult.Count = 3) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 3)
        assertThat (response.QueryResult.Items.Count = 3)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 3)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["ReplyDateTime"].S <= sevenDaysAgoStr))
                   (sprintf "Reply.ReplyDateTime should all be older than %A" sevenDaysAgo)

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with ORDER ASC and LIMIT :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 3) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 3)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"
        assertThat (replies |> Seq.forall (fun reply -> reply.ReplyDateTime <= sevenDaysAgo))
                   (sprintf "Reply.ReplyDateTime should all be older than %A" sevenDaysAgo)

        // this query should return the last 3 replies - from 1 - 3 days ago
        let selectQuery = "SELECT * FROM Reply WHERE @HashKey = 250 ORDER DESC LIMIT 3"    
    
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with ORDER DESC and LIMIT :\n\t\t%s" selectQuery
        let response = clientV1.Query(selectQuery)
        assertThat (response.QueryResult.Count = 3) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 3)
        assertThat (response.QueryResult.Items.Count = 3)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 3)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["ReplyDateTime"].S >= threeDaysAgoStr))
                   (sprintf "Reply.ReplyDateTime should all be earlier than %A" threeDaysAgo)

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with ORDER DESC and LIMIT :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 3) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 3)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"
        assertThat (replies |> Seq.forall (fun reply -> reply.ReplyDateTime >= threeDaysAgo))
                   (sprintf "Reply.ReplyDateTime should all be earlier than %A" threeDaysAgo)

    /// By defaut, queries are executed with ConsistentRead switched on.
    /// To disable consistent read, use the NoConsistentRead query option
    let queryWithNoConsistentRead () =
        // this query should return all 10 replies
        let selectQuery = "SELECT * FROM Reply WHERE @HashKey = 250 WITH (NoConsistentRead)"

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with NoConsistentRead :\n\t\t%s" selectQuery    
        let response = clientV1.Query(selectQuery)
        assertThat (response.QueryResult.Count = 10) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 10)
        assertThat (response.QueryResult.Items.Count = 10)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 10)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with NoConsistentRead :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 10) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 10)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"

    /// AWS Guideline says that you should avoid sudden bursts of read activity:
    ///     http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html
    /// To accomplish this you can specify the page size for each request by using the PageSize query option
    let throttlingWithQueryPageSize () =
        // this query should return 3 replies, but 3 requests are made (behind the scene) with each returning
        // only one item before the results are aggregated
        let selectQuery = "SELECT * FROM Reply WHERE @HashKey = 250 LIMIT 3 WITH (PageSize(1))"

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with PageSize :\n\t\t%s" selectQuery
        let response = clientV1.Query(selectQuery)    
        assertThat (response.QueryResult.Count = 3) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 3)
        assertThat (response.QueryResult.Items.Count = 3)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 3)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with PageSize :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 3) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 3)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"

    /// Rather than always getting all the attributes back, you can also choose a specific subset of 
    /// the available attributes
    let selectSpecificAttributes () =
        // should return all 10 replies with only Id and Message attributes
        let selectQuery = "SELECT Id, Message FROM Reply WHERE @HashKey = 250"

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running query with speicifc attributes :\n\t\t%s" selectQuery
        let response = clientV1.Query(selectQuery)
        assertThat (response.QueryResult.Count = 10) 
                   (sprintf "Response has incorrect Count : %d %d" response.QueryResult.Count 10)
        assertThat (response.QueryResult.Items.Count = 10)
                   (sprintf "Response has incorrect Items count : %d %d" response.QueryResult.Items.Count 10)
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.["Id"].N = "250"))
                   "Id should all be 250"
        assertThat (response.QueryResult.Items |> Seq.forall (fun attrs -> attrs.Count = 2 && attrs.ContainsKey "Id" && attrs.ContainsKey "Message"))
                   "QueryResult items should only contain Id and Message attributes"

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running query with speicifc attributes :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecQuery<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 10) 
                   (sprintf "Replies has incorrect length : %d %d" replies.Length 10)
        assertThat (replies |> Seq.forall (fun reply -> reply.Id = 250))
                   "Reply.Id should all be 250"
        assertThat (replies |> Seq.forall (fun reply -> reply.Message.Length > 0 && reply.PostedBy = "" && reply.ReplyDateTime = DateTime.MinValue))
                   "Reply.PostedBy and Reply.ReplyDateTime should not be set"

module ScanExamples =
    /// Basic scan example
    let basicScan () =
        // this scan should return quite a number of items... all the replies should be posted by either John or James
        let selectQuery = "SELECT * FROM Reply 
                           WHERE Id >= 900 
                           AND PostedBy BEGINS WITH \"J\""

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic scan :\n\t\t%s" selectQuery
        let response = clientV1.Scan(selectQuery)
        assertThat (response.ScanResult.Count > 0) 
                   "Response should have found some results"
        assertThat (response.ScanResult.Items.Count > 0)
                   "Response should have some items"
        assertThat (response.ScanResult.Items |> Seq.forall (fun attrs -> attrs.["PostedBy"].S.StartsWith "J"))
                   "PostedBy should all start with \"J\""

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic scan :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecScan<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length > 0) 
                   "Replies should have some items"
        assertThat (replies |> Seq.forall (fun reply -> reply.PostedBy.StartsWith "J"))
                   "Reply.PostedBy should all start with \"J\""

        (* --------------------------------------------------------------- *)
        let countQuery = "Count * FROM Reply 
                          WHERE Id >= 900 
                          AND PostedBy BEGINS WITH \"J\""

        printfn "(AmazonDynamoDBClient) Running basic scan count :\n\t\t%s" countQuery
        let countResponse = clientV1.Scan(countQuery)
        assertThat (countResponse.ScanResult.Count > 0)
                   "Count response should have found some results"

    /// You can use ORDER ASC/DESC and LIMIT to get the first/last X number of items that
    /// matches the scan filters
    let scanWithLimit () =
        // this scan should return 10 replies, posted by John or James
        let selectQuery = "SELECT * FROM Reply 
                           WHERE Id >= 900 
                           AND PostedBy BEGINS WITH \"J\"
                           LIMIT 10"
        
        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running scan with LIMIT :\n\t\t%s" selectQuery
        let response = clientV1.Scan(selectQuery)
        assertThat (response.ScanResult.Count = 10) 
                   (sprintf "Response has incorrect Count : %d %d" response.ScanResult.Count 10)
        assertThat (response.ScanResult.Items.Count > 0)
                   (sprintf "Response has incorrect Items count : %d %d" response.ScanResult.Items.Count 10)
        assertThat (response.ScanResult.Items |> Seq.forall (fun attrs -> attrs.["PostedBy"].S.StartsWith "J"))
                   "PostedBy should all start with \"J\""

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running scan with LIMIT :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecScan<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length = 10) 
                   (sprintf "Replies have incorrect length : %d %d" replies.Length 10)
        assertThat (replies |> Seq.forall (fun reply -> reply.PostedBy.StartsWith "J"))
                   "Reply.PostedBy should all start with \"J\""

        (* --------------------------------------------------------------- *)
        let countQuery = "Count * FROM Reply 
                          WHERE Id >= 900 
                          AND PostedBy BEGINS WITH \"J\"
                          LIMIT 10"
        
        printfn "(AmazonDynamoDBClient) Running scan count with LIMIT :\n\t\t%s" countQuery
        let countResponse = clientV1.Scan(countQuery)
        assertThat (countResponse.ScanResult.Count = 10) 
                   (sprintf "Count response has incorrect Count : %d %d" countResponse.ScanResult.Count 10)

    /// Similar to throttling queries, you can do the same with scans too, using the PageSize scan option
    let throttlingWithScanPageSize () =
        // this scan should return the same results as the basic example, but 
        //  - requires more requests
        //  - less capacity consumed per request
        //  - takes longer
        let selectQuery = "SELECT * FROM Reply 
                           WHERE Id >= 900 
                           AND PostedBy BEGINS WITH \"J\"
                           WITH (PageSize(20))"

        (* ---------------- query using AmazonDynamoDBClient ---------------- *)
        printfn "(AmazonDynamoDBClient) Running basic scan with PageSize :\n\t\t%s" selectQuery
        let response = clientV1.Scan(selectQuery)
        assertThat (response.ScanResult.Count > 0) 
                   "Response should have found some results"
        assertThat (response.ScanResult.Items.Count > 0)
                   "Response should have some items"
        assertThat (response.ScanResult.Items |> Seq.forall (fun attrs -> attrs.["PostedBy"].S.StartsWith "J"))
                   "PostedBy should all start with \"J\""

        (* ---------------- query using DynamoDBContext ---------------- *)
        printfn "(DynamoDBContext) Running basic scan with PageSize :\n\t\t%s" selectQuery
        let replies  = cxtV1.ExecScan<Reply>(selectQuery) |> Seq.toArray
        assertThat (replies.Length > 0) 
                   "Replies should have some items"
        assertThat (replies |> Seq.forall (fun reply -> reply.PostedBy.StartsWith "J"))
                   "Reply.PostedBy should all start with \"J\""

QueryExamples.queryByHashKey()
QueryExamples.queryByHashAndRangeKey()
QueryExamples.queryWithOrderByAndLimit()
QueryExamples.queryWithNoConsistentRead()
QueryExamples.throttlingWithQueryPageSize()
QueryExamples.selectSpecificAttributes()

ScanExamples.basicScan()
ScanExamples.scanWithLimit()
ScanExamples.throttlingWithScanPageSize()