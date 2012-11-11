// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Execution.LowLevel.Tests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Execution

let equal = FsUnit.equal

[<TestFixture>]
type ``Given a DynamoQuery`` () =
    [<Test>]
    member this.``when there is only a hash key equality filter it should return a QueryRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | IsQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                              req.Limit = 0 && req.RangeKeyCondition = null &&
                              req.AttributesToGet = null
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key equality filter it should return a QueryRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey = 30"

        match dynamoQuery with
        | IsQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                              req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "EQ" &&
                              req.RangeKeyCondition.AttributeValueList.Count = 1 &&
                              req.RangeKeyCondition.AttributeValueList.[0].N = "30" &&
                              req.AttributesToGet = null
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key greater than filter it should return a QueryRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey > 30"

        match dynamoQuery with
        | IsQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                              req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "GT" &&
                              req.RangeKeyCondition.AttributeValueList.Count = 1 &&
                              req.RangeKeyCondition.AttributeValueList.[0].N = "30" &&
                              req.AttributesToGet = null
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key in filter it should return a QueryRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        match dynamoQuery with
        | IsQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                              req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                              req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                              req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                              req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                              req.AttributesToGet = null
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is one attribute in filter it should return a ScanRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        match dynamoQuery with
        | IsScanReq req when req.TableName = "Employees" &&
                             req.Limit = 0 &&
                             req.AttributesToGet = null &&
                             req.ScanFilter.Count = 1 &&
                             req.ScanFilter.["FirstName"].ComparisonOperator = "EQ" &&
                             req.ScanFilter.["FirstName"].AttributeValueList.Count = 1 &&
                             req.ScanFilter.["FirstName"].AttributeValueList.[0].S = "Yan"
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there are multiple attributes in filter it should return a ScanRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age >= 30"

        match dynamoQuery with
        | IsScanReq req when req.TableName = "Employees" &&
                             req.Limit = 0 &&
                             req.AttributesToGet = null &&
                             req.ScanFilter.Count = 3 &&
                             req.ScanFilter.["FirstName"].ComparisonOperator = "EQ" &&
                             req.ScanFilter.["FirstName"].AttributeValueList.Count = 1 &&
                             req.ScanFilter.["FirstName"].AttributeValueList.[0].S = "Yan" &&
                             req.ScanFilter.["LastName"].ComparisonOperator = "NE" &&
                             req.ScanFilter.["LastName"].AttributeValueList.Count = 1 &&
                             req.ScanFilter.["LastName"].AttributeValueList.[0].S = "Cui" &&
                             req.ScanFilter.["Age"].ComparisonOperator = "GE" &&
                             req.ScanFilter.["Age"].AttributeValueList.Count = 1 &&
                             req.ScanFilter.["Age"].AttributeValueList.[0].N = "30"
            -> true
        | _ -> false
        |> should equal true