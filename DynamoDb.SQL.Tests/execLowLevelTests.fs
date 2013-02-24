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
    member this.``when there is only a hash key equality filter it should null as RangeKeyCondition`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | GetQueryReq true req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 0 && req.RangeKeyCondition = null &&
                 req.AttributesToGet = null &&
                 req.Count = false &&
                 req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key equality filter it should return RangeKeyCondition with EQ operator`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey = 30"

        match dynamoQuery with
        | GetQueryReq true req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "EQ" &&
                 req.RangeKeyCondition.AttributeValueList.Count = 1 &&
                 req.RangeKeyCondition.AttributeValueList.[0].N = "30" &&
                 req.AttributesToGet = null &&
                 req.Count = false &&
                 req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key greater than filter it should return RangeKeyCondition with GT operator`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey > 30"

        match dynamoQuery with
        | GetQueryReq true req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "GT" &&
                 req.RangeKeyCondition.AttributeValueList.Count = 1 &&
                 req.RangeKeyCondition.AttributeValueList.[0].N = "30" &&
                 req.AttributesToGet = null &&
                 req.Count = false &&
                 req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key in between filter it should return RangeKeyCondition with BETWEEN operator`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        match dynamoQuery with
        | GetQueryReq true req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                 req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                 req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                 req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                 req.AttributesToGet = null &&
                 req.Count = false &&
                 req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a limit it should be returned as part of the QueryRequest`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 Limit 100"

        match dynamoQuery with
        | GetQueryReq true req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 100 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                 req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                 req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                 req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                 req.AttributesToGet = null &&
                 req.Count = false &&
                 req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when specified to not use consistent read the returned QueryRequest should have consistent read set to false`` () =
        let dynamoQuery = parseDynamoQuery "COUNT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 Limit 100"

        match dynamoQuery with
        | GetQueryReq false req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 100 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                 req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                 req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                 req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                 req.AttributesToGet = null &&
                 req.Count = true &&
                 not req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when `` () =
        let dynamoQuery = parseDynamoQuery "COUNT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 Limit 100"

        match dynamoQuery with
        | GetQueryReq true req 
            when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                 req.Limit = 100 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                 req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                 req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                 req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                 req.AttributesToGet = null &&
                 req.Count = true &&
                 req.ConsistentRead
            -> true
        | _ -> false
        |> should equal true

[<TestFixture>]
type ``Given a DynamoScan`` () =
    [<Test>]
    member this.``when there is no where clause it should return a ScanRequest with empty ScanFilter`` () =
        let dynamoQuery = parseDynamoScan "SELECT * FROM Employees"

        match dynamoQuery with
        | GetScanReq req when req.TableName = "Employees" &&
                              req.Limit = 0 &&
                              req.AttributesToGet = null &&
                              req.ScanFilter.Count = 0 &&
                              req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a limit of 100 it should return a ScanRequest with limit set to 100`` () =
        let dynamoQuery = parseDynamoScan "SELECT * FROM Employees LIMIT 100"

        match dynamoQuery with
        | GetScanReq req when req.TableName = "Employees" &&
                              req.Limit = 100 &&
                              req.AttributesToGet = null &&
                              req.ScanFilter.Count = 0 &&
                              req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is one attribute in filter it should return a ScanRequest with a ScanFilter of 1`` () =
        let dynamoQuery = parseDynamoScan "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        match dynamoQuery with
        | GetScanReq req when req.TableName = "Employees" &&
                              req.Limit = 0 &&
                              req.AttributesToGet = null &&
                              req.ScanFilter.Count = 1 &&
                              req.ScanFilter.["FirstName"].ComparisonOperator = "EQ" &&
                              req.ScanFilter.["FirstName"].AttributeValueList.Count = 1 &&
                              req.ScanFilter.["FirstName"].AttributeValueList.[0].S = "Yan" &&
                              req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there are 3 attributes in filter it should return a ScanRequest with ScanFilter of 3`` () =
        let dynamoQuery = parseDynamoScan "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age >= 30"

        match dynamoQuery with
        | GetScanReq req when req.TableName = "Employees" &&
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
                              req.ScanFilter.["Age"].AttributeValueList.[0].N = "30" &&
                              req.Count = false
            -> true
        | _ -> false
        |> should equal true
        
    [<Test>]
    member this.``when the action is Count it should return a ScanRequest with Count set to true`` () =
        let dynamoQuery = parseDynamoScan "COUNT * FROM Employees WHERE FirstName = \"Yan\""

        match dynamoQuery with
        | GetScanReq req when req.TableName = "Employees" &&
                              req.Limit = 0 &&
                              req.AttributesToGet = null &&
                              req.ScanFilter.Count = 1 &&
                              req.ScanFilter.["FirstName"].ComparisonOperator = "EQ" &&
                              req.ScanFilter.["FirstName"].AttributeValueList.Count = 1 &&
                              req.ScanFilter.["FirstName"].AttributeValueList.[0].S = "Yan" &&
                              req.Count = true
            -> true
        | _ -> false
        |> should equal true