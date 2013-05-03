// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Execution.LowLevel.Tests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Execution

let equal = FsUnit.equal

[<TestFixture>]
type ``Given a V1 DynamoQuery`` () =
    [<Test>]
    member this.``when there is only a hash key equality filter it should return null as RangeKeyCondition`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition = null &&
                               req.AttributesToGet = null &&
                               req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key equality filter it should return RangeKeyCondition with EQ operator`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey = 30"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "EQ" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 1 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "30" &&
                               req.AttributesToGet = null &&
                               req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key greater than filter it should return RangeKeyCondition with GT operator`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey > 30"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "GT" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 1 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "30" &&
                               req.AttributesToGet = null &&
                               req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key in between filter it should return RangeKeyCondition with BETWEEN operator`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                               req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                               req.AttributesToGet = null &&
                               req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a page size option it should be returned as part of the QueryRequest`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 with (pagesize(100))"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 100 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                               req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                               req.AttributesToGet = null &&
                               req.Count = false
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the action is Count the QueryRequest shoul have Count set to true`` () =
        let dynamoQuery = parseDynamoQueryV1 "COUNT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                               req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                               req.AttributesToGet = null &&
                               req.Count = true
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the ASC order is specified the QueryRequest shoul have ScanIndexForward set to true`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 ORDER ASC"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                               req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                               req.AttributesToGet = null &&
                               req.Count = false &&
                               req.ScanIndexForward = true
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the DESC order is specified the QueryRequest shoul have ScanIndexForward set to false`` () =
        let dynamoQuery = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 ORDER DESC"

        match dynamoQuery with
        | GetQueryReq req when req.TableName = "Employees" && req.HashKeyValue.S = "Yan" &&
                               req.Limit = 0 && req.RangeKeyCondition.ComparisonOperator = "BETWEEN" &&
                               req.RangeKeyCondition.AttributeValueList.Count = 2 &&
                               req.RangeKeyCondition.AttributeValueList.[0].N = "5" &&
                               req.RangeKeyCondition.AttributeValueList.[1].N = "25" &&
                               req.AttributesToGet = null &&
                               req.Count = false &&
                               req.ScanIndexForward = false
            -> true
        | _ -> false
        |> should equal true
               
[<TestFixture>]
type ``Given a V1 DynamoScan`` () =
    [<Test>]
    member this.``when there is no where clause it should return a ScanRequest with empty ScanFilter`` () =
        let dynamoQuery = parseDynamoScanV1 "SELECT * FROM Employees"

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
    member this.``when there is a page size option of 100 it should return a ScanRequest with limit set to 100`` () =
        let dynamoQuery = parseDynamoScanV1 "SELECT * FROM Employees with (pagesize(100))"

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
        let dynamoQuery = parseDynamoScanV1 "SELECT * FROM Employees WHERE FirstName = \"Yan\""

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
        let dynamoQuery = parseDynamoScanV1 "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age >= 30"

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
        let dynamoQuery = parseDynamoScanV1 "COUNT * FROM Employees WHERE FirstName = \"Yan\""

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