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
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        req.TableName           |> should equal "Employees" 
        req.HashKeyValue.S      |> should equal "Yan"
        req.Limit               |> should equal 0 
        req.RangeKeyCondition   |> should equal null
        req.AttributesToGet     |> should equal null
        req.Count               |> should equal false

    [<Test>]
    member this.``when there is a hash key and a range key equality filter it should return RangeKeyCondition with EQ operator`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey = 30"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 0
        req.RangeKeyCondition.ComparisonOperator        |> should equal "EQ"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 1
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "30"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal false

    [<Test>]
    member this.``when there is a hash key and a range key greater than filter it should return RangeKeyCondition with GT operator`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey > 30"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 0
        req.RangeKeyCondition.ComparisonOperator        |> should equal "GT"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 1
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "30"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal false

    [<Test>]
    member this.``when there is a hash key and a range key in between filter it should return RangeKeyCondition with BETWEEN operator`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 0
        req.RangeKeyCondition.ComparisonOperator        |> should equal "BETWEEN"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 2
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "5"
        req.RangeKeyCondition.AttributeValueList.[1].N  |> should equal "25"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal false

    [<Test>]
    member this.``when there is a page size option it should be returned as part of the QueryRequest`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 with (pagesize(100))"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 100
        req.RangeKeyCondition.ComparisonOperator        |> should equal "BETWEEN"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 2
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "5"
        req.RangeKeyCondition.AttributeValueList.[1].N  |> should equal "25"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal false

    [<Test>]
    member this.``when the action is Count the QueryRequest shoul have Count set to true`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "COUNT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 0
        req.RangeKeyCondition.ComparisonOperator        |> should equal "BETWEEN"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 2
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "5"
        req.RangeKeyCondition.AttributeValueList.[1].N  |> should equal "25"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal true

    [<Test>]
    member this.``when the ASC order is specified the QueryRequest shoul have ScanIndexForward set to true`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 ORDER ASC"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 0
        req.RangeKeyCondition.ComparisonOperator        |> should equal "BETWEEN"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 2
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "5"
        req.RangeKeyCondition.AttributeValueList.[1].N  |> should equal "25"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal false
        req.ScanIndexForward                            |> should equal true

    [<Test>]
    member this.``when the DESC order is specified the QueryRequest shoul have ScanIndexForward set to false`` () =
        let (GetQueryReq req) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25 ORDER DESC"

        req.TableName                                   |> should equal "Employees"
        req.HashKeyValue.S                              |> should equal "Yan"
        req.Limit                                       |> should equal 0 
        req.RangeKeyCondition.ComparisonOperator        |> should equal "BETWEEN"
        req.RangeKeyCondition.AttributeValueList.Count  |> should equal 2
        req.RangeKeyCondition.AttributeValueList.[0].N  |> should equal "5"
        req.RangeKeyCondition.AttributeValueList.[1].N  |> should equal "25"
        req.AttributesToGet                             |> should equal null
        req.Count                                       |> should equal false
        req.ScanIndexForward                            |> should equal false
               
[<TestFixture>]
type ``Given a V1 DynamoScan`` () =
    [<Test>]
    member this.``when there is no where clause it should return a ScanRequest with empty ScanFilter`` () =
        let (GetScanReq req) = parseDynamoScanV1 "SELECT * FROM Employees"

        req.TableName           |> should equal "Employees"
        req.Limit               |> should equal 0
        req.AttributesToGet     |> should equal null
        req.ScanFilter.Count    |> should equal 0
        req.Count               |> should equal false

    [<Test>]
    member this.``when there is a page size option of 100 it should return a ScanRequest with limit set to 100`` () =
        let (GetScanReq req) = parseDynamoScanV1 "SELECT * FROM Employees with (pagesize(100))"

        req.TableName           |> should equal "Employees"
        req.Limit               |> should equal 100
        req.AttributesToGet     |> should equal null
        req.ScanFilter.Count    |> should equal 0
        req.Count               |> should equal false

    [<Test>]
    member this.``when there is one attribute in filter it should return a ScanRequest with a ScanFilter of 1`` () =
        let (GetScanReq req) = parseDynamoScanV1 "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        req.TableName                                           |> should equal "Employees"
        req.Limit                                               |> should equal 0
        req.AttributesToGet                                     |> should equal null
        req.ScanFilter.Count                                    |> should equal 1
        req.ScanFilter.["FirstName"].ComparisonOperator         |> should equal "EQ"
        req.ScanFilter.["FirstName"].AttributeValueList.Count   |> should equal 1
        req.ScanFilter.["FirstName"].AttributeValueList.[0].S   |> should equal "Yan"
        req.Count                                               |> should equal false

    [<Test>]
    member this.``when there are 3 attributes in filter it should return a ScanRequest with ScanFilter of 3`` () =
        let (GetScanReq req) = parseDynamoScanV1 "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age >= 30"

        req.TableName                                           |> should equal "Employees"
        req.Limit                                               |> should equal 0
        req.AttributesToGet                                     |> should equal null
        req.ScanFilter.Count                                    |> should equal 3
        req.ScanFilter.["FirstName"].ComparisonOperator         |> should equal "EQ"
        req.ScanFilter.["FirstName"].AttributeValueList.Count   |> should equal 1
        req.ScanFilter.["FirstName"].AttributeValueList.[0].S   |> should equal "Yan"
        req.ScanFilter.["LastName"].ComparisonOperator          |> should equal "NE"
        req.ScanFilter.["LastName"].AttributeValueList.Count    |> should equal 1
        req.ScanFilter.["LastName"].AttributeValueList.[0].S    |> should equal "Cui"
        req.ScanFilter.["Age"].ComparisonOperator               |> should equal "GE"
        req.ScanFilter.["Age"].AttributeValueList.Count         |> should equal 1
        req.ScanFilter.["Age"].AttributeValueList.[0].N         |> should equal "30"
        req.Count                                               |> should equal false
        
    [<Test>]
    member this.``when the action is Count it should return a ScanRequest with Count set to true`` () =
        let (GetScanReq req) = parseDynamoScanV1 "COUNT * FROM Employees WHERE FirstName = \"Yan\""

        req.TableName                                           |> should equal "Employees"
        req.Limit                                               |> should equal 0
        req.AttributesToGet                                     |> should equal null
        req.ScanFilter.Count                                    |> should equal 1
        req.ScanFilter.["FirstName"].ComparisonOperator         |> should equal "EQ"
        req.ScanFilter.["FirstName"].AttributeValueList.Count   |> should equal 1
        req.ScanFilter.["FirstName"].AttributeValueList.[0].S   |> should equal "Yan"
        req.Count                                               |> should equal true