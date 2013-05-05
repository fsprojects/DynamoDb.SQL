// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Execution.Helper.Tests

open System
open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Execution

let equal = FsUnit.equal

[<TestFixture>]
type ``Given a V1 DynamoQuery`` () =
    [<Test>]
    member this.``when there is only a hash key equality filter then Filter.Condition should be null`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        config.HashKey.AsString()   |> should equal "Yan"
        config.Filter.Condition     |> should equal null

    [<Test>]
    member this.``when asterisk is used then AttributesToGet should default to null`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        config.HashKey.AsString()   |> should equal "Yan"
        config.AttributesToGet      |> should equal null

    [<Test>]
    member this.``when a number of attributes are specified then AttributesToGet should contain those attribute names`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT FirstName, LastName, Age FROM Employees WHERE @HashKey = \"Yan\""

        config.HashKey.AsString()           |> should equal "Yan"
        config.AttributesToGet.Count        |> should equal 3
        config.AttributesToGet.ToArray()    |> should equal [| "FirstName"; "LastName"; "Age" |]

    [<Test>]
    member this.``when there are both hash key and range key filters then Filter.Condition should capture the range key filter`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey BEGINS WITH \"C\""

        config.HashKey.AsString()                           |> should equal "Yan"
        config.Filter.Condition.ComparisonOperator          |> should equal "BEGINS_WITH"
        config.Filter.Condition.AttributeValueList.Count    |> should equal 1
        config.Filter.Condition.AttributeValueList.[0].S    |> should equal "C"

    [<Test>]
    member this.``when the page size (100) option is specified then Limit should be set to 100`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" WITH (PageSize(100))"

        config.HashKey.AsString()   |> should equal "Yan"
        config.Limit                |> should equal 100

    [<Test>]
    member this.``when the no consistent read option is specified then ConsistentRead should be set to false`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" WITH (NoConsistentRead)"

        config.HashKey.AsString()   |> should equal "Yan"
        config.ConsistentRead       |> should equal false

    [<Test>]
    member this.``when the ASC order is specified then BackwardSearch should be set to false`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" ORDER ASC"

        config.HashKey.AsString()   |> should equal "Yan"
        config.BackwardSearch       |> should equal false

    [<Test>]
    member this.``when the DESC order is specified then BackwardSearch should be set to true`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\" ORDER DESC"

        config.HashKey.AsString()   |> should equal "Yan"
        config.BackwardSearch       |> should equal true

    [<Test>]
    member this.``when the no consistent read option is not specified then ConsistentRead should default to true`` () =
        let (GetQueryConfig config) = parseDynamoQueryV1 "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        config.HashKey.AsString()   |> should equal "Yan"
        config.ConsistentRead       |> should equal true

    [<Test>]
    [<ExpectedException(typeof<NotSupportedException>)>]
    member this.``when the query is Count then it should except with NotSupportedException`` () =
        let query = parseDynamoQueryV1 "COUNT * FROM Employees WHERE @HashKey = \"Yan\" WITH (PageSize(100))"

        match query with
        | GetQueryConfig _ -> ()
        |> should throw typeof<NotSupportedException>

[<TestFixture>]
type ``Given a V1 DynamoScan`` () =
    [<Test>]
    member this.``when there is no where clause Filter.ToConditions() should be empty`` () =
        let (GetScanConfig config) = parseDynamoScanV1 "SELECT * FROM Employees"

        config.AttributesToGet              |> should equal null
        config.Filter.ToConditions().Count  |> should equal 0

    [<Test>]
    member this.``when a number of attributes are specified then AttributesToGet should contain those attribute names`` () =
        let (GetScanConfig config) = parseDynamoScanV1 "SELECT FirstName, LastName, Age FROM Employees"

        config.AttributesToGet.Count        |> should equal 3
        config.AttributesToGet.ToArray()    |> should equal [| "FirstName"; "LastName"; "Age" |]

    [<Test>]
    member this.``when a page size option (100) is specified then Limit should be set to 100`` () =
        let (GetScanConfig config) = parseDynamoScanV1 "SELECT * FROM Employees with (pagesize(100))"

        config.AttributesToGet              |> should equal null
        config.Limit                        |> should equal 100
        config.Filter.ToConditions().Count  |> should equal 0

    [<Test>]
    member this.``when there are filter conditions then they should be captured in the Filter`` () =
        let (GetScanConfig config) = parseDynamoScanV1 "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age BETWEEN 30 AND 40 AND Title IN (\"Dev\", \"Developer\")"

        config.AttributesToGet |> should equal null
        config.Filter.ToConditions().Count |> should equal 4

        config.Filter.ToConditions().ContainsKey("FirstName")               |> should equal true
        config.Filter.ToConditions().["FirstName"].ComparisonOperator       |> should equal "EQ"
        config.Filter.ToConditions().["FirstName"].AttributeValueList.Count |> should equal 1
        config.Filter.ToConditions().["FirstName"].AttributeValueList.[0].S |> should equal "Yan"

        config.Filter.ToConditions().ContainsKey("LastName")                |> should equal true
        config.Filter.ToConditions().["LastName"].ComparisonOperator        |> should equal "NE"
        config.Filter.ToConditions().["LastName"].AttributeValueList.Count  |> should equal 1
        config.Filter.ToConditions().["LastName"].AttributeValueList.[0].S  |> should equal "Cui"

        config.Filter.ToConditions().ContainsKey("Age")                     |> should equal true
        config.Filter.ToConditions().["Age"].ComparisonOperator             |> should equal "BETWEEN"
        config.Filter.ToConditions().["Age"].AttributeValueList.Count       |> should equal 2
        config.Filter.ToConditions().["Age"].AttributeValueList.[0].N       |> should equal "30"
        config.Filter.ToConditions().["Age"].AttributeValueList.[1].N       |> should equal "40"

        config.Filter.ToConditions().ContainsKey("Title")                   |> should equal true
        config.Filter.ToConditions().["Title"].ComparisonOperator           |> should equal "IN"
        config.Filter.ToConditions().["Title"].AttributeValueList.Count     |> should equal 2
        config.Filter.ToConditions().["Title"].AttributeValueList.[0].S     |> should equal "Dev"
        config.Filter.ToConditions().["Title"].AttributeValueList.[1].S     |> should equal "Developer"
    
    [<Test>]
    [<ExpectedException(typeof<NotSupportedException>)>]
    member this.``when the action is Count then it should except with NotSupportedException`` () =
        let scan = parseDynamoScanV1 "COUNT * FROM Employees with (pagesize(100))"

        match scan with
        | GetScanConfig _ -> ()
        |> should throw typeof<NotSupportedException>