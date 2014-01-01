// Author : Yan Cui (twitter @theburningmonk)

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.execCtx.Tests

open System
open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel

[<TestFixture>]
type ``Given a DynamoQuery`` () =
    [<Test>]
    member this.``when there is only an equality filter then Filter should contain a single key condition`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        config.Filter.ToConditions().Count                                  |> should equal 1
        config.Filter.ToConditions().ContainsKey("FirstName")               |> should equal true
        config.Filter.ToConditions().["FirstName"].ComparisonOperator       |> should equal ComparisonOperator.EQ
        config.Filter.ToConditions().["FirstName"].AttributeValueList.Count |> should equal 1
        config.Filter.ToConditions().["FirstName"].AttributeValueList.[0].S |> should equal "Yan"

    [<Test>]
    member this.``when asterisk is used and no Index option is specified then Select should default to AllAttributes`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        config.AttributesToGet  |> should equal null
        config.Select           |> should equal SelectValues.AllAttributes

    [<Test>]
    member this.``when a number of attributes were specified in the SELECT clause then they should be captured in AttributesToGet and Select should be set to SpecificAttributes`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT FirstName, LastName, Age FROM Employees WHERE FirstName = \"Yan\""

        config.AttributesToGet.Count       |> should equal 3
        config.AttributesToGet.ToArray()   |> should equal [| "FirstName"; "LastName"; "Age" |]
        config.Select                      |> should equal SelectValues.SpecificAttributes

    [<Test>]
    member this.``when there are more than one filter condition specified then they should all be captured in Filter`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age BETWEEN 30 AND 40 And LastName BEGINS WITH \"C\""

        config.Filter.ToConditions().Count                                  |> should equal 3

        config.Filter.ToConditions().ContainsKey("FirstName")               |> should equal true
        config.Filter.ToConditions().["FirstName"].AttributeValueList.Count |> should equal 1
        config.Filter.ToConditions().["FirstName"].ComparisonOperator       |> should equal ComparisonOperator.EQ
               
        config.Filter.ToConditions().ContainsKey("Age")                     |> should equal true
        config.Filter.ToConditions().["Age"].AttributeValueList.Count       |> should equal 2
        config.Filter.ToConditions().["Age"].AttributeValueList.[0].N       |> should equal "30"
        config.Filter.ToConditions().["Age"].AttributeValueList.[1].N       |> should equal "40"
        config.Filter.ToConditions().["Age"].ComparisonOperator             |> should equal ComparisonOperator.BETWEEN
           
        config.Filter.ToConditions().ContainsKey("LastName")                |> should equal true
        config.Filter.ToConditions().["LastName"].AttributeValueList.Count  |> should equal 1
        config.Filter.ToConditions().["LastName"].AttributeValueList.[0].S  |> should equal "C"
        config.Filter.ToConditions().["LastName"].ComparisonOperator        |> should equal ComparisonOperator.BEGINS_WITH

    [<Test>]
    member this.``when an Index option is specified with AllAttributes set to true then Select should be set to AllAttributes`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (INDEX(MyIndex, true))"

        config.Select       |> should equal SelectValues.AllAttributes
        config.IndexName    |> should equal "MyIndex"

    [<Test>]
    member this.``when an Index option is specified with AllAttributes set to false then Select should be set to AllProjectedAttributes`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (INDEX(MyIndex, false))"

        config.Select       |> should equal SelectValues.AllProjectedAttributes
        config.IndexName    |> should equal "MyIndex"

    [<Test>]
    member this.``when the ASC order is specified then BackwardSearch should be set to false`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" ORDER ASC"

        config.BackwardSearch   |> should equal false

    [<Test>]
    member this.``when the DESC order is specified then BackwardSearch should be set to true`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" ORDER DESC"

        config.BackwardSearch   |> should equal true

    [<Test>]
    member this.``when the NoConsistentRead option is specified then ConsistentRead should be set to false`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (NoConsistentRead)"

        config.ConsistentRead   |> should equal false

    [<Test>]
    member this.``when no NoConsistentRead option is specified then ConsistentRead should be default to true`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        config.ConsistentRead   |> should equal true

    [<Test>]
    member this.``when the QueryPageSize option is specified to be 5 then Limit should be set to 5`` () =
        let (GetQueryConfig config) = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (PageSize(5))"

        config.Limit    |> should equal 5

    [<Test>]
    [<ExpectedException(typeof<NotSupportedException>)>]
    member this.``when the query is a Count query then it should except with NotSupportedException`` () =
        let query = parseDynamoQuery "COUNT * FROM Employees WHERE FirstName = \"Yan\""

        match query with
        | GetQueryConfig _ -> ()
        |> should throw typeof<NotSupportedException>

[<TestFixture>]
type ``Given a DynamoScan`` () =
    [<Test>]
    member this.``when there is no where clause Filter.ToConditions() should be empty`` () =
        let (GetScanConfigs configs) = parseDynamoScan "SELECT * FROM Employees"

        configs.Length                      |> should equal 1
        let config = configs.[0]
        config.AttributesToGet              |> should equal null
        config.Filter.ToConditions().Count  |> should equal 0

    [<Test>]
    member this.``when a number of attributes are specified then AttributesToGet should contain those attribute names`` () =
        let (GetScanConfigs configs) = parseDynamoScan "SELECT FirstName, LastName, Age FROM Employees"

        configs.Length                      |> should equal 1
        let config = configs.[0]
        config.AttributesToGet.Count        |> should equal 3
        config.AttributesToGet.ToArray()    |> should equal [| "FirstName"; "LastName"; "Age" |]

    [<Test>]
    member this.``when a page size option (100) is specified then Limit should be set to 100`` () =
        let (GetScanConfigs configs) = parseDynamoScan "SELECT * FROM Employees with (pagesize(100))"

        configs.Length                      |> should equal 1
        let config = configs.[0]
        config.AttributesToGet              |> should equal null
        config.Limit                        |> should equal 100
        config.Filter.ToConditions().Count  |> should equal 0

    [<Test>]
    member this.``when there are filter conditions then they should be captured in the Filter`` () =
        let (GetScanConfigs configs) = parseDynamoScan "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age BETWEEN 30 AND 40 AND Title IN (\"Dev\", \"Developer\")"

        configs.Length                      |> should equal 1
        let config = configs.[0]
        config.AttributesToGet |> should equal null
        config.Filter.ToConditions().Count |> should equal 4

        config.Filter.ToConditions().ContainsKey("FirstName")               |> should equal true
        config.Filter.ToConditions().["FirstName"].ComparisonOperator       |> should equal ComparisonOperator.EQ
        config.Filter.ToConditions().["FirstName"].AttributeValueList.Count |> should equal 1
        config.Filter.ToConditions().["FirstName"].AttributeValueList.[0].S |> should equal "Yan"

        config.Filter.ToConditions().ContainsKey("LastName")                |> should equal true
        config.Filter.ToConditions().["LastName"].ComparisonOperator        |> should equal ComparisonOperator.NE
        config.Filter.ToConditions().["LastName"].AttributeValueList.Count  |> should equal 1
        config.Filter.ToConditions().["LastName"].AttributeValueList.[0].S  |> should equal "Cui"

        config.Filter.ToConditions().ContainsKey("Age")                     |> should equal true
        config.Filter.ToConditions().["Age"].ComparisonOperator             |> should equal ComparisonOperator.BETWEEN
        config.Filter.ToConditions().["Age"].AttributeValueList.Count       |> should equal 2
        config.Filter.ToConditions().["Age"].AttributeValueList.[0].N       |> should equal "30"
        config.Filter.ToConditions().["Age"].AttributeValueList.[1].N       |> should equal "40"

        config.Filter.ToConditions().ContainsKey("Title")                   |> should equal true
        config.Filter.ToConditions().["Title"].ComparisonOperator           |> should equal ComparisonOperator.IN
        config.Filter.ToConditions().["Title"].AttributeValueList.Count     |> should equal 2
        config.Filter.ToConditions().["Title"].AttributeValueList.[0].S     |> should equal "Dev"
        config.Filter.ToConditions().["Title"].AttributeValueList.[1].S     |> should equal "Developer"
    
    [<Test>]
    member this.``when a segments option (15) is specified then number of configs should be 15`` () =
        let (GetScanConfigs configs) = parseDynamoScan "SELECT * FROM Employees WITH ( SEGMENTS ( 15 ) )"

        configs.Length                  |> should equal 15        
        for n = 0 to 14 do
            configs.[n].Segment         |> should equal n
            configs.[n].TotalSegments   |> should equal 15

    [<Test>]
    [<ExpectedException(typeof<NotSupportedException>)>]
    member this.``when the action is Count then it should except with NotSupportedException`` () =
        let scan = parseDynamoScan "COUNT * FROM Employees with (pagesize(100))"

        match scan with
        | GetScanConfigs _ -> ()
        |> should throw typeof<NotSupportedException>