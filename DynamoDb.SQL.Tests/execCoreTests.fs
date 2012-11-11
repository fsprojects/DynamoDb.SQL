// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Execution.Core.Tests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Execution

let equal = FsUnit.equal

[<TestFixture>]
type ``Given a DynamoQuery`` () =
    [<Test>]
    member this.``when there is only a hash key equality filter it should be interpreted as a Query operation`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | { Where = Some(Where(Query(S "Yan", None))) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key equality filter it should be interpreted as a Query operation`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey = 30"

        match dynamoQuery with
        | { Where = Some(Where(Query(S "Yan", Some(Equal (N 30.0))))) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key greater than filter it should be interpreted as a Query operation`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey > 30"

        match dynamoQuery with
        | { Where = Some(Where(Query(S "Yan", Some(GreaterThan (N 30.0))))) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is a hash key and a range key in filter it should be interpreted as a Query operation`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\" AND @RangeKey between 5 and 25"

        match dynamoQuery with
        | { Where = Some(Where(Query(S "Yan", Some(Between (N 5.0, N 25.0))))) } 
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is one attribute in filter it should be interpreted as a Scan operation`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\""

        match dynamoQuery with
        | { Where = Some(Where(Scan [ ("FirstName", Equal (S "Yan")) ])) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there are multiple attributes in filter it should be interpreted as a Scan operation`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName != \"Cui\" AND Age >= 30"

        match dynamoQuery with
        | { Where = Some(Where(Scan [ ("FirstName", Equal (S "Yan")); ("LastName", NotEqual (S "Cui")); ("Age", GreaterThanOrEqual (N 30.0)) ])) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is only an asterisk (*) in the SELECT clause it should return null as attribtue values`` () =
        let dynamoQuery = parseDynamoQuery "SELECT * FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | { Select = Select(SelectAttributes(null)) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is an asterisk (*) and other attribute names in the SELECT clause it should return null as attribtue values`` () =
        let dynamoQuery = parseDynamoQuery "SELECT *, Name, Age FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | { Select = Select(SelectAttributes(null)) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there is no asterisk (*) in the SELECT clause it should return a list of attribtue values`` () =
        let dynamoQuery = parseDynamoQuery "SELECT Name, Age FROM Employees WHERE @HashKey = \"Yan\""

        match dynamoQuery with
        | { Select = Select(SelectAttributes(lst)) } when lst.Count = 2 && lst.[0] = "Name" && lst.[1] = "Age"
            -> true
        | _ -> false
        |> should equal true