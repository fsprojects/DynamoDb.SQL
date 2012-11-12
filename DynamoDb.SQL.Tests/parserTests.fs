// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.ParserTests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser

let equal = FsUnit.equal

[<TestFixture>]
type ``Given a select query`` () =
    [<Test>]
    member this.``when there is white spaces around the attribute names and table name they should be ignored`` () =
        let select = "SELECT Name,    Age,
                             Salary
                      FROM   Employees "

        match parseDynamoQuery select with
        | { Select = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ];
            From = From "Employees" }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the SELECT and FROM keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                      FrOm Employees"

        match parseDynamoQuery select with
        | { Select = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ];
            From = From "Employees" }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a hash key filter is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\""

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (HashKey, Equal(S "Yan")) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a range key is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE @rangekey = \"Yan\""

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (RangeKey, Equal(S "Yan")) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when an attribute name is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE Age = 30"

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Age"), Equal(N 30.0)) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the != operator is used it should be parsed as correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey != \"Yan\""

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (HashKey, NotEqual(S "Yan")) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when <, <=, >, >= operators are used, they should be parsed correctly`` () =
        let select = "SELECT * FROM Employees 
                      WHERE Age < 99
                      AND   Age <= 90
                      AND   Age > 10
                      AND   Age >= 30 "

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where ([ (Attribute("Age"), LessThan(N 99.0));
                                  (Attribute("Age"), LessThanOrEqual(N 90.0));
                                  (Attribute("Age"), GreaterThan(N 10.0));
                                  (Attribute("Age"), GreaterThanOrEqual(N 30.0)) ])) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Contains operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Name CONTAINS \"Yan\""

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Name"), Contains(S "Yan")) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the NotContains operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Name NOT CONTAINS \"Yan\""

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Name"), NotContains(S "Yan")) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Begins With operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Name BEGINS WITH \"Yan\""

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Name"), BeginsWith(S "Yan")) ]) }
            -> true
        | _ -> false
        |> should equal true
    
    [<Test>]
    member this.``when the Between operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age BETWEEN 10 AND 30"

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Age"), Between(N 10.0, N 30.0)) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the In operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Name IN (\"Foo\", \"Bar\") 
                                                AND Age IN (99, 98, 97)"

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Name"), In([ S "Foo"; S "Bar"]));
                                 (Attribute("Age"),  In([ N  99.0; N 98.0; N 97.0])) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Is Null operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE NickName IS NULL"

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("NickName"), Null) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Is Not Null operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE NickName IS NOT NULL"

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("NickName"), NotNull) ]) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age >= 30 LIMIT 10"

        match parseDynamoQuery select with
        | { Select = Select [ Asterisk ]; From = From "Employees";
            Where = Some(Where [ (Attribute("Age"), GreaterThanOrEqual(N 30.0)) ]);
            Limit = Some(Limit 10) }
            -> true
        | _ -> false
        |> should equal true