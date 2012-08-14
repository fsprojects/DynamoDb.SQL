module DynamoDb.SQL.ParserTests

open FParsec
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

        match run query select with
        | IsSuccess true & 
          GetSelect ["Name"; "Age"; "Salary" ] &
          GetFrom "Employees"
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the SELECT and FROM keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                      FrOm Employees"

        match run query select with
        | IsSuccess true &
          GetSelect ["Name"; "Age"; "Salary"] &
          GetFrom "Employees"
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a hash key filter is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\""

        match run query select with
        | IsSuccess true &
          GetSelect ["*"] &
          GetFrom "Employees" &
          GetWhere [| (HashKey, Equal, value) |] when unbox value = "Yan"
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a range key is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE @rangekey = \"Yan\""

        match run query select with
        | IsSuccess true &
          GetSelect ["*"] &
          GetFrom "Employees" &
          GetWhere [| (RangeKey, Equal, value) |] when unbox value = "Yan"
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when an attribute name is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE Age = 30"

        match run query select with
        | IsSuccess true &
          GetSelect ["*"] &
          GetFrom "Employees" &
          GetWhere [| (Attribute("Age"), Equal, age) |] when unbox age = 30.0
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

        match run query select with
        | IsSuccess true &
          GetSelect ["*"] &
          GetFrom "Employees" &
          GetWhere [| (Attribute("Age"), LessThan, age1);
                        (Attribute("Age"), LessThanOrEqual, age2);
                        (Attribute("Age"), GreaterThan, age3);
                        (Attribute("Age"), GreaterThanOrEqual, age4) |]
            when unbox age1 = 99.0
            &&   unbox age2 = 90.0
            &&   unbox age3 = 10.0
            &&   unbox age4 = 30.0
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age >= 30 LIMIT 10"

        match run query select with
        | IsSuccess true &
          GetSelect ["*"] &
          GetFrom "Employees" &
          GetWhere [| (Attribute("Age"), GreaterThanOrEqual, age) |] &
          GetLimit 10
            when unbox age = 30.0
            -> true
        | _ -> false
        |> should equal true