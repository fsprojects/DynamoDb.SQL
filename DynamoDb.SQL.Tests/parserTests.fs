// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.ParserTests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser

let equal = FsUnit.equal

[<TestFixture>]
type ``Given a V1 query`` () =
    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is no attributes in the select clause it should except`` () =
        let select = "SELECT FROM Employees WHERE @hashkey = \"Yan\""
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidTableName>)>]
    member this.``when there is no table name in the from clause it should except`` () =
        let select = "SELECT * FROM WHERE @hashkey = \"Yan\""
        parseDynamoQueryV1 select |> should throw typeof<InvalidTableName>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is no where clause it should except`` () =
        let select = "SELECT * FROM Employees"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is no filter conditions in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when there is white spaces around the attribute names and table name they should be ignored`` () =
        let select = "SELECT Name,    Age,
                             Salary
                      FROM   Employees 
                        WHERE @HashKey   =      \"Yan\"
                      LIMIT     5"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")) ]
            Limit   = Some(Limit 5) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the SELECT, FROM, WHERE and LIMIT keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                      FrOm Employees
                      where @hAshkeY = \"Yan\"
                      liMIt 5"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")) ]
            Limit   = Some(Limit 5) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a range key is included in a filter condition it should be parsed`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey = \"Cui\""

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal(S "Yan")); (RangeKey, Equal(S "Cui")) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when an attribute name is included in a filter condition it should except`` () =
        let select = "SELECT * FROM Employees WHERE Age = 30"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the != operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey != 30"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when < operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey < 99"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, LessThan (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when <= operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey <= 99"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, LessThanOrEqual (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when > operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey > 99"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, GreaterThan (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when >= operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey >= 99"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, GreaterThanOrEqual (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the Contains operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey CONTAINS \"Cui\""
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the NotContains operator is used it except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey NOT CONTAINS \"Cui\""
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when the Begins With operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey BEGINS WITH \"Cui\""

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, BeginsWith (S "Cui")) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true
    
    [<Test>]
    member this.``when the Between operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey BETWEEN 10 AND 30"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, Between ((N 10.0), (N 30.0))) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the In operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey IN (\"Foo\", \"Bar\")"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the Is Null operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey IS NULL"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the Is Not Null operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey IS NOT NULL"
        parseDynamoQueryV1 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey >= 30 LIMIT 10"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when order asc is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey >= 30 ORDER ASC LIMIT 10"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10);
            Order   = Some(Asc) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when order desc is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey >= 30 ORDER DESC LIMIT 10"

        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10);
            Order   = Some(Desc) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a count query is specified, it should be parsed correctly`` () =
        let count = "COUNT * FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey >= 30 ORDER DESC LIMIT 10"

        match parseDynamoQueryV1 count with
        | { Action  = Count
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")); (RangeKey, GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10);
            Order   = Some(Desc) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when a count query is specified with attribute names, it should except`` () =
        let count = "COUNT FirstName FROM Employees WHERE @hashkey = \"Yan\" AND @rangekey >= 30 ORDER DESC LIMIT 10"
        parseDynamoQueryV1 count |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when NoConsistentRead option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" WITH (  nOConsiStentRead )"
        
        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")) ]
            Options = Some [| NoConsistentRead |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when PageSize option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" WITH (Pagesize(  10) )"
        
        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")) ]
            Options = Some [| QueryPageSize 10 |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when both NoConsistentRead and PageSize options are specified they should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\" WITH ( NOconsistentRead, Pagesize(  10) )"
        
        match parseDynamoQueryV1 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (HashKey, Equal (S "Yan")) ]
            Options = Some [| NoConsistentRead; QueryPageSize 10 |] }
            -> true
        | _ -> false
        |> should equal true

[<TestFixture>]
type ``Given a V2 query`` () =
    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is no attributes in the select clause it should except`` () =
        let select = "SELECT FROM Employees WHERE FirstName = \"Yan\""
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidTableName>)>]
    member this.``when there is no table name in the from clause it should except`` () =
        let select = "SELECT * FROM WHERE FirstName = \"Yan\""
        parseDynamoQueryV2 select |> should throw typeof<InvalidTableName>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is no where clause it should except`` () =
        let select = "SELECT * FROM Employees"
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is no filter conditions in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE"
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is a HashKey keyword in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE @HashKey = \"Yan\""
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when there is a RangeKey keyword in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE @RangeKey = \"Yan\""
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when there is white spaces around the attribute names and table name they should be ignored`` () =
        let select = "SELECT Name,    Age,
                             Salary
                      FROM   Employees 
                        WHERE FirstName   =      \"Yan\"
                      LIMIT     5"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Limit   = Some(Limit 5) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the SELECT, FROM, WHERE and LIMIT keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                      FrOm Employees
                      where FirstName = \"Yan\"
                      liMIt 5"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Limit   = Some(Limit 5) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when there are multiple conditions in the where clause they should all be parsed`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName = \"Cui\""

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal(S "Yan")); (Attribute "LastName", Equal(S "Cui")) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the != operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age != 30"
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when < operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age < 99"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", LessThan (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when <= operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age <= 99"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", LessThanOrEqual (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when > operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age > 99"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThan (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when >= operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 99"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual (N 99.0)) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the Contains operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName CONTAINS \"Cui\""
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the NotContains operator is used it except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName NOT CONTAINS \"Cui\""
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when the Begins With operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName BEGINS WITH \"Cui\""

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "LastName", BeginsWith (S "Cui")) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true
    
    [<Test>]
    member this.``when the Between operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age BETWEEN 10 AND 30"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", Between ((N 10.0), (N 30.0))) ]
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the In operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age IN (\"Foo\", \"Bar\")"
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the Is Null operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName IS NULL"
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when the Is Not Null operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName IS NOT NULL"
        parseDynamoQueryV2 select |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 LIMIT 10"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when order asc is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER ASC LIMIT 10"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10);
            Order   = Some(Asc) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when order desc is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10);
            Order   = Some(Desc) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a count query is specified, it should be parsed correctly`` () =
        let count = "COUNT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"

        match parseDynamoQueryV2 count with
        | { Action  = Count
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(N 30.0)) ]
            Limit   = Some(Limit 10);
            Order   = Some(Desc) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidQuery>)>]
    member this.``when a count query is specified with attribute names, it should except`` () =
        let count = "COUNT FirstName FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"
        parseDynamoQueryV2 count |> should throw typeof<InvalidQuery>

    [<Test>]
    member this.``when NoConsistentRead option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (  nOConsiStentRead )"
        
        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Options = Some [| NoConsistentRead |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when PageSize option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Pagesize(  10) )"
        
        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Options = Some [| QueryPageSize 10 |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when Index option is specified with AllAttributes set to true it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Index( _M-y.1nd3x ,  true) )"
        
        let res = parseDynamoQueryV2 select

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Options = Some [| Index("_M-y.1nd3x", true) |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when Index option is specified with AllAttributes set to false it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Index( _M-y.1nd3x ,  false) )"
        
        let res = parseDynamoQueryV2 select

        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Options = Some [| Index("_M-y.1nd3x", false) |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when NoReturnedCapacity option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH ( NoReturnedCapacity)"
        
        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Options = Some [| NoReturnedCapacity |] }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when both NoConsistentRead and PageSize options are specified they should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH ( NOconsistentRead, Pagesize(  10) )"
        
        match parseDynamoQueryV2 select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
            Options = Some [| NoConsistentRead; QueryPageSize 10 |] }
            -> true
        | _ -> false
        |> should equal true

[<TestFixture>]
type ``Given a scan`` () =
    [<Test>]
    [<ExpectedException(typeof<InvalidScan>)>]
    member this.``when there is no attributes in the select clause it should except`` () =
        let select = "SELECT FROM Employees WHERE @hashkey = \"Yan\""
        parseDynamoScan select |> should throw typeof<InvalidScan>

    [<Test>]
    [<ExpectedException(typeof<InvalidTableName>)>]
    member this.``when there is no table name in the from clause it should except`` () =
        let select = "SELECT * FROM WHERE FirstName = \"Yan\""
        parseDynamoScan select |> should throw typeof<InvalidTableName>

    [<Test>]   
    member this.``when there is no where clause it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees"

        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = None
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidScan>)>]
    member this.``when there is no filter conditions in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE"
        parseDynamoScan select |> should throw typeof<InvalidScan>

    [<Test>]
    member this.``when there is white spaces around the attribute names and table name they should be ignored`` () =
        let select = "SELECT Name,    Age,
                             Salary
                      FROM   Employees 
                        WHERE FirstName   =      \"Yan\"
                      LIMIT     5"

        match parseDynamoScan select with
        | { Action  = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", Equal (S "Yan")) ])
            Limit   = Some(Limit 5) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the SELECT, FROM, WHERE and LIMIT keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                      FrOm Employees
                      where FirstName = \"Yan\"
                      liMIt 5"

        match parseDynamoScan select with
        | { Action  = Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", Equal (S "Yan")) ])
            Limit   = Some(Limit 5) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidScan>)>]
    member this.``when a hash key is included it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\""
        parseDynamoScan select |> should throw typeof<InvalidScan>

    [<Test>]
    [<ExpectedException(typeof<InvalidScan>)>]
    member this.``when a range key is included it should except`` () =
        let select = "SELECT * FROM Employees WHERE @rangekey = \"Cui\""
        parseDynamoScan select |> should throw typeof<InvalidScan>

    [<Test>]
    member this.``when an attribute name is included in a filter condition it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age = 30"

        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "Age", Equal (N 30.0)) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the != operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName != \"Yan\""
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", NotEqual (S "Yan")) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when <, <=, >, >= operators are used they should be parsed correctly`` () =
        let select = "SELECT * FROM Employees 
                      WHERE Age >= 10 
                      AND   Age > 20
                      AND   Age <= 99
                      AND   Age < 90"

        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "Age", GreaterThanOrEqual(N 10.0))
                                   (Attribute "Age", GreaterThan(N 20.0))
                                   (Attribute "Age", LessThanOrEqual(N 99.0))
                                   (Attribute "Age", LessThan(N 90.0))])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Contains operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName CONTAINS \"Yan\""
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", Contains(S "Yan")) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the NotContains operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName NOT CONTAINS \"Yan\""
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", NotContains(S "Yan")) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Begins With operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName BEGINS WITH \"Yan\""

        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", BeginsWith (S "Yan")) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true
    
    [<Test>]
    member this.``when the Between operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age BETWEEN 10 AND 30"

        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "Age", Between ((N 10.0), (N 30.0))) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the In operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age IN (10, 30, 50)"
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "Age", In [ N 10.0; N 30.0; N 50.0 ]) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Is Null operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE LastName IS NULL"
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "LastName", Null) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when the Is Not Null operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE LastName IS NOT NULL"
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "LastName", NotNull) ])
            Limit   = None }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 LIMIT 10"

        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(N 30.0)) ])
            Limit   = Some(Limit 10) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    member this.``when a count query is specified, it should be parsed correctly`` () =
        let count = "COUNT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 LIMIT 10"

        match parseDynamoScan count with
        | { Action  = Count
            From    = From "Employees"
            Where   = Some(Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(N 30.0)) ])
            Limit   = Some(Limit 10) }
            -> true
        | _ -> false
        |> should equal true

    [<Test>]
    [<ExpectedException(typeof<InvalidScan>)>]
    member this.``when a count query is specified with attribute names, it should except`` () =
        let count = "COUNT FirstName FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"
        parseDynamoScan count |> should throw typeof<InvalidScan>

    [<Test>]
    member this.``when PageSize option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WITH (Pagesize(  10) )"
        
        match parseDynamoScan select with
        | { Action  = Select [ Asterisk ]
            From    = From "Employees"
            Where   = None
            Options = Some [| ScanPageSize 10 |] }
            -> true
        | _ -> false
        |> should equal true