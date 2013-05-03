// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Parser.Tests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser

module V2Tests = 
    let equal = FsUnit.equal

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
        member this.``when NoConsistentRead option is specified it should be captured in the Options clause`` () =
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
        member this.``when PageSize option is specified it should be captured in the Options clause`` () =
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
        member this.``when Index option is specified with AllAttributes set to true it should be captured in the Options clause`` () =
            let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Index( _M-y.1nd3x ,  true) )"
        
            match parseDynamoQueryV2 select with
            | { Action  = Select [ Asterisk ]
                From    = From "Employees"
                Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
                Options = Some [| Index("_M-y.1nd3x", true) |] }
                -> true
            | _ -> false
            |> should equal true

        [<Test>]
        member this.``when Index option is specified with AllAttributes set to false it should be captured in the Options clause`` () =
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
        member this.``when NoReturnedCapacity option is specified it should be captured in the Options clause`` () =
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
        member this.``when both NoConsistentRead and PageSize options are specified they should be captured in the Options clause`` () =
            let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH ( NOconsistentRead, Pagesize(  10) )"
        
            match parseDynamoQueryV2 select with
            | { Action  = Select [ Asterisk ]
                From    = From "Employees"
                Where   = Where [ (Attribute "FirstName", Equal (S "Yan")) ]
                Options = Some [| NoConsistentRead; QueryPageSize 10 |] }
                -> true
            | _ -> false
            |> should equal true

