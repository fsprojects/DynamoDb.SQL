// Author : Yan Cui (twitter @theburningmonk)

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.parser.Tests

open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser

[<TestFixture>]
type ``Given a query`` () =
    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when there is no attributes in the select clause it should except`` () =
        let select = "SELECT FROM Employees WHERE FirstName = \"Yan\""
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidTableNameException>)>]
    member this.``when there is no table name in the from clause it should except`` () =
        let select = "SELECT * FROM WHERE FirstName = \"Yan\""
        parseDynamoQuery select |> should throw typeof<InvalidTableNameException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when there is no where clause it should except`` () =
        let select = "SELECT * FROM Employees"
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when there is no filter conditions in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE"
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when there is a HashKey keyword in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE @HashKey = \"Yan\""
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when there is a RangeKey keyword in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE @RangeKey = \"Yan\""
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    member this.``when there is white spaces around the attribute names and table name they should be ignored`` () =
        let select = "SELECT Name,    Age,
                                Salary
                        FROM   Employees 
                        WHERE FirstName   =      \"Yan\"
                        LIMIT     5"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Limit  |> should equal <| Some(Limit 5)

    [<Test>]
    member this.``when the SELECT, FROM, WHERE and LIMIT keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                        FrOm Employees
                        where FirstName = \"Yan\"
                        liMIt 5"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Limit  |> should equal <| Some(Limit 5)

    [<Test>]
    member this.``when there are multiple conditions in the where clause they should all be parsed`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName = \"Cui\""

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal(S "Yan")); (Attribute "LastName", Equal(S "Cui")) ]
        query.Limit  |> should equal <| None

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when the != operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age != 30"
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    member this.``when < operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age < 99"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", LessThan (NBigInt 99I)) ]
        query.Limit  |> should equal <| None

    [<Test>]
    member this.``when <= operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age <= 99"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", LessThanOrEqual (NBigInt 99I)) ]
        query.Limit  |> should equal <| None

    [<Test>]
    member this.``when > operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age > 99"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThan (NBigInt 99I)) ]
        query.Limit  |> should equal <| None

    [<Test>]
    member this.``when >= operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 99"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual (NBigInt 99I)) ]
        query.Limit  |> should equal <| None

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when the Contains operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName CONTAINS \"Cui\""
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when the NotContains operator is used it except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName NOT CONTAINS \"Cui\""
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    member this.``when the Begins With operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName BEGINS WITH \"Cui\""

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "LastName", BeginsWith (S "Cui")) ]
        query.Limit  |> should equal <| None
    
    [<Test>]
    member this.``when the Between operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age BETWEEN 10 AND 30"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", Between ((NBigInt 10I), (NBigInt 30I))) ]
        query.Limit  |> should equal <| None

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when the In operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age IN (\"Foo\", \"Bar\")"
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when the Is Null operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName IS NULL"
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when the Is Not Null operator is used it should except`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND LastName IS NOT NULL"
        parseDynamoQuery select |> should throw typeof<InvalidQueryException>

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 LIMIT 10"

        let query = parseDynamoQuery select
            
        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(NBigInt 30I)) ]
        query.Limit  |> should equal <| Some(Limit 10)

    [<Test>]
    member this.``when order asc is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER ASC LIMIT 10"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(NBigInt 30I)) ]
        query.Limit  |> should equal <| Some(Limit 10)
        query.Order  |> should equal <| Some(Asc)

    [<Test>]
    member this.``when order desc is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"

        let query = parseDynamoQuery select

        query.Action |> should equal <| Select [ Asterisk ]
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(NBigInt 30I)) ]
        query.Limit  |> should equal <| Some(Limit 10)
        query.Order  |> should equal <| Some(Desc)

    [<Test>]
    member this.``when a count query is specified, it should be parsed correctly`` () =
        let count = "COUNT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"

        let query = parseDynamoQuery count

        query.Action |> should equal <| Count
        query.From   |> should equal <| From "Employees"
        query.Where  |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(NBigInt 30I)) ]
        query.Limit  |> should equal <| Some(Limit 10)
        query.Order  |> should equal <| Some(Desc)

    [<Test>]
    [<ExpectedException(typeof<InvalidQueryException>)>]
    member this.``when a count query is specified with attribute names, it should except`` () =
        let count = "COUNT FirstName FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"
        parseDynamoQuery count |> should throw typeof<InvalidQueryException>

    [<Test>]
    member this.``when NoConsistentRead option is specified it should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (  nOConsiStentRead )"
        
        let query = parseDynamoQuery select

        query.Action  |> should equal <| Select [ Asterisk ]
        query.From    |> should equal <| From "Employees"
        query.Where   |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Options |> should equal <| Some [| NoConsistentRead |]

    [<Test>]
    member this.``when PageSize option is specified it should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Pagesize(  10) )"
        
        let query = parseDynamoQuery select

        query.Action  |> should equal <| Select [ Asterisk ]
        query.From    |> should equal <| From "Employees"
        query.Where   |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Options |> should equal <| Some [| QueryPageSize 10 |]

    [<Test>]
    member this.``when Index option is specified with AllAttributes set to true it should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Index( _M-y.1nd3x ,  true) )"
        
        let query = parseDynamoQuery select

        query.Action  |> should equal <| Select [ Asterisk ]
        query.From    |> should equal <| From "Employees"
        query.Where   |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Options |> should equal <| Some [| Index("_M-y.1nd3x", true) |]

    [<Test>]
    member this.``when Index option is specified with AllAttributes set to false it should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH (Index( _M-y.1nd3x ,  false) )"
        
        let query = parseDynamoQuery select

        query.Action  |> should equal <| Select [ Asterisk ]
        query.From    |> should equal <| From "Employees"
        query.Where   |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Options |> should equal <| Some [| Index("_M-y.1nd3x", false) |]

    [<Test>]
    member this.``when NoReturnedCapacity option is specified it should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH ( NoReturnedCapacity)"
        
        let query = parseDynamoQuery select

        query.Action  |> should equal <| Select [ Asterisk ]
        query.From    |> should equal <| From "Employees"
        query.Where   |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Options |> should equal <| Some [| QueryNoReturnedCapacity |]

    [<Test>]
    member this.``when both NoConsistentRead and PageSize options are specified they should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" WITH ( NOconsistentRead, Pagesize(  10) )"
        
        let query = parseDynamoQuery select

        query.Action  |> should equal <| Select [ Asterisk ]
        query.From    |> should equal <| From "Employees"
        query.Where   |> should equal <| Where [ (Attribute "FirstName", Equal (S "Yan")) ]
        query.Options |> should equal <| Some [| NoConsistentRead; QueryPageSize 10 |]
            
    [<Test>]
    member this.``when the attribute value is a very long number it should be parsed without losing precision`` () =
        // from issue #29
        let select = "SELECT * FROM pd_cs_friendshiprequest WHERE ForUserId = 10205471065416239"
        
        let query = parseDynamoQuery select
        query.Where |> should equal <| Where [ (Attribute "ForUserId", Equal (NBigInt 10205471065416239I))]

        // where the attribute value is > int64 but < uint64
        let select = "SELECT * FROM pd_cs_friendshiprequest WHERE ForUserId = 9223372036854775809"

        let query = parseDynamoQuery select
        query.Where |> should equal <| Where [ (Attribute "ForUserId", Equal (NBigInt 9223372036854775809I))]

        // where the attribute value is negative
        let select = "SELECT * FROM pd_cs_friendshiprequest WHERE ForUserId = -9223372036854775808"

        let query = parseDynamoQuery select
        query.Where |> should equal <| Where [ (Attribute "ForUserId", Equal (NBigInt -9223372036854775808I))]

    [<Test>]
    member this.``when the attribute value is a small number it should still be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Id = 3"

        let query = parseDynamoQuery select
        query.Where |> should equal <| Where [ (Attribute "Id", Equal (NBigInt 3I))]

        let select = "SELECT * FROM Employees WHERE Id = -3"

        let query = parseDynamoQuery select
        query.Where |> should equal <| Where [ (Attribute "Id", Equal (NBigInt -3I))]

    [<Test>]
    member this.``when the attribute value is a decimal it should be parsed as NDouble`` () =
        let select = "SELECT * FROM Employees WHERE Id = 42.32"

        let query = parseDynamoQuery select
        query.Where |> should equal <| Where [ (Attribute "Id", Equal (NDouble 42.32))]

[<TestFixture>]
type ``Given a scan`` () =
    [<Test>]
    [<ExpectedException(typeof<InvalidScanException>)>]
    member this.``when there is no attributes in the select clause it should except`` () =
        let select = "SELECT FROM Employees WHERE @hashkey = \"Yan\""
        parseDynamoScan select |> should throw typeof<InvalidScanException>

    [<Test>]
    [<ExpectedException(typeof<InvalidTableNameException>)>]
    member this.``when there is no table name in the from clause it should except`` () =
        let select = "SELECT * FROM WHERE FirstName = \"Yan\""
        parseDynamoScan select |> should throw typeof<InvalidTableNameException>

    [<Test>]   
    member this.``when there is no where clause it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| None
        scan.Limit  |> should equal <| None

    [<Test>]
    [<ExpectedException(typeof<InvalidScanException>)>]
    member this.``when there is no filter conditions in the where clause it should except`` () =
        let select = "SELECT * FROM Employees WHERE"
        parseDynamoScan select |> should throw typeof<InvalidScanException>

    [<Test>]
    member this.``when there is white spaces around the attribute names and table name they should be ignored`` () =
        let select = "SELECT Name,    Age,
                                Salary
                        FROM   Employees 
                        WHERE FirstName   =      \"Yan\"
                        LIMIT     5"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", Equal (S "Yan")) ])
        scan.Limit  |> should equal <| Some(Limit 5)

    [<Test>]
    member this.``when table name has valid non-alphanumeric characters it should parse`` () =
        let select = "SELECT * FROM t-A_b.le1"
        let scan = parseDynamoScan select
        scan.From       |> should equal <| From "t-A_b.le1"

    [<Test>]
    member this.``when the SELECT, FROM, WHERE and LIMIT keywords are not in capitals they should still be parsed correctly`` () =
        let select = "sELeCT Name, Age, Salary
                        FrOm Employees
                        where FirstName = \"Yan\"
                        liMIt 5"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Attribute "Name"; Attribute "Age"; Attribute "Salary" ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", Equal (S "Yan")) ])
        scan.Limit  |> should equal <| Some(Limit 5)

    [<Test>]
    [<ExpectedException(typeof<InvalidScanException>)>]
    member this.``when a hash key is included it should except`` () =
        let select = "SELECT * FROM Employees WHERE @hashkey = \"Yan\""
        parseDynamoScan select |> should throw typeof<InvalidScanException>

    [<Test>]
    [<ExpectedException(typeof<InvalidScanException>)>]
    member this.``when a range key is included it should except`` () =
        let select = "SELECT * FROM Employees WHERE @rangekey = \"Cui\""
        parseDynamoScan select |> should throw typeof<InvalidScanException>

    [<Test>]
    member this.``when an attribute name is included in a filter condition it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age = 30"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "Age", Equal (NBigInt 30I)) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the != operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName != \"Yan\""
        
        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", NotEqual (S "Yan")) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when <, <=, >, >= operators are used they should be parsed correctly`` () =
        let select = "SELECT * FROM Employees 
                        WHERE Age >= 10 
                        AND   Age > 20
                        AND   Age <= 99
                        AND   Age < 90"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "Age", GreaterThanOrEqual(NBigInt 10I))
                                                    (Attribute "Age", GreaterThan(NBigInt 20I))
                                                    (Attribute "Age", LessThanOrEqual(NBigInt 99I))
                                                    (Attribute "Age", LessThan(NBigInt 90I)) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the Contains operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName CONTAINS \"Yan\""
        
        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", Contains(S "Yan")) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the NotContains operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName NOT CONTAINS \"Yan\""
        
        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", NotContains(S "Yan")) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the Begins With operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName BEGINS WITH \"Yan\""

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", BeginsWith (S "Yan")) ])
        scan.Limit  |> should equal <| None
    
    [<Test>]
    member this.``when the Between operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age BETWEEN 10 AND 30"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "Age", Between ((NBigInt 10I), (NBigInt 30I))) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the In operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE Age IN (10, 30, 50)"
        
        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "Age", In [ NBigInt 10I; NBigInt 30I; NBigInt 50I ]) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the Is Null operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE LastName IS NULL"
        
        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "LastName", Null) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when the Is Not Null operator is used it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE LastName IS NOT NULL"
        
        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "LastName", NotNull) ])
        scan.Limit  |> should equal <| None

    [<Test>]
    member this.``when limit clause is specified, it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 LIMIT 10"

        let scan = parseDynamoScan select

        scan.Action |> should equal <| Select [ Asterisk ]
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(NBigInt 30I)) ])
        scan.Limit  |> should equal <| Some(Limit 10)

    [<Test>]
    member this.``when a count query is specified, it should be parsed correctly`` () =
        let count = "COUNT * FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 LIMIT 10"

        let scan = parseDynamoScan count

        scan.Action |> should equal <| Count
        scan.From   |> should equal <| From "Employees"
        scan.Where  |> should equal <| Some(Where [ (Attribute "FirstName", Equal (S "Yan")); (Attribute "Age", GreaterThanOrEqual(NBigInt 30I)) ])
        scan.Limit  |> should equal <| Some(Limit 10)

    [<Test>]
    [<ExpectedException(typeof<InvalidScanException>)>]
    member this.``when a count query is specified with attribute names, it should except`` () =
        let count = "COUNT FirstName FROM Employees WHERE FirstName = \"Yan\" AND Age >= 30 ORDER DESC LIMIT 10"
        parseDynamoScan count |> should throw typeof<InvalidScanException>

    [<Test>]
    member this.``when PageSize option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WITH (Pagesize( 10 ) )"
        
        let scan = parseDynamoScan select

        scan.Action  |> should equal <| Select [ Asterisk ]
        scan.From    |> should equal <| From "Employees"
        scan.Where   |> should equal <| None
        scan.Limit   |> should equal <| None
        scan.Options |> should equal <| Some [| ScanPageSize 10 |]

    [<Test>]
    member this.``when NoReturnedCapacity option is specified it should be captured in the Options clause`` () =
        let select = "SELECT * FROM Employees WITH ( NoReturnedCapacity )"
        
        let scan = parseDynamoScan select

        scan.Action  |> should equal <| Select [ Asterisk ]
        scan.From    |> should equal <| From "Employees"
        scan.Where   |> should equal None
        scan.Limit   |> should equal None
        scan.Options |> should equal <| Some [| ScanNoReturnedCapacity |]

    [<Test>]
    member this.``when ScanSegments option is specified it should be parsed correctly`` () =
        let select = "SELECT * FROM Employees WITH (Segments( 15 ) )"
        
        let scan = parseDynamoScan select

        scan.Action  |> should equal <| Select [ Asterisk ]
        scan.From    |> should equal <| From "Employees"
        scan.Where   |> should equal <| None
        scan.Limit   |> should equal <| None
        scan.Options |> should equal <| Some [| ScanSegments 15 |]