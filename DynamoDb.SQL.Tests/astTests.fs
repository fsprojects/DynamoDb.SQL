// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Tests

open System
open FsUnit
open NUnit.Framework
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open DynamoDb.SQL.Execution

let equal = FsUnit.equal

type EntryTypeV1 = Amazon.DynamoDB.DocumentModel.DynamoDBEntryType
type EntryTypeV2 = Amazon.DynamoDBv2.DocumentModel.DynamoDBEntryType

[<TestFixture>]
type ``Given an Operant`` () =
    [<Test>]
    member this.``S.ToAttributeValue should return an AttributeValue with S set to its string value`` () =
        let op = S "Test"

        op.ToAttributeValueV1().S   |> should equal "Test"
        op.ToAttributeValueV2().S   |> should equal "Test"

    [<Test>]
    member this.``N.ToAttributeValue should return an AttributeValue with N set to string representation of its numeric value value`` () =
        let op = N 30.0

        op.ToAttributeValueV1().N   |> should equal "30"
        op.ToAttributeValueV2().N   |> should equal "30"

    [<Test>]
    member this.``S.ToPrimitive should return a Primitive with type String of its string value`` () =
        let op = S "Test"

        op.ToPrimitiveV1().Type     |> should equal EntryTypeV1.String
        op.ToPrimitiveV1().Value    |> should equal "Test"

        op.ToPrimitiveV2().Type     |> should equal EntryTypeV2.String
        op.ToPrimitiveV2().Value    |> should equal "Test"

    [<Test>]
    member this.``N.ToPrimitive should return a Primitive with type Numeric of string representation of its numeric value`` () =
        let op = N 30.0

        op.ToPrimitiveV1().Type     |> should equal EntryTypeV1.Numeric
        op.ToPrimitiveV1().Value    |> should equal "30"

        op.ToPrimitiveV2().Type     |> should equal EntryTypeV2.Numeric
        op.ToPrimitiveV2().Value    |> should equal "30"

[<TestFixture>]
type ``Given a FilterCondition`` () =
    [<Test>]
    member this.``NotEqual, NotNull, Null, Contains, NotContains and In are not allowed in queries`` () =
        NotEqual(S "Test").IsAllowedInQuery         |> should equal false
        NotNull.IsAllowedInQuery                    |> should equal false
        Null.IsAllowedInQuery                       |> should equal false
        Contains(S "Test").IsAllowedInQuery         |> should equal false
        NotContains(S "Test").IsAllowedInQuery      |> should equal false
        In([ S "Test" ]).IsAllowedInQuery           |> should equal false

    [<Test>]
    member this.``Equal.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Equal(S "Test").ToConditionV1().ComparisonOperator          |> should equal "EQ"
        Equal(S "Test").ToConditionV1().AttributeValueList.Count    |> should equal 1
        Equal(S "Test").ToConditionV1().AttributeValueList.[0].S    |> should equal "Test"

        Equal(S "Test").ToConditionV2().ComparisonOperator          |> should equal "EQ"
        Equal(S "Test").ToConditionV2().AttributeValueList.Count    |> should equal 1
        Equal(S "Test").ToConditionV2().AttributeValueList.[0].S    |> should equal "Test"

    [<Test>]
    member this.``NotEqual.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        NotEqual(S "Test").ToConditionV1().ComparisonOperator       |> should equal "NE"
        NotEqual(S "Test").ToConditionV1().AttributeValueList.Count |> should equal 1
        NotEqual(S "Test").ToConditionV1().AttributeValueList.[0].S |> should equal "Test"
        
        NotEqual(S "Test").ToConditionV2().ComparisonOperator       |> should equal "NE"
        NotEqual(S "Test").ToConditionV2().AttributeValueList.Count |> should equal 1
        NotEqual(S "Test").ToConditionV2().AttributeValueList.[0].S |> should equal "Test"

    [<Test>]
    member this.``GreaterThan.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        GreaterThan(N 30.0).ToConditionV1().ComparisonOperator       |> should equal "GT"
        GreaterThan(N 30.0).ToConditionV1().AttributeValueList.Count |> should equal 1
        GreaterThan(N 30.0).ToConditionV1().AttributeValueList.[0].N |> should equal "30"
                    
        GreaterThan(N 30.0).ToConditionV2().ComparisonOperator       |> should equal "GT"
        GreaterThan(N 30.0).ToConditionV2().AttributeValueList.Count |> should equal 1
        GreaterThan(N 30.0).ToConditionV2().AttributeValueList.[0].N |> should equal "30"

    [<Test>]
    member this.``GreaterThanOrEqual.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        GreaterThanOrEqual(N 30.0).ToConditionV1().ComparisonOperator       |> should equal "GE"
        GreaterThanOrEqual(N 30.0).ToConditionV1().AttributeValueList.Count |> should equal 1
        GreaterThanOrEqual(N 30.0).ToConditionV1().AttributeValueList.[0].N |> should equal "30"
         
        GreaterThanOrEqual(N 30.0).ToConditionV2().ComparisonOperator       |> should equal "GE"
        GreaterThanOrEqual(N 30.0).ToConditionV2().AttributeValueList.Count |> should equal 1
        GreaterThanOrEqual(N 30.0).ToConditionV2().AttributeValueList.[0].N |> should equal "30"

    [<Test>]
    member this.``LessThan.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        LessThan(N 30.0).ToConditionV1().ComparisonOperator         |> should equal "LT"
        LessThan(N 30.0).ToConditionV1().AttributeValueList.Count   |> should equal 1
        LessThan(N 30.0).ToConditionV1().AttributeValueList.[0].N   |> should equal "30"
         
        LessThan(N 30.0).ToConditionV2().ComparisonOperator         |> should equal "LT"
        LessThan(N 30.0).ToConditionV2().AttributeValueList.Count   |> should equal 1
        LessThan(N 30.0).ToConditionV2().AttributeValueList.[0].N   |> should equal "30"

    [<Test>]
    member this.``LessThanOrEqual.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        LessThanOrEqual(N 30.0).ToConditionV1().ComparisonOperator          |> should equal "LE"
        LessThanOrEqual(N 30.0).ToConditionV1().AttributeValueList.Count    |> should equal 1
        LessThanOrEqual(N 30.0).ToConditionV1().AttributeValueList.[0].N    |> should equal "30"
      
        LessThanOrEqual(N 30.0).ToConditionV2().ComparisonOperator          |> should equal "LE"
        LessThanOrEqual(N 30.0).ToConditionV2().AttributeValueList.Count    |> should equal 1
        LessThanOrEqual(N 30.0).ToConditionV2().AttributeValueList.[0].N    |> should equal "30"

    [<Test>]
    member this.``NotNull.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        NotNull.ToConditionV1().ComparisonOperator          |> should equal "NOT_NULL"
        NotNull.ToConditionV1().AttributeValueList.Count    |> should equal 0
        
        NotNull.ToConditionV2().ComparisonOperator          |> should equal "NOT_NULL"
        NotNull.ToConditionV2().AttributeValueList.Count    |> should equal 0

    [<Test>]
    member this.``Null.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Null.ToConditionV1().ComparisonOperator             |> should equal "NULL"
        Null.ToConditionV1().AttributeValueList.Count       |> should equal 0
    
        Null.ToConditionV2().ComparisonOperator             |> should equal "NULL"
        Null.ToConditionV2().AttributeValueList.Count       |> should equal 0

    [<Test>]
    member this.``Contains.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Contains(S "Test").ToConditionV1().ComparisonOperator           |> should equal "CONTAINS"
        Contains(S "Test").ToConditionV1().AttributeValueList.Count     |> should equal 1
        Contains(S "Test").ToConditionV1().AttributeValueList.[0].S     |> should equal "Test"
                
        Contains(S "Test").ToConditionV2().ComparisonOperator           |> should equal "CONTAINS"
        Contains(S "Test").ToConditionV2().AttributeValueList.Count     |> should equal 1
        Contains(S "Test").ToConditionV2().AttributeValueList.[0].S     |> should equal "Test"

    [<Test>]
    member this.``NotContains.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        NotContains(S "Test").ToConditionV1().ComparisonOperator        |> should equal "NOT_CONTAINS"
        NotContains(S "Test").ToConditionV1().AttributeValueList.Count  |> should equal 1
        NotContains(S "Test").ToConditionV1().AttributeValueList.[0].S  |> should equal "Test"
    
        NotContains(S "Test").ToConditionV2().ComparisonOperator        |> should equal "NOT_CONTAINS"
        NotContains(S "Test").ToConditionV2().AttributeValueList.Count  |> should equal 1
        NotContains(S "Test").ToConditionV2().AttributeValueList.[0].S  |> should equal "Test"

    [<Test>]
    member this.``BeginsWith.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        BeginsWith(S "Test").ToConditionV1().ComparisonOperator         |> should equal "BEGINS_WITH"
        BeginsWith(S "Test").ToConditionV1().AttributeValueList.Count   |> should equal 1
        BeginsWith(S "Test").ToConditionV1().AttributeValueList.[0].S   |> should equal "Test"
        
        BeginsWith(S "Test").ToConditionV2().ComparisonOperator         |> should equal "BEGINS_WITH"
        BeginsWith(S "Test").ToConditionV2().AttributeValueList.Count   |> should equal 1
        BeginsWith(S "Test").ToConditionV2().AttributeValueList.[0].S   |> should equal "Test"

    [<Test>]
    member this.``Between.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Between(N 30.0, N 40.0).ToConditionV1().ComparisonOperator         |> should equal "BETWEEN"
        Between(N 30.0, N 40.0).ToConditionV1().AttributeValueList.Count   |> should equal 2
        Between(N 30.0, N 40.0).ToConditionV1().AttributeValueList.[0].N   |> should equal "30"
        Between(N 30.0, N 40.0).ToConditionV1().AttributeValueList.[1].N   |> should equal "40"

        Between(N 30.0, N 40.0).ToConditionV2().ComparisonOperator         |> should equal "BETWEEN"
        Between(N 30.0, N 40.0).ToConditionV2().AttributeValueList.Count   |> should equal 2
        Between(N 30.0, N 40.0).ToConditionV2().AttributeValueList.[0].N   |> should equal "30"
        Between(N 30.0, N 40.0).ToConditionV2().AttributeValueList.[1].N   |> should equal "40"

    [<Test>]
    member this.``In.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        In([ N 30.0; N 40.0 ]).ToConditionV1().ComparisonOperator         |> should equal "IN"
        In([ N 30.0; N 40.0 ]).ToConditionV1().AttributeValueList.Count   |> should equal 2
        In([ N 30.0; N 40.0 ]).ToConditionV1().AttributeValueList.[0].N   |> should equal "30"
        In([ N 30.0; N 40.0 ]).ToConditionV1().AttributeValueList.[1].N   |> should equal "40"
                            
        In([ N 30.0; N 40.0 ]).ToConditionV2().ComparisonOperator         |> should equal "IN"
        In([ N 30.0; N 40.0 ]).ToConditionV2().AttributeValueList.Count   |> should equal 2
        In([ N 30.0; N 40.0 ]).ToConditionV2().AttributeValueList.[0].N   |> should equal "30"
        In([ N 30.0; N 40.0 ]).ToConditionV2().AttributeValueList.[1].N   |> should equal "40"