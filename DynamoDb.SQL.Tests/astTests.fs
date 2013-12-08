// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Tests

open System
open FsUnit
open NUnit.Framework
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DocumentModel
open DynamoDb.SQL
open DynamoDb.SQL.Parser
open DynamoDbV2.SQL.Execution

let equal = FsUnit.equal

[<TestFixture>]
type ``Given an Operant`` () =
    [<Test>]
    member this.``S.ToAttributeValue should return an AttributeValue with S set to its string value`` () =
        let op = S "Test"

        op.ToAttributeValue().S   |> should equal "Test"

    [<Test>]
    member this.``N.ToAttributeValue should return an AttributeValue with N set to string representation of its numeric value value`` () =
        let op = N 30.0

        op.ToAttributeValue().N   |> should equal "30"

    [<Test>]
    member this.``S.ToPrimitive should return a Primitive with type String of its string value`` () =
        let op = S "Test"

        op.ToPrimitive().Type     |> should equal DynamoDBEntryType.String
        op.ToPrimitive().Value    |> should equal "Test"

    [<Test>]
    member this.``N.ToPrimitive should return a Primitive with type Numeric of string representation of its numeric value`` () =
        let op = N 30.0

        op.ToPrimitive().Type     |> should equal DynamoDBEntryType.Numeric
        op.ToPrimitive().Value    |> should equal "30"

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
        Equal(S "Test").ToCondition().ComparisonOperator          |> should equal ComparisonOperator.EQ
        Equal(S "Test").ToCondition().AttributeValueList.Count    |> should equal 1
        Equal(S "Test").ToCondition().AttributeValueList.[0].S    |> should equal "Test"

    [<Test>]
    member this.``NotEqual.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        NotEqual(S "Test").ToCondition().ComparisonOperator       |> should equal ComparisonOperator.NE
        NotEqual(S "Test").ToCondition().AttributeValueList.Count |> should equal 1
        NotEqual(S "Test").ToCondition().AttributeValueList.[0].S |> should equal "Test"

    [<Test>]
    member this.``GreaterThan.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        GreaterThan(N 30.0).ToCondition().ComparisonOperator       |> should equal ComparisonOperator.GT
        GreaterThan(N 30.0).ToCondition().AttributeValueList.Count |> should equal 1
        GreaterThan(N 30.0).ToCondition().AttributeValueList.[0].N |> should equal "30"

    [<Test>]
    member this.``GreaterThanOrEqual.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        GreaterThanOrEqual(N 30.0).ToCondition().ComparisonOperator       |> should equal ComparisonOperator.GE
        GreaterThanOrEqual(N 30.0).ToCondition().AttributeValueList.Count |> should equal 1
        GreaterThanOrEqual(N 30.0).ToCondition().AttributeValueList.[0].N |> should equal "30"

    [<Test>]
    member this.``LessThan.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        LessThan(N 30.0).ToCondition().ComparisonOperator         |> should equal ComparisonOperator.LT
        LessThan(N 30.0).ToCondition().AttributeValueList.Count   |> should equal 1
        LessThan(N 30.0).ToCondition().AttributeValueList.[0].N   |> should equal "30"

    [<Test>]
    member this.``LessThanOrEqual.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        LessThanOrEqual(N 30.0).ToCondition().ComparisonOperator          |> should equal ComparisonOperator.LE
        LessThanOrEqual(N 30.0).ToCondition().AttributeValueList.Count    |> should equal 1
        LessThanOrEqual(N 30.0).ToCondition().AttributeValueList.[0].N    |> should equal "30"

    [<Test>]
    member this.``NotNull.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        NotNull.ToCondition().ComparisonOperator          |> should equal ComparisonOperator.NOT_NULL
        NotNull.ToCondition().AttributeValueList.Count    |> should equal 0

    [<Test>]
    member this.``Null.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Null.ToCondition().ComparisonOperator             |> should equal ComparisonOperator.NULL
        Null.ToCondition().AttributeValueList.Count       |> should equal 0

    [<Test>]
    member this.``Contains.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Contains(S "Test").ToCondition().ComparisonOperator           |> should equal ComparisonOperator.CONTAINS
        Contains(S "Test").ToCondition().AttributeValueList.Count     |> should equal 1
        Contains(S "Test").ToCondition().AttributeValueList.[0].S     |> should equal "Test"

    [<Test>]
    member this.``NotContains.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        NotContains(S "Test").ToCondition().ComparisonOperator        |> should equal ComparisonOperator.NOT_CONTAINS
        NotContains(S "Test").ToCondition().AttributeValueList.Count  |> should equal 1
        NotContains(S "Test").ToCondition().AttributeValueList.[0].S  |> should equal "Test"

    [<Test>]
    member this.``BeginsWith.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        BeginsWith(S "Test").ToCondition().ComparisonOperator         |> should equal ComparisonOperator.BEGINS_WITH
        BeginsWith(S "Test").ToCondition().AttributeValueList.Count   |> should equal 1
        BeginsWith(S "Test").ToCondition().AttributeValueList.[0].S   |> should equal "Test"

    [<Test>]
    member this.``Between.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        Between(N 30.0, N 40.0).ToCondition().ComparisonOperator         |> should equal ComparisonOperator.BETWEEN
        Between(N 30.0, N 40.0).ToCondition().AttributeValueList.Count   |> should equal 2
        Between(N 30.0, N 40.0).ToCondition().AttributeValueList.[0].N   |> should equal "30"
        Between(N 30.0, N 40.0).ToCondition().AttributeValueList.[1].N   |> should equal "40"

    [<Test>]
    member this.``In.ToCondition should return valid ComparisonOperator and AttributeValueList`` () =
        In([ N 30.0; N 40.0 ]).ToCondition().ComparisonOperator         |> should equal ComparisonOperator.IN
        In([ N 30.0; N 40.0 ]).ToCondition().AttributeValueList.Count   |> should equal 2
        In([ N 30.0; N 40.0 ]).ToCondition().AttributeValueList.[0].N   |> should equal "30"
        In([ N 30.0; N 40.0 ]).ToCondition().AttributeValueList.[1].N   |> should equal "40"