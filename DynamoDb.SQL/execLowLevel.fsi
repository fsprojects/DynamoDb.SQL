// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open Amazon.DynamoDB
open Amazon.DynamoDB.Model

exception EmptySelect
exception EmptyFrom

[<AutoOpen>]
module LowLevel =
    type AmazonDynamoDBClient with
        member ExecQuery : string -> seq<Dictionary<string, AttributeValue>>