// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open System.Threading.Tasks

exception EmptySelect
exception EmptyFrom

[<AutoOpen>]
module LowLevel =
    type AmazonDynamoDBClient with
        /// Executes a query asynchronously and returns the results
        member ExecQuery        : string -> Async<seq<Dictionary<string, AttributeValue>>>

        /// Executes a query asynchronously as a task and returns the results
        member ExecQueryAsTask  : string -> Task<seq<Dictionary<string, AttributeValue>>>

        /// Executes a query synchronously and returns the results
        member ExecQuerySync    : string -> seq<Dictionary<string, AttributeValue>>