// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open DynamoDb.SQL.Ast
open Amazon.DynamoDB.DataModel
open Amazon.DynamoDB.DocumentModel

[<AutoOpen>]
module Helper =
    /// Active pattern for getting the query or scan operation config
    val (|IsQueryConfig|IsScanConfig|) : DynamoQuery -> Choice<QueryOperationConfig, ScanOperationConfig>

    /// Extension methods for the DynamoDBContext class
    type DynamoDBContext with
        /// Executes a query synchronously and returns the results
        member Query<'T>    : string -> IEnumerable<'T>

        /// Executes a scan synchronously and returns the results
        member Scan<'T>     : string -> IEnumerable<'T>