// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open System.Runtime.CompilerServices
open DynamoDb.SQL.Ast
open Amazon.DynamoDB.DataModel
open Amazon.DynamoDB.DocumentModel

[<AutoOpen>]
module Helper =
    /// Active pattern for getting the query or scan operation config
    val (|IsQueryConfig|IsScanConfig|) : DynamoQuery -> Choice<QueryOperationConfig, ScanOperationConfig>

/// Extension methods for the DynamoDBContext class
[<Extension>]
[<AbstractClass>]
[<Sealed>]
type DynamoDBContextExt =
    /// Executes a query synchronously and returns the results
    [<Extension>]
    static member ExecQuery<'T>    : DynamoDBContext * string -> IEnumerable<'T>

    /// Executes a scan synchronously and returns the results
    [<Extension>]
    static member ExecScan<'T>     : DynamoDBContext * string -> IEnumerable<'T>