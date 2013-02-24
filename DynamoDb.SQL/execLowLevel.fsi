// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Runtime.CompilerServices
open System.Collections.Generic
open System.Threading.Tasks
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open DynamoDb.SQL.Ast

[<AutoOpen>]
module LowLevel =
    /// Active pattern for getting the query request object out of a DynamoQuery
    val (|GetQueryReq|) : bool -> DynamoQuery -> QueryRequest

    /// Active pattern for getting the scan request object out of a DynamoScan
    val (|GetScanReq|) : DynamoScan -> ScanRequest

/// Extension methods for the low level DynamoDB client to be used in F#
[<AutoOpen>]
module ClientExt =
    type AmazonDynamoDBClient with
        /// Executes a query asynchronously and returns the results
        member QueryAsync   : string * bool option -> Async<QueryResponse>

        /// Executes a query synchronously and returns the results
        member Query        : string * bool option -> QueryResponse

        /// Executes a scan asynchronously and returns the results
        member ScanAsync    : string -> Async<ScanResponse>

        /// Executes a scan synchronously and returns the results
        member Scan         : string -> ScanResponse

/// Extension methods for the low level DynamoDB client to be used in C#
[<Extension>]
[<AbstractClass>]
[<Sealed>]
type AmazonDynamoDBClientExt =
    /// Executes a query asynchronously as a task and returns the results
    /// This method uses consistent read by default
    [<Extension>]
    static member QueryAsyncAsTask : AmazonDynamoDBClient * string -> Task<QueryResponse>
    
    /// Executes a query asynchronously as a task and returns the results
    /// This method lets you decide whether to use consistent read
    [<Extension>]
    static member QueryAsyncAsTask : AmazonDynamoDBClient * string * bool -> Task<QueryResponse>

    /// Executes a query synchronously and returns the results
    /// This method uses consistent read by default
    [<Extension>]
    static member Query            : AmazonDynamoDBClient * string -> QueryResponse

    /// Executes a query synchronously and returns the results
    /// This method lets you decide whether to use consistent read
    [<Extension>]
    static member Query            : AmazonDynamoDBClient * string * bool -> QueryResponse

    /// Executes a scan asynchronously as a task and returns the results
    [<Extension>]
    static member ScanAsyncAsTask  : AmazonDynamoDBClient * string -> Task<ScanResponse>

    /// Executes a scan synchronously and returns the results
    [<Extension>]
    static member Scan             : AmazonDynamoDBClient * string -> ScanResponse