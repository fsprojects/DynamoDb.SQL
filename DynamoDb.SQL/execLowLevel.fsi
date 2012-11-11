// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open System.Threading.Tasks
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open DynamoDb.SQL.Ast

[<AutoOpen>]
module LowLevel =
    /// Active pattern for getting the query or scan request object
    val (|IsQueryReq|IsScanReq|) : DynamoQuery -> Choice<QueryRequest, ScanRequest>

    /// Extension methods for the low level DynamoDB client
    type AmazonDynamoDBClient with
        /// Executes a query asynchronously and returns the results
        member QueryAsync       : string -> Async<QueryResponse>

        /// Executes a query asynchronously as a task and returns the results
        member QueryAsyncAsTask : string -> Task<QueryResponse>

        /// Executes a query synchronously and returns the results
        member Query            : string -> QueryResponse

        /// Executes a scan asynchronously and returns the results
        member ScanAsync        : string -> Async<ScanResponse>

        /// Executes a scan asynchronously as a task and returns the results
        member ScanAsyncAsTask  : string -> Task<ScanResponse>

        /// Executes a scan synchronously and returns the results
        member Scan             : string -> ScanResponse