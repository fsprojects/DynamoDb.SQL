// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

/// Exception that's raised when the query being parsed is invalid
exception InvalidTableName  of string
exception InvalidQuery      of string
exception InvalidScan       of string

[<AutoOpen>]
module internal Parser =
    /// Function to parse a V1 query string and return the corresponding DynamoQuery
    val parseDynamoQueryV1  : string -> DynamoQuery

    /// Function to parse a V2 query string and return the corresponding DynamoQuery
    val parseDynamoQueryV2  : string -> DynamoQuery

    /// Function to parse a string and return the corresponding DynamoScan
    val parseDynamoScanV1   : string -> DynamoScan

    /// Function to parse a string and return the corresponding DynamoScan
    val parseDynamoScanV2   : string -> DynamoScan