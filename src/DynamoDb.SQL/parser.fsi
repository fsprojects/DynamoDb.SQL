// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

open System

type InvalidTableNameException =
    inherit Exception

    new : string -> InvalidTableNameException

type InvalidQueryException = 
    inherit Exception

    new : string -> InvalidQueryException

type InvalidScanException = 
    inherit Exception

    new : string -> InvalidScanException

[<AutoOpen>]
module internal Parser =
    /// Function to parse a query string and return the corresponding DynamoQuery
    val parseDynamoQuery  : string -> DynamoQuery

    /// Function to parse a string and return the corresponding DynamoScan
    val parseDynamoScan   : string -> DynamoScan