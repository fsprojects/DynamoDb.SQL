// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Parser

open FParsec
open DynamoDb.SQL.Ast

/// Exception that's raised when the query being parsed is invalid
exception InvalidQuery  of string

/// Parser for a DynamoQuery
val pquery : Parser<DynamoQuery, unit>

/// Function to parse a query string and return the corresponding DynamoQuery
val parseDynamoQuery : string -> DynamoQuery