// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Parser

open FParsec
open DynamoDb.SQL.Ast

/// Exception that's raised when the query being parsed is invalid
exception InvalidTableName  of string
exception InvalidQuery      of string
exception InvalidScan       of string

/// Function to parse a string and return the corresponding DynamoQuery
val parseDynamoQuery : string -> DynamoQuery

/// Function to parse a string and return the corresponding DynamoScan
val parseDynamoScan  : string -> DynamoScan