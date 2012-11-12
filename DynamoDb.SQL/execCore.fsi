// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open DynamoDb.SQL.Ast
open Amazon.DynamoDB.Model

exception EmptySelect
exception EmptyFrom
exception InvalidQuery      of string
exception InvalidScan       of string
exception NotAllowedInQuery of FilterCondition

[<AutoOpen>]
module Core =
    /// Active pattern to match the query conditions out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html
    val (|QueryCondition|_|) : Filter list -> (Operant * FilterCondition option) option

    /// Active pattern to determine whether a query represents a Query/Scan request
    val (|Query|Scan|) : Filter list -> Choice<(Operant * FilterCondition option), (string * FilterCondition) list>

    /// Active pattern to get the names of the attributes from a list of identifiers in a select
    val (|SelectAttributes|)    : Identifier list -> List<string>