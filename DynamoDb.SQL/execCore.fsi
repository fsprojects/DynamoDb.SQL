// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open DynamoDb.SQL.Ast
open Amazon.DynamoDB.Model

[<AutoOpen>]
module Core =
    /// Active pattern to match the key out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_GetItem.html
    val (|Key|_|) : Filter list -> Key option

    /// Active pattern to match the query conditions out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html
    val (|RangeKeyCondition|_|) : Filter list -> (AttributeValue * FilterCondition) option

    /// Active pattern to determine whether a query represents a Get/Query/Scan request
    val (|GetByKey|Query|Scan|) : Filter list -> Choice<Key, (AttributeValue * FilterCondition), unit>

    /// Active pattern to get the names of the attributes from a list of identifiers in a select
    val (|SelectAttributes|)    : Identifier list -> List<string>