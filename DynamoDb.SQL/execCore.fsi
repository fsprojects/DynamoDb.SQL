// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

open System.Collections.Generic

exception InvalidQueryFormat    of string

[<AutoOpen>]
module internal Core =
    /// Active pattern to match the query v1 conditions out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html
    val (|QueryV1Condition|)    : Filter list -> (Operant * FilterCondition option)

    /// Active pattern to match the query v2 conditions out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html
    val (|QueryV2Condition|)    : Filter list -> (string * FilterCondition) list

    /// Active pattern to match the scan conditions out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Scan.html
    val (|ScanCondition|)       : Filter list -> (string * FilterCondition) list
    
    /// Active pattern to get the names of the attributes from a list of identifiers in a select
    val (|SelectAttributes|)    : Identifier list -> List<string>

    /// Active pattern to return the values for the Count and AttributesToGet request parameters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Count
    val (|ActionParams|)        : Action -> bool * List<string>

    /// Returns whether to use consistent read based on specified query options, default is to use consistent read
    val isConsistentRead        : QueryOption[] option -> bool

    /// Returns whether consumed capacity count is not returned
    val returnConsumedCapacity  : QueryOption[] option -> bool

    /// Try to get the page size option from the specified query options
    val tryGetQueryPageSize     : QueryOption[] option -> int option

    /// Try to get the local secondary index name and whether to use all attributes
    val tryGetQueryIndex        : QueryOption[] option -> (string * bool) option

    /// Try to get the page size option from the specified scan options
    val tryGetScanPageSize      : ScanOption[] option -> int option