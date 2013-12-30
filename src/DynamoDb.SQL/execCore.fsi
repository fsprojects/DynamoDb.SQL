// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

open System.Collections.Generic

type InvalidQueryFormatException =
    inherit System.Exception

    new : string -> InvalidQueryFormatException

[<AutoOpen>]
module internal Core =
    /// Active pattern to match the query conditions out of a list of filters
    /// See http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html
    val (|QueryCondition|)      : Filter list -> (string * FilterCondition) list

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
    val returnQueryConsumedCapacity : QueryOption[] option -> bool

    /// Try to get the page size option from the specified query options
    val tryGetQueryPageSize     : QueryOption[] option -> int option

    /// Try to get the local secondary index name and whether to use all attributes
    val tryGetQueryIndex        : QueryOption[] option -> (string * bool) option
        
    /// Returns whether consumed capacity count is not returned
    val returnScanConsumedCapacity  : ScanOption[] option -> bool

    /// Try to get the page size option from the specified scan options
    val tryGetScanPageSize      : ScanOption[] option -> int option

    /// Get the page size option from the specified scan options, default is 1
    val getScanSegments         : ScanOption[] option -> int