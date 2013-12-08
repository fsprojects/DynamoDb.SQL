// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

open System.Collections.Generic
open Amazon.DynamoDBv2.Model

exception InvalidQueryFormat    of string

module Core =
    let (|QueryCondition|) (conditions : Filter list) = 
        // only attribute names are allowed by the parser, so safe to assume Attribute clause here
        let filters = conditions |> List.map (fun (Attribute name, cond) -> name, cond)

        match filters with
        | [] -> raise <| InvalidQueryFormat "Query should specify a '=' clause against the hash key attribute"
        | _ when not <| (filters |> List.exists (function | _, Equal _ -> true | _ -> false))
             -> raise <| InvalidQueryFormat "Query should specify a '=' clause against the hash key attribute"
        | _  -> filters

    let (|ScanCondition|) (conditions : Filter list) =
        // only attribute names are allowed by the parser, so safe to assume Attribute clause here
        conditions |> List.map (fun (Attribute name, cond) -> name, cond)

    /// Active pattern to find out whether a given identifier exists in a list of identifiers
    let (|ContainsIdentifier|) expected identifiers = identifiers |> List.exists ((=) expected)
    
    /// Active pattern to get the names of the attributes from a list of identifiers in a Select
    let (|SelectAttributes|) (identifiers : Identifier list) =
        match identifiers with
        | ContainsIdentifier Asterisk true 
            -> Unchecked.defaultof<List<string>>
        | _ -> // only asterisk and attribute names are allowed in select so ok to assume attribute names here
               let attrValues = identifiers |> Seq.map (fun (Attribute name) -> name)
               new List<string>(attrValues)

    /// Active pattern to return the values for the Count and AttributesToGet request parameters
    let (|ActionParams|) action =
        match action with
        | Count -> true, Unchecked.defaultof<List<string>>
        | Select(SelectAttributes attributes) -> false, attributes

    /// Returns whether to use consistent read based on specified query options, default is to use consistent read
    let isConsistentRead (opts : QueryOption[] option) =
        match opts with
        | Some arr ->
            arr 
            |> Array.exists (function | NoConsistentRead -> true | _ -> false)
            |> not
        | _ -> true

    /// Returns whether consumed capacity count is not returned
    let returnQueryConsumedCapacity (opts : QueryOption[] option) =
        match opts with
        | Some arr -> 
            arr 
            |> Array.exists (function | QueryNoReturnedCapacity -> true | _ -> false)
            |> not
        | _ -> true

    /// Try to get the page size option from the specified query options
    let tryGetQueryPageSize (opts : QueryOption[] option) =
        match opts with
        | Some arr -> arr |> Array.tryPick (function | QueryPageSize n -> Some n | _ -> None)
        | _ -> None

    /// Try to get the local secondary index name and whether to use all attributes
    let tryGetQueryIndex (opts : QueryOption[] option) =
        match opts with
        | Some arr -> arr |> Array.tryPick (function | Index(name, allAttrs) -> Some(name, allAttrs) | _ -> None)
        | _ -> None
    
    /// Returns whether consumed capacity count is not returned
    let returnScanConsumedCapacity (opts : ScanOption[] option) =
        match opts with
        | Some arr -> 
            arr 
            |> Array.exists (function | ScanNoReturnedCapacity -> true | _ -> false)
            |> not
        | _ -> true

    /// Try to get the page size option from the specified scan options
    let tryGetScanPageSize (opts : ScanOption[] option) =
        match opts with
        | Some arr -> arr |> Array.tryPick (function | ScanPageSize n -> Some n | _ -> None)
        | _ -> None

    /// Get the scan segments option from the specified scan options, default is 1
    let getScanSegments (opts : ScanOption[] option) =
        match opts with
        | Some arr -> arr |> Array.map (function | ScanSegments n -> n | _ -> 1) |> Array.max
        | _ -> 1