// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

open System.Collections.Generic
open Amazon.DynamoDB.Model

exception InvalidQueryFormat    of string

module Core =
    let (|QueryV1Condition|) (conditions : Filter list) = 
        let hKey     = conditions |> List.tryPick (function | (HashKey, Equal(op)) -> Some(op) | _ -> None)
        let rndConds = conditions |> List.choose (function | (RangeKey, cond) when cond.IsAllowedInQuery -> Some(cond) | _ -> None)

        // if a hash key value is specified and there is at most one filter condition then this is a Query
        match hKey, rndConds with
        | Some(hOp), [] -> hOp, None
        | Some(hOp), [ cond ] -> hOp, Some cond
        | _ -> raise <| InvalidQueryFormat "Query should specify a '@haskey =' clause and at most one @rangekey filter"

    let (|QueryV2Condition|) (conditions : Filter list) = 
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
            |> Array.exists (fun opt -> match opt with | NoConsistentRead -> true | _ -> false)
            |> not
        | _ -> true

    /// Returns whether consumed capacity count is not returned
    let returnConsumedCapacity (opts : QueryOption[] option) =
        match opts with
        | Some arr -> 
            arr 
            |> Array.exists (fun opt -> match opt with | NoReturnedCapacity -> true | _ -> false)
            |> not
        | _ -> true

    /// Try to get the page size option from the specified query options
    let tryGetQueryPageSize (opts : QueryOption[] option) =
        match opts with
        | Some arr -> arr |> Array.tryPick (fun opt -> match opt with | QueryPageSize n -> Some n | _ -> None)
        | _ -> None

    /// Try to get the local secondary index name and whether to use all attributes
    let tryGetQueryIndex (opts : QueryOption[] option) =
        match opts with
        | Some arr -> arr |> Array.tryPick (fun opt -> match opt with | Index(name, allAttrs) -> Some(name, allAttrs) | _ -> None)
        | _ -> None

    /// Try to get the page size option from the specified scan options
    let tryGetScanPageSize (opts : ScanOption[] option) =
        match opts with
        | Some arr -> arr |> Array.tryPick (fun opt -> match opt with | ScanPageSize n -> Some n | _ -> None)
        | _ -> None