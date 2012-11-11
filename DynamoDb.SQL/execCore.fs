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
    let (|QueryCondition|_|) (conditions : Filter list) = 
        let hKey = conditions |> List.tryPick (function | (HashKey, Equal(op)) -> Some(op) | _ -> None)
        let rndConds = conditions |> List.choose (function | (RangeKey, cond) when cond.IsAllowedInQuery -> Some(cond) | _ -> None)

        // if a hash key value is specified and there is at most one filter condition then this is a Query
        match hKey, rndConds with
        | Some(hOp), [] -> Some(hOp, None)
        | Some(hOp), [ cond ] -> Some(hOp, Some cond)
        | _ -> None

    let (|ScanCondition|) (conditions : Filter list) =
        // since @HashKey and @RangeKey aren't the actual names of the attributes, do not allow them in scan queries
        let hasKey = conditions |> List.exists (function | (HashKey, _) | (RangeKey, _) -> true | _ -> false)
        if hasKey then raise <| InvalidScan "@HashKey and @RangeKey cannot be used in Scan queries"

        // no Asterisk allowed in the where clause by the parser, so ok to assume we only have attribute names in there
        conditions |> List.map (fun (Attribute name, cond) -> name, cond)

    let (|Query|Scan|) = function
        | QueryCondition (hKey, rngCond)    -> Query(hKey, rngCond)
        | ScanCondition scanFilters         -> Scan(scanFilters)

    /// Active pattern to find out whether a given identifier exists in a list of identifiers
    let (|ContainsIdentifier|) expected identifiers = identifiers |> List.exists ((=) expected)
    
    let (|SelectAttributes|) (identifiers : Identifier list) =
        match identifiers with
        | ContainsIdentifier Asterisk true 
            -> Unchecked.defaultof<List<string>>
        | _ -> // only asterisk and attribute names are allowed in select so ok to assume attribute names here
               let attrValues = identifiers |> Seq.map (fun (Attribute name) -> name)
               new List<string>(attrValues)