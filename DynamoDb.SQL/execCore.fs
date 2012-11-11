// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Collections.Generic
open DynamoDb.SQL.Ast
open Amazon.DynamoDB.Model

[<AutoOpen>]
module Core =    
    let (|Key|_|) = function
        | [ (HashKey, Equal(hOp)) ] 
            -> Some <| new Key(HashKeyElement = hOp.ToAttributeValue())
        | [ (HashKey, Equal(hOp)); (RangeKey, Equal(rOp)) ]
        | [ (RangeKey, Equal(rOp)); (HashKey, Equal(hOp)) ] 
            -> Some <| new Key(HashKeyElement = hOp.ToAttributeValue(), RangeKeyElement = rOp.ToAttributeValue())
        | _ -> None
    
    let (|RangeKeyCondition|_|) (conditions : Filter list) = 
        let hKey = conditions |> List.tryPick (function | (HashKey, Equal(op)) -> Some(op.ToAttributeValue()) | _ -> None)
        let rndConds = conditions |> List.choose (function | (RangeKey, cond) -> Some(cond) | _ -> None)

        // if a hash key value is specified and there is at most one filter condition then this is a Query
        match hKey, rndConds with
        | Some(attrVal), [ cond ] -> Some(attrVal, cond)
        | _ -> None

    let (|GetByKey|Query|Scan|) = function
        | Key key -> GetByKey(key)
        | RangeKeyCondition (hKey, rngCond) -> Query(hKey, rngCond)
        | _ -> Scan

    /// Active pattern to find out whether a given identifier exists in a list of identifiers
    let (|ContainsIdentifier|) expected identifiers = identifiers |> List.exists ((=) expected)
    
    let (|SelectAttributes|) (identifiers : Identifier list) =
        match identifiers with
        | ContainsIdentifier Asterisk true -> Unchecked.defaultof<List<string>>
        | _ -> let attrValues = identifiers |> Seq.map (fun (Attribute(str)) -> str)
               new List<string>(attrValues)