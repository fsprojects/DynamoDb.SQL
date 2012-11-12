// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System.Runtime.CompilerServices
open Amazon.DynamoDB.DataModel
open Amazon.DynamoDB.DocumentModel
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser

[<AutoOpen>]
module Helper =
    let (|IsQueryConfig|IsScanConfig|) = function
        // handle invalid requests
        | { Select = Select([]) }   -> raise EmptySelect
        | { From = From("") }       -> raise EmptyFrom
        // handle Query requests
        | { From = From(table); Where = Some(Where(Query(hKey, rngKeyCondition)));
            Select = Select(SelectAttributes attributes); Limit = limit }
            -> let config = new QueryOperationConfig(ConsistentRead = true, // TODO
                                                     AttributesToGet = attributes,
                                                     HashKey = hKey.ToPrimitive())

               // optionally set the range key condition and limit if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> config.Filter <- new RangeFilter(rndCond.ToCondition())
               | _ -> ()

               match limit with | Some(Limit n) -> config.Limit <- n | _ -> ()
               
               IsQueryConfig config
        // handle Scan requests
        | { From = From(table); Where = Some(Where(Scan(scanFilters)));
            Select = Select(SelectAttributes attributes); Limit = limit }
            -> let scanFilter = new ScanFilter()
               scanFilters |> List.iter (fun (attr, cond) -> scanFilter.AddCondition(attr, cond.ToCondition()))
               
               let config = new ScanOperationConfig(AttributesToGet = attributes, Filter = scanFilter)

               // optionally set the limit if applicable
               match limit with | Some(Limit n) -> config.Limit <- n | _ -> ()

               IsScanConfig config

[<Extension>]
[<AbstractClass>]
[<Sealed>]
type DynamoDBContextExt =
    [<Extension>]
    static member ExecQuery<'T> (cxt : DynamoDBContext, query : string) =
        let dynamoQuery = parseDynamoQuery query
        let co = new DynamoDBOperationConfig()
        match dynamoQuery with
        | IsQueryConfig config -> cxt.FromQuery<'T> config
        | _ -> raise <| InvalidQuery (sprintf "Not a valid query operation : %s" query)

    [<Extension>]
    static member ExecScan (cxt : DynamoDBContext, query : string) =
        let dynamoQuery = parseDynamoQuery query

        match dynamoQuery with
        | IsScanConfig config -> cxt.FromScan<'T> config
        | _ -> raise <| InvalidQuery (sprintf "Not a valid scan operation : %s" query)