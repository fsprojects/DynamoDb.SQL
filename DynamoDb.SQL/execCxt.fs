// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

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

    type DynamoDBContext with
        member this.Query<'T> (query : string) =
            let dynamoQuery = parseDynamoQuery query
            let co = new DynamoDBOperationConfig()
            match dynamoQuery with
            | IsQueryConfig config -> this.FromQuery<'T> config
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query operation : %s" query)

        member this.Scan (query : string) =
            let dynamoQuery = parseDynamoQuery query

            match dynamoQuery with
            | IsScanConfig config -> this.FromScan<'T> config
            | _ -> raise <| InvalidQuery (sprintf "Not a valid scan operation : %s" query)