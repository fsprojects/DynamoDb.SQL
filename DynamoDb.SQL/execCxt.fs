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
module Cxt =
    let (|GetQueryConfig|) (query : DynamoQuery) = 
        match query with
        | { From    = From(table) 
            Where   = Where(QueryCondition(hKey, rngKeyCondition))
            Select  = Select(SelectAttributes attributes)
            Limit   = limit }
            -> let config = new QueryOperationConfig(ConsistentRead = true, // TODO
                                                     AttributesToGet = attributes,
                                                     HashKey = hKey.ToPrimitive())

               // optionally set the range key condition and limit if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> config.Filter <- new RangeFilter(rndCond.ToCondition())
               | _ -> ()

               match limit with | Some(Limit n) -> config.Limit <- n | _ -> ()
               
               config
        
    let (|GetScanConfig|) (scan : DynamoScan) =
        match scan with
        | { From    = From(table)
            Where   = where
            Select  = Select(SelectAttributes attributes)
            Limit   = limit }
            -> let config = new ScanOperationConfig(AttributesToGet = attributes)

               // optionally set the scan filter and limit if applicable
               match where with
               | Some(Where(ScanCondition scanFilters)) -> 
                    let scanFilter = new ScanFilter()
                    scanFilters |> List.iter (fun (attr, cond) -> scanFilter.AddCondition(attr, cond.ToCondition()))

                    config.Filter <- scanFilter
               | _ -> ()
               
               match limit with | Some(Limit n) -> config.Limit <- n | _ -> ()

               config

[<Extension>]
[<AbstractClass>]
[<Sealed>]
type DynamoDBContextExt =
    [<Extension>]
    static member ExecQuery<'T> (cxt : DynamoDBContext, query : string) =
        let dynamoQuery = parseDynamoQuery query
        let co = new DynamoDBOperationConfig()
        match dynamoQuery with
        | GetQueryConfig config -> cxt.FromQuery<'T> config
        | _ -> raise <| InvalidQuery (sprintf "Not a valid query operation : %s" query)

    [<Extension>]
    static member ExecScan (cxt : DynamoDBContext, query : string) =
        let dynamoScan = parseDynamoScan query

        match dynamoScan with
        | GetScanConfig config -> cxt.FromScan<'T> config
        | _ -> raise <| InvalidScan (sprintf "Not a valid scan operation : %s" query)