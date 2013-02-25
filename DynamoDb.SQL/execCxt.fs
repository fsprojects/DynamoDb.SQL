// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open System
open System.Linq
open System.Runtime.CompilerServices
open Amazon.DynamoDB.DataModel
open Amazon.DynamoDB.DocumentModel
open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser

[<AutoOpen>]
module Cxt =
    let (|GetQueryConfig|) consistentRead (query : DynamoQuery) = 
        match query with
        | { From    = From(table) 
            Where   = Where(QueryCondition(hKey, rngKeyCondition))
            Action  = Select(SelectAttributes attributes)
            Limit   = limit
            Order   = order }
            -> let config = new QueryOperationConfig(ConsistentRead = consistentRead,
                                                     AttributesToGet = attributes,
                                                     HashKey = hKey.ToPrimitive())

               // optionally set the range key condition and limit if applicable
               match rngKeyCondition with 
               | Some(rndCond) -> config.Filter <- new RangeFilter(rndCond.ToCondition())
               | _ -> ()

               match limit with | Some(Limit n) -> config.Limit <- n | _ -> ()
               match order with 
               | Some(Asc)  -> config.BackwardSearch <- false
               | Some(Desc) -> config.BackwardSearch <- true
               | None       -> ()

               config
        | { Action = Count } -> raise <| NotSupportedException("Count is not supported by DynamoDBContext")
        
    let (|GetScanConfig|) (scan : DynamoScan) =
        match scan with
        | { From    = From(table)
            Where   = where
            Action  = Select(SelectAttributes attributes)
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
        | { Action = Count } -> raise <| NotSupportedException("Count is not supported by DynamoDBContext")

[<AutoOpen>]
module ContextExt = 
    type DynamoDBContext with
        member this.ExecQuery (query : string, ?consistentRead) =
            let consistentRead = defaultArg consistentRead true
            let dynamoQuery = parseDynamoQuery query
            match dynamoQuery with
            | { Limit = Some(Limit n) } & GetQueryConfig consistentRead config
                -> // NOTE: the reason the Seq.take is needed here is that the limit set in the 
                   // Query operation limit is 'per page', and DynamoDBContext lazy-loads all results
                   // see https://forums.aws.amazon.com/thread.jspa?messageID=375136&#375136
                   (this.FromQuery config).Take n
            | GetQueryConfig consistentRead config
                -> this.FromQuery config
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query operation : %s" query)

        member this.ExecScan (query : string) =
            let dynamoScan = parseDynamoScan query

            match dynamoScan with
            | GetScanConfig config -> this.FromScan config
            | _ -> raise <| InvalidScan (sprintf "Not a valid scan operation : %s" query)

[<Extension>]
[<AbstractClass>]
[<Sealed>]
type DynamoDBContextExt =
    [<Extension>]
    static member ExecQuery (cxt : DynamoDBContext, query : string) = cxt.ExecQuery(query, true)

    [<Extension>]
    static member ExecQuery (cxt : DynamoDBContext, query : string, consistentRead) = cxt.ExecQuery(query, consistentRead)

    [<Extension>]
    static member ExecScan (cxt : DynamoDBContext, query : string) = cxt.ExecScan(query)