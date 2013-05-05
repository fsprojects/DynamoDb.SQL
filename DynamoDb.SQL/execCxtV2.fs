// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDbV2.SQL.Execution

open System
open System.Linq
open System.Runtime.CompilerServices
open Amazon.DynamoDBv2.DataModel
open Amazon.DynamoDBv2.DocumentModel
open DynamoDb.SQL
open DynamoDb.SQL.Extensions

[<AutoOpen>]
module Cxt =
    let (|GetQueryConfig|) (query : DynamoQuery) = 
        match query with
        | { From    = From(table) 
            Where   = Where(QueryV2Condition keyConditions)
            Action  = Select(SelectAttributes attributes)
            Order   = order
            Options = opts }
            -> let config = new QueryOperationConfig(AttributesToGet = attributes)

               let allAttributes =  
                    match tryGetQueryIndex opts with
                    | Some(idxName, allAttributes) 
                        -> config.IndexName <- idxName
                           allAttributes
                    | _ -> true

               // you cannot specify both AttributesToGet and SPECIFIC_ATTRIBUTES in Select
               // for more details, see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
               config.Select <- match attributes, allAttributes with
                                | null, false -> SelectValues.AllProjectedAttributes
                                | null, true  -> SelectValues.AllAttributes
                                | _, _        -> SelectValues.SpecificAttributes
               
               let queryFilter = new QueryFilter()
               keyConditions |> List.iter (fun (attr, cond) -> queryFilter.AddCondition(attr, cond.ToConditionV2()))
               config.Filter <- queryFilter

               match order with 
               | Some(Asc)  -> config.BackwardSearch <- false
               | Some(Desc) -> config.BackwardSearch <- true
               | None       -> ()

               config.ConsistentRead <- isConsistentRead opts
               match tryGetQueryPageSize opts with | Some n -> config.Limit <- n | _ -> ()
               
               config
        | { Action = Count } -> raise <| NotSupportedException("Count is not supported by DynamoDBContext")
        
    let (|GetScanConfig|) (scan : DynamoScan) =
        match scan with
        | { From    = From(table)
            Where   = where
            Action  = Select(SelectAttributes attributes)
            Options = opts }
            -> let config = new ScanOperationConfig(AttributesToGet = attributes)

               // optionally set the scan filter if applicable
               match where with
               | Some(Where(ScanCondition scanFilters)) -> 
                    let scanFilter = new ScanFilter()
                    scanFilters |> List.iter (fun (attr, cond) -> scanFilter.AddCondition(attr, cond.ToConditionV2()))

                    config.Filter <- scanFilter
               | _ -> ()
               
               match tryGetScanPageSize opts with | Some n -> config.Limit <- n | _ -> ()

               config
        | { Action = Count } -> raise <| NotSupportedException("Count is not supported by DynamoDBContext")

[<AutoOpen>]
module ContextExt = 
    type DynamoDBContext with
        member this.ExecQuery (query : string) =
            let dynamoQuery = parseDynamoQueryV2 query
            match dynamoQuery with
            | { Limit = Some(Limit n) } & GetQueryConfig config 
                -> // NOTE: the reason the Seq.take is needed here is that the limit set in the 
                   // Query operation limit is 'per page', and DynamoDBContext lazy-loads all results
                   // see https://forums.aws.amazon.com/thread.jspa?messageID=375136&#375136
                   (this.FromQuery config).Take n
            | GetQueryConfig config
                -> this.FromQuery config
            | _ -> raise <| InvalidQuery (sprintf "Not a valid query operation : %s" query)

        member this.ExecScan (query : string) =
            let dynamoScan = parseDynamoScanV2 query

            match dynamoScan with
            | { Limit = Some(Limit n) } & GetScanConfig config 
                -> // NOTE: the reason the Seq.take is needed here is that the limit set in the 
                   // Scan operation limit is 'per page', and DynamoDBContext lazy-loads all results
                   // see https://forums.aws.amazon.com/thread.jspa?messageID=375136&#375136
                   (this.FromScan config).Take n
            | GetScanConfig config
                -> this.FromScan config
            | _ -> raise <| InvalidScan (sprintf "Not a valid scan operation : %s" query)

[<Extension>]
[<AbstractClass>]
[<Sealed>]
type DynamoDBContextExt =
    [<Extension>]
    static member ExecQuery (cxt : DynamoDBContext, query : string) = cxt.ExecQuery(query)

    [<Extension>]
    static member ExecScan (cxt : DynamoDBContext, query : string) = cxt.ExecScan(query)