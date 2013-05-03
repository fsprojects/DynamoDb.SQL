// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

module Ast =
    open System
    open System.Collections.Generic

    type AttributeValueV1 = Amazon.DynamoDB.Model.AttributeValue
    type AttributeValueV2 = Amazon.DynamoDBv2.Model.AttributeValue

    type PrimitiveV1 = Amazon.DynamoDB.DocumentModel.Primitive
    type PrimitiveV2 = Amazon.DynamoDBv2.DocumentModel.Primitive

    type ConditionV1 = Amazon.DynamoDB.Model.Condition
    type ConditionV2 = Amazon.DynamoDBv2.Model.Condition

    module Utils = 
        let inline appendIfSome f someOption appendee = 
            match someOption with
            | Some(x)   -> sprintf "%s %s" appendee (f x)
            | _         -> appendee

        let inline join separator (arr : string[]) = String.Join(separator, arr)

        let inline surround before after (str : string) = before + str + after

    open Utils

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type Identifier = 
        | HashKey
        | RangeKey
        | Asterisk
        | Attribute of string
        with
            override this.ToString () =
                match this with
                | HashKey        -> "@HashKey"
                | RangeKey       -> "@RangeKey"
                | Asterisk       -> "*"
                | Attribute(str) -> str

            member private this.StructuredFormatDisplay = this.ToString()

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type Operant = 
        | S     of string
        | N     of double
        with
            override this.ToString() =
                match this with
                | S(str)    -> str
                | N(n)      -> n.ToString()

            member this.ToAttributeValueV1() =
                match this with
                | S(str) -> new AttributeValueV1(S = str)
                | N(n)   -> new AttributeValueV1(N = string n)

            member this.ToAttributeValueV2() =
                match this with
                | S(str) -> new AttributeValueV2(S = str)
                | N(n)   -> new AttributeValueV2(N = string n)

            member this.ToPrimitiveV1() =
                match this with
                | S(str) -> new PrimitiveV1(str)
                | N(n)   -> new PrimitiveV1(string n, true)

            member this.ToPrimitiveV2() =
                match this with
                | S(str) -> new PrimitiveV2(str)
                | N(n)   -> new PrimitiveV2(string n, true)

            member private this.StructuredFormatDisplay = this.ToString()

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type FilterCondition = 
        | Equal                 of Operant
        | NotEqual              of Operant
        | GreaterThan           of Operant
        | GreaterThanOrEqual    of Operant
        | LessThan              of Operant
        | LessThanOrEqual       of Operant
        | NotNull
        | Null
        | Contains              of Operant
        | NotContains           of Operant
        | BeginsWith            of Operant    
        | Between               of Operant * Operant
        | In                    of Operant list
        with
            override this.ToString () =
                match this with
                | Equal(value)              -> sprintf "= %A"   value
                | NotEqual(value)           -> sprintf "!= %A"  value
                | GreaterThan(value)        -> sprintf "> %A"   value
                | GreaterThanOrEqual(value) -> sprintf ">= %A"  value
                | LessThan(value)           -> sprintf "< %A"   value
                | LessThanOrEqual(value)    -> sprintf "<= %A"  value
                | NotNull                   -> "IS NOT NULL"
                | Null                      -> "IS NULL"
                | Contains(value)           -> sprintf "CONTAINS %A" value
                | NotContains(value)        -> sprintf "NOT CONTAINS %A" value
                | BeginsWith(value)         -> sprintf "BEGINS WITH %A" value            
                | Between(value1, value2)   -> sprintf "BETWEEN %A AND %A" value1 value2
                | In(lst)                   -> sprintf "IN (%s)" (lst |> List.fold (fun acc elem -> acc + ", " + elem.ToString()) "")

            member private this.StructuredFormatDisplay = this.ToString()

            member this.IsAllowedInQuery =
                match this with
                | Equal(_) 
                | GreaterThan(_) | GreaterThanOrEqual(_) 
                | LessThan(_)    | LessThanOrEqual(_)
                | BeginsWith(_)  | Between(_)   
                    -> true
                | _ -> false

            /// returns a corresponding Condition (from the Amazon SDK)
            member this.ToConditionV1() =
                let operator, attrVals = 
                    match this with
                    | Equal(op)              -> "EQ",           seq { yield op.ToAttributeValueV1() }
                    | NotEqual(op)           -> "NE",           seq { yield op.ToAttributeValueV1() }
                    | GreaterThan(op)        -> "GT",           seq { yield op.ToAttributeValueV1() }
                    | GreaterThanOrEqual(op) -> "GE",           seq { yield op.ToAttributeValueV1() }
                    | LessThan(op)           -> "LT",           seq { yield op.ToAttributeValueV1() }
                    | LessThanOrEqual(op)    -> "LE",           seq { yield op.ToAttributeValueV1() }
                    | NotNull                -> "NOT_NULL",     Seq.empty
                    | Null                   -> "NULL",         Seq.empty
                    | Contains(op)           -> "CONTAINS",     seq { yield op.ToAttributeValueV1() }
                    | NotContains(op)        -> "NOT_CONTAINS", seq { yield op.ToAttributeValueV1() }
                    | BeginsWith(op)         -> "BEGINS_WITH",  seq { yield op.ToAttributeValueV1() }
                    | Between(op1, op2)      -> "BETWEEN",      seq { yield op1.ToAttributeValueV1(); yield op2.ToAttributeValueV1() }
                    | In(opLst)              -> "IN",           opLst |> Seq.map (fun op -> op.ToAttributeValueV1())

                new ConditionV1(ComparisonOperator = operator, AttributeValueList = new List<AttributeValueV1>(attrVals))

            /// returns a corresponding V2 Condition (from the Amazon SDK >= 1.5.18.0)
            member this.ToConditionV2() =
                let operator, attrVals = 
                    match this with
                    | Equal(op)              -> "EQ",           seq { yield op.ToAttributeValueV2() }
                    | NotEqual(op)           -> "NE",           seq { yield op.ToAttributeValueV2() }
                    | GreaterThan(op)        -> "GT",           seq { yield op.ToAttributeValueV2() }
                    | GreaterThanOrEqual(op) -> "GE",           seq { yield op.ToAttributeValueV2() }
                    | LessThan(op)           -> "LT",           seq { yield op.ToAttributeValueV2() }
                    | LessThanOrEqual(op)    -> "LE",           seq { yield op.ToAttributeValueV2() }
                    | NotNull                -> "NOT_NULL",     Seq.empty
                    | Null                   -> "NULL",         Seq.empty
                    | Contains(op)           -> "CONTAINS",     seq { yield op.ToAttributeValueV2() }
                    | NotContains(op)        -> "NOT_CONTAINS", seq { yield op.ToAttributeValueV2() }
                    | BeginsWith(op)         -> "BEGINS_WITH",  seq { yield op.ToAttributeValueV2() }
                    | Between(op1, op2)      -> "BETWEEN",      seq { yield op1.ToAttributeValueV2(); yield op2.ToAttributeValueV2() }
                    | In(opLst)              -> "IN",           opLst |> Seq.map (fun op -> op.ToAttributeValueV2())

                new ConditionV2(ComparisonOperator = operator, AttributeValueList = new List<AttributeValueV2>(attrVals))

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]     
    type OrderDirection =
        | Asc
        | Desc
        with
            override this.ToString() =
                match this with
                | Asc   -> "ORDER ASC"
                | Desc  -> "ORDER DESC"

            member private this.StructuredFormatDisplay = this.ToString()
            
    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type Action = 
        | Select of Identifier list
        | Count
        with 
            override this.ToString () = 
                match this with 
                | Select(lst) -> sprintf "SELECT %s" <| System.String.Join(", ", lst)
                | Count       -> "COUNT *"

            member private this.StructuredFormatDisplay = this.ToString()

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type From =
        From of string
        with 
            override this.ToString () = 
                match this with 
                | From(str) -> "FROM " + str

            member private this.StructuredFormatDisplay = this.ToString()

    type Filter = Identifier * FilterCondition

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type Where = 
        Where of Filter list
        with 
            override this.ToString () = 
                match this with 
                | Where(lst) -> 
                    lst
                    |> List.map (fun (id, condition) -> sprintf "%A %A" id condition)
                    |> (fun lst -> sprintf "WHERE %s" <| System.String.Join(", ", lst))

            member private this.StructuredFormatDisplay = this.ToString()

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type Limit =
        Limit of int
        with 
            override this.ToString () =
                match this with 
                | Limit(n) -> sprintf "LIMIT %d" n

            member private this.StructuredFormatDisplay = this.ToString()

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type QueryOption =
        | NoConsistentRead
        | QueryPageSize         of int
        | Index                 of string * bool // index name * all attributes
        | QueryNoReturnedCapacity
        with
            override this.ToString () =
                match this with
                | NoConsistentRead          -> "NOCONSISTENTREAD"
                | QueryPageSize n           -> sprintf "PAGESIZE(%d)" n
                | Index(idx, allAttrs)      -> sprintf "INDEX(%s, %b)" idx allAttrs
                | QueryNoReturnedCapacity   -> "NORETURNEDCAPACITY"

            member private this.StructuredFormatDisplay = this.ToString()

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type ScanOption =
        | ScanPageSize        of int
        | ScanNoReturnedCapacity
        with
            override this.ToString () =
                match this with
                | ScanPageSize n            -> sprintf "PAGESIZE(%d)" n
                | ScanNoReturnedCapacity    -> "NORETURNEDCAPACITY"

            member private this.StructuredFormatDisplay = this.ToString()

    /// Represents a query against data in DynamoDB
    type DynamoQuery =
        {
            Action  : Action
            From    : From
            Where   : Where
            Limit   : Limit option
            Order   : OrderDirection option
            Options : QueryOption [] option
        }

        override this.ToString () = 
            sprintf "%s %s" (string this.Action) (string this.From)
            |> appendIfSome string this.Limit
            |> appendIfSome string this.Order
            |> appendIfSome (Array.map string >> join "," >> surround "WITH (" ")") this.Options

    /// Represents a scan against data in DynamoDB
    type DynamoScan =
        {
            Action  : Action
            From    : From
            Where   : Where option
            Limit   : Limit option
            Options : ScanOption[] option
        }

        override this.ToString () = 
            sprintf "%s %s" (string this.Action) (string this.From)
            |> appendIfSome string this.Where
            |> appendIfSome string this.Limit
            |> appendIfSome (Array.map string >> join "," >> surround "WITH (" ")") this.Options