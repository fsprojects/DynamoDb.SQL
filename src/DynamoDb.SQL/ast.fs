// Author : Yan Cui (twitter @theburningmonk)

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

module Ast =
    open System
    open System.Collections.Generic
    open Amazon.DynamoDBv2
    open Amazon.DynamoDBv2.Model
    open Amazon.DynamoDBv2.DocumentModel

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

            member this.ToAttributeValue() =
                match this with
                | S(str) -> new AttributeValue(S = str)
                | N(n)   -> new AttributeValue(N = string n)

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

            /// returns a corresponding V2 Condition (from the Amazon SDK >= 1.5.18.0)
            member this.ToCondition() =
                let operator, attrVals = 
                    match this with
                    | Equal(op)              -> ComparisonOperator.EQ,           seq { yield op.ToAttributeValue() }
                    | NotEqual(op)           -> ComparisonOperator.NE,           seq { yield op.ToAttributeValue() }
                    | GreaterThan(op)        -> ComparisonOperator.GT,           seq { yield op.ToAttributeValue() }
                    | GreaterThanOrEqual(op) -> ComparisonOperator.GE,           seq { yield op.ToAttributeValue() }
                    | LessThan(op)           -> ComparisonOperator.LT,           seq { yield op.ToAttributeValue() }
                    | LessThanOrEqual(op)    -> ComparisonOperator.LE,           seq { yield op.ToAttributeValue() }
                    | NotNull                -> ComparisonOperator.NOT_NULL,     Seq.empty
                    | Null                   -> ComparisonOperator.NULL,         Seq.empty
                    | Contains(op)           -> ComparisonOperator.CONTAINS,     seq { yield op.ToAttributeValue() }
                    | NotContains(op)        -> ComparisonOperator.NOT_CONTAINS, seq { yield op.ToAttributeValue() }
                    | BeginsWith(op)         -> ComparisonOperator.BEGINS_WITH,  seq { yield op.ToAttributeValue() }
                    | Between(op1, op2)      -> ComparisonOperator.BETWEEN,      seq { yield op1.ToAttributeValue(); yield op2.ToAttributeValue() }
                    | In(opLst)              -> ComparisonOperator.IN,           opLst |> Seq.map (fun op -> op.ToAttributeValue())

                new Condition(ComparisonOperator = operator, AttributeValueList = new List<AttributeValue>(attrVals))

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
        | ScanPageSize          of int
        | ScanSegments          of int
        | ScanNoReturnedCapacity
        with
            override this.ToString () =
                match this with
                | ScanPageSize n            -> sprintf "PAGESIZE(%d)" n
                | ScanSegments n            -> sprintf "SEGMENTS(%d)" n
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