// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Ast

open System.Collections.Generic
open Amazon.DynamoDB.Model
open Amazon.DynamoDB.DocumentModel

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

        member this.ToPrimitive() =
            match this with
            | S(str) -> new Primitive(str)
            | N(n)   -> new Primitive(string n, true)

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
        member this.ToCondition() =
            let operator, attrVals = 
                match this with
                | Equal(op)              -> "EQ",           seq { yield op.ToAttributeValue() }
                | NotEqual(op)           -> "NE",           seq { yield op.ToAttributeValue() }
                | GreaterThan(op)        -> "GT",           seq { yield op.ToAttributeValue() }
                | GreaterThanOrEqual(op) -> "GE",           seq { yield op.ToAttributeValue() }
                | LessThan(op)           -> "LT",           seq { yield op.ToAttributeValue() }
                | LessThanOrEqual(op)    -> "LE",           seq { yield op.ToAttributeValue() }
                | NotNull                -> "NOT_NULL",     Seq.empty
                | Null                   -> "NULL",         Seq.empty
                | Contains(op)           -> "CONTAINS",     seq { yield op.ToAttributeValue() }
                | NotContains(op)        -> "NOT_CONTAINS", seq { yield op.ToAttributeValue() }
                | BeginsWith(op)         -> "BEGINS_WITH",  seq { yield op.ToAttributeValue() }
                | Between(op1, op2)      -> "BETWEEN",      seq { yield op1.ToAttributeValue(); yield op2.ToAttributeValue() }
                | In(opLst)              -> "IN",           opLst |> Seq.map (fun op -> op.ToAttributeValue())

            new Condition(ComparisonOperator = operator, AttributeValueList = new List<AttributeValue>(attrVals))
            
[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type Select = 
    Select of Identifier list
    with 
        override this.ToString () = 
            match this with 
            | Select(lst) -> sprintf "SELECT %s" <| System.String.Join(", ", lst)

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

/// Represents a query against data in DynamoDB
type DynamoQuery =
    {
        Select          : Select
        From            : From
        Where           : Where
        Limit           : Limit option
    }

    override this.ToString () = 
        match this.Limit with
        | Some(limit) -> sprintf "%A %A %A %A" this.Select this.From this.Where limit
        | _           -> sprintf "%A %A %A" this.Select this.From this.Where

/// Represents a scan against data in DynamoDB
type DynamoScan =
    {
        Select          : Select
        From            : From
        Where           : Where option
        Limit           : Limit option
    }

    override this.ToString () = 
        let appendIfSome someOption appendee = 
            match someOption with
            | Some(x)   -> sprintf "%A %A" appendee x
            | _         -> appendee

        sprintf "%A %A" this.Select this.From
        |> appendIfSome this.Where
        |> appendIfSome this.Limit