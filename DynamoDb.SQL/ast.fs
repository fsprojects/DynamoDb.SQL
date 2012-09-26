// Copyright (c) Yan Cui 2012

module DynamoDb.SQL.Ast

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type Identifier = 
    | HashKey
    | RangeKey
    | Attribute of string
    with
        override this.ToString () =
            match this with
            | HashKey        -> "@HashKey"
            | RangeKey       -> "@RangeKey"
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

        member private this.StructuredFormatDisplay = this.ToString()

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type Condition = 
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

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type Where = 
    Where of (Identifier * Condition) list
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

type DynamoQuery =
    {
        Select          : Select;
        From            : From;
        Where           : Where option;
        Limit           : Limit option
    }

    override this.ToString () =
        sprintf "%A %A" this.Select this.From