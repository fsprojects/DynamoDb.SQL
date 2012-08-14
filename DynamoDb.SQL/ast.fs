// Copyright (c) Yan Cui 2012

module DynamoDb.SQL.Ast

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type Identifier = HashKey
                | RangeKey
                | Attribute of string
                with
                    member private id.StructuredFormatDisplay =
                        match id with
                        | HashKey        -> "@HashKey"
                        | RangeKey       -> "@RangeKey"
                        | Attribute(str) -> str

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type Operator = Equal
              | GreaterThan
              | GreaterThanOrEqual
              | LessThan
              | LessThanOrEqual
              | BeginsWith
              with
                member private op.StructuredFormatDisplay =
                    match op with
                    | Equal              -> "="
                    | GreaterThan        -> ">"
                    | GreaterThanOrEqual -> ">="
                    | LessThan           -> "<"
                    | LessThanOrEqual    -> "<="
                    | BeginsWith         -> "BeginsWith"

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type QueryExpression = Select       of string list
                     | From         of string
                     | Where        of (Identifier * Operator * obj)[]
                     | Limit        of int
                     with
                        member private ex.StructuredFormatDisplay =
                            match ex with
                            | Select(attributes) -> sprintf "SELECT %A" attributes
                            | From(tableName)    -> sprintf "FROM %s" tableName
                            | Where(filters)     -> sprintf "WHERE %A" filters
                            | Limit(n)           -> sprintf "LIMIT %d" n