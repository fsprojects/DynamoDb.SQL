// Author : Yan Cui (twitter @theburningmonk)

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

[<AutoOpen>]
module internal Ast =
    open Amazon.DynamoDBv2.Model
    open Amazon.DynamoDBv2.DocumentModel

    type Identifier = 
        | HashKey
        | RangeKey
        | Asterisk
        | Attribute of string

    type Operant = 
        | S         of string
        | NDouble   of double
        | NBigInt   of bigint

        member ToAttributeValue   : unit -> AttributeValue

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

        member IsAllowedInQuery : bool
        member ToCondition    : unit -> Condition

    type OrderDirection =
        | Asc
        | Desc

    type Action = 
        | Select of Identifier list
        | Count

    type From =
        From of string

    type Filter = Identifier * FilterCondition

    type Where = 
        Where of Filter list

    type Limit =
        Limit of int

    type QueryOption =
        | NoConsistentRead
        | QueryPageSize         of int
        | Index                 of string * bool // index name * all attributes
        | QueryNoReturnedCapacity

    type ScanOption =
        | ScanPageSize          of int
        | ScanSegments          of int
        | ScanNoReturnedCapacity

    type DynamoQuery =
        {
            Action  : Action
            From    : From
            Where   : Where
            Limit   : Limit option
            Order   : OrderDirection option
            Options : QueryOption [] option
        }

    type DynamoScan =
        {
            Action  : Action
            From    : From
            Where   : Where option
            Limit   : Limit option
            Options : ScanOption[] option
        }