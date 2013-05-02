namespace DynamoDb.SQL

[<AutoOpen>]
module internal Ast =
    type AttributeValueV1 = Amazon.DynamoDB.Model.AttributeValue
    type AttributeValueV2 = Amazon.DynamoDBv2.Model.AttributeValue

    type PrimitiveV1 = Amazon.DynamoDB.DocumentModel.Primitive
    type PrimitiveV2 = Amazon.DynamoDBv2.DocumentModel.Primitive

    type ConditionV1 = Amazon.DynamoDB.Model.Condition
    type ConditionV2 = Amazon.DynamoDBv2.Model.Condition

    type Identifier = 
        | HashKey
        | RangeKey
        | Asterisk
        | Attribute of string

    type Operant = 
        | S     of string
        | N     of double

        member ToAttributeValueV1   : unit -> AttributeValueV1
        member ToAttributeValueV2   : unit -> AttributeValueV2

        member ToPrimitiveV1        : unit -> PrimitiveV1
        member ToPrimitiveV2        : unit -> PrimitiveV2

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

        member ToConditionV1    : unit -> ConditionV1
        member ToConditionV2    : unit -> ConditionV2

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
        | NoReturnedCapacity

    type ScanOption =
        ScanPageSize        of int

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