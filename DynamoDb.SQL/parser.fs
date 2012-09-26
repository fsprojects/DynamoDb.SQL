// Copyright (c) Yan Cui 2012

module DynamoDb.SQL.Parser

open FParsec
open DynamoDb.SQL.Ast

type Parser<'t> = Parser<'t, unit>

// abbreviations
let ws = spaces     // eats any whitespace

// helper functions that ignores subsequent whitespaces
let pstring_ws s            = pstring s .>> ws
let pstringCI_ws s          = pstringCI s .>> ws
let skipString_ws s         = skipString s .>> ws
let skipStringCI_ws s       = skipStringCI s .>> ws
let stringReturn_ws s r     = stringReturn s r .>> ws
let stringCIReturn_ws s r   = stringCIReturn s r .>> ws
let pfloat_ws               = pfloat
let pint64_ws               = pint64
let pint32_ws               = pint32
let pint16_ws               = pint16
let pint8_ws                = pint8

let (<&&>) f g x = (f x) && (g x)
let (<||>) f g x = (f x) || (g x)

// parsers for identifiers
let hashkey     = stringCIReturn_ws "@hashkey" HashKey
let rangekey    = stringCIReturn_ws "@rangekey" RangeKey

let isAttrName = isLetter <||> isDigit <||> ((=) '*')
let attributeName   : Parser<_> = many1SatisfyL isAttrName "attribute name"
let attribute   = attributeName .>> ws |>> Attribute

let identifier  = choice [ hashkey; rangekey; attribute ]

let attributeNames = 
    ws
    >>. skipStringCI_ws "select" 
    >>. (sepBy1 identifier (pstring_ws ",") |>> Select)
    .>> ws

// parser for table names
let isTableName = isLetter <||> isDigit
let tableName =
    ws
    >>. skipStringCI_ws "from"
    >>. ((many1SatisfyL isTableName "table name") |>> From)
    .>> ws

let stringLiteral =
    let normalCharSnippet = manySatisfy (fun c -> c <> '\\' && c <> '"')
    let escapedChar = pstring "\\" >>. (anyOf "\\nrt\"" |>> function
                                                            | 'n' -> "\n"
                                                            | 'r' -> "\r"
                                                            | 't' -> "\t"
                                                            | c   -> string c)
    between (pstring "\"") (pstring "\"")
            (stringsSepBy normalCharSnippet escapedChar)

// parser for the operant (string or numeric value)
let operant = choiceL [ (stringLiteral |>> S); (pfloat |>> N) ] "String or Numeric value" .>> ws

// parsers for binary/unary/between conditions
let binaryOperators     = choice [ stringReturn_ws "=" Equal;           
                                   stringReturn_ws "!=" NotEqual;
                                   stringReturn_ws ">=" GreaterThanOrEqual;
                                   stringReturn_ws ">" GreaterThan;                                   
                                   stringReturn_ws "<=" LessThanOrEqual;
                                   stringReturn_ws "<" LessThan;                                   
                                   stringCIReturn_ws "contains" Contains;
                                   stringCIReturn_ws "not contains" NotContains;
                                   stringCIReturn_ws "begins with" BeginsWith ]
let binaryCondition     = pipe3 identifier binaryOperators operant (fun id op v -> id, op v)

let unaryOperators      = choice [ stringCIReturn_ws "is null" Null; 
                                   stringCIReturn_ws "is not null" NotNull ]
let unaryCondition      = pipe2 identifier unaryOperators (fun id op -> id, op)

let between             = stringCIReturn_ws "between" Between
let and'                = skipStringCI_ws "and"
let betweenCondition    = pipe5 identifier between operant and' operant (fun id op v1 _ v2 -> id, op(v1, v2))

// TODO - add support for 'IN'

let filterCondition     = ws >>. choiceL [ binaryCondition; unaryCondition; betweenCondition ] "Invalid condition" .>> ws

let where =
    ws
    >>. skipStringCI_ws "where"
    >>. (sepBy1 filterCondition (ws >>. and')
        |>> (fun filterLst -> filterLst |> Where))
    .>> ws

let limit = ws >>. skipStringCI_ws "limit" >>. pint32_ws |>> Limit

let query = tuple4 attributeNames tableName (opt where) (opt limit)
            |>> (fun (select, from, where, limit) -> 
                    { Select = select; From = from; Where = where; Limit = limit })

let parse = run query

// helper active patterns
let (|IsSuccess|)   = function | Success(_) -> true | _ -> false
let (|IsFailure|_|) = function | Failure(errMsg, _, _) -> Some(errMsg) | _ -> None
let (|GetSelect|_|) = function | Success({ Select = Select(attrLst) }, _, _) -> Some(attrLst) | _ -> None
let (|GetFrom|_|)   = function | Success({ From = From(table) }, _, _) -> Some(table) | _ -> None
let (|GetWhere|_|)  = function | Success({ Where = Some(Where(filters)) }, _, _) -> Some(filters) | _ -> None
let (|GetLimit|_|)  = function | Success({ Limit = Some(Limit(n)) }, _, _) -> Some(n) | _ -> None