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
let skipStringCI_ws s       = skipStringCI s .>> ws
let stringReturn_ws s r     = stringReturn s r .>> ws
let stringCIReturn_ws s r   = stringCIReturn s r .>> ws
let pfloat_ws               = pfloat
let pint64_ws               = pint64
let pint32_ws               = pint32
let pint16_ws               = pint16
let pint8_ws                = pint8

// parsers for attribute names
let isAttrName c = isLetter c || isDigit c || c = '*'
let attributeName   : Parser<_> = many1SatisfyL isAttrName "attribute name"
let attributeNames = 
    ws
    >>. skipStringCI_ws "select" 
    >>. (sepBy1 attributeName (pstring_ws ",") |>> Select)
    .>> ws

// parser for table names
let isTableName c = isLetter c || isDigit c
let tableName =
    ws
    >>. skipStringCI_ws "from"
    >>. ((many1SatisfyL isTableName "table name") |>> From)
    .>> ws

// parsers for identifiers in the where clauses
let hashkey     = stringCIReturn_ws "@hashkey" HashKey
let rangekey    = stringCIReturn_ws "@rangekey" RangeKey
let attribute   = attributeName .>> ws |>> Attribute
let identifier  = choice [ hashkey; rangekey; attribute ]

// parsers for operators
let equal               = stringReturn_ws "=" Equal
let greaterThan         = stringReturn_ws ">" GreaterThan
let greaterThanOrEqual  = stringReturn_ws ">=" GreaterThanOrEqual
let lessThan            = stringReturn_ws "<" LessThan
let lessThanOrEqual     = stringReturn_ws "<=" LessThanOrEqual
let beginsWith          = stringReturn_ws "BeginsWith" BeginsWith
let operators           = choice [ greaterThanOrEqual; 
                                   lessThanOrEqual; 
                                   equal; 
                                   greaterThan; 
                                   lessThan;
                                   beginsWith ]

let stringLiteral =
    let normalCharSnippet = manySatisfy (fun c -> c <> '\\' && c <> '"')
    let escapedChar = pstring "\\" >>. (anyOf "\\nrt\"" |>> function
                                                            | 'n' -> "\n"
                                                            | 'r' -> "\r"
                                                            | 't' -> "\t"
                                                            | c   -> string c)
    between (pstring "\"") (pstring "\"")
            (stringsSepBy normalCharSnippet escapedChar)

let value = choiceL [ (stringLiteral |>> box); (pfloat |>> box) ] "String or Numeric value" .>> ws

let filterCondition = ws >>. pipe3 identifier operators value (fun id op v -> id, op, v) .>> ws

let where =
    ws
    >>. skipStringCI_ws "where"
    >>. (sepBy1 filterCondition (ws >>. pstringCI_ws "and")
        |>> (fun filterLst -> filterLst |> List.toArray |> Where))
    .>> ws

let limit = ws >>. skipStringCI_ws "limit" >>. pint32_ws |>> Limit

let query = tuple4 attributeNames tableName (opt where) (opt limit)

let parse = run query

// helper active patterns
let (|IsSuccess|)   = function | Success(_) -> true | _ -> false
let (|IsFailure|_|) = function | Failure(errMsg, _, _) -> Some(errMsg) | _ -> None
let (|GetSelect|_|) = function | Success((Select(attrLst), _, _, _), _, _) -> Some(attrLst) | _ -> None
let (|GetFrom|_|)   = function | Success((_, From(table), _, _), _, _) -> Some(table) | _ -> None
let (|GetWhere|_|)  = function | Success((_, _, Some(Where(filters)), _), _, _) -> Some(filters) | _ -> None
let (|GetLimit|_|)  = function | Success((_, _, _, Some(Limit(n))), _, _) -> Some(n) | _ -> None