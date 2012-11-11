// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Parser

open FParsec
open DynamoDb.SQL.Ast

exception InvalidQuery  of string

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
let pint32_ws               = pint32

let (<&&>) f g x = (f x) && (g x)
let (<||>) f g x = (f x) || (g x)

// parsers for identifiers
let hashkey     = stringCIReturn_ws "@hashkey" HashKey
let rangekey    = stringCIReturn_ws "@rangekey" RangeKey
let asterisk    = stringCIReturn_ws "*" Asterisk

let isAttrName = isLetter <||> isDigit
let attributeName   : Parser<_> = many1SatisfyL isAttrName "attribute name"
let attribute   = attributeName .>> ws |>> Attribute

let identifier  = choice [ hashkey; rangekey; asterisk; attribute ]

// only allow explicit attribute name and asterisk in select
let selectAttributes = choice [ asterisk; attribute ]
let pselect = 
    ws
    >>. skipStringCI_ws "select" 
    >>. (sepBy1 selectAttributes (pstring_ws ",") |>> Select)
    .>> ws

// parser for table names
let isTableName = isLetter <||> isDigit
let pfrom =
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
let operant = ws >>. choiceL [ (stringLiteral |>> S); (pfloat |>> N) ] "String or Numeric value" .>> ws

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

let in'                 = stringCIReturn_ws "in" In
let openBracket         = skipString_ws "("
let closeBracket        = skipString_ws ")"
let operantLst          = sepBy1 operant (ws >>. skipString_ws ",")
let inCondition         = pipe5 identifier in' openBracket operantLst closeBracket (fun id op _ lst _ -> id, op(lst))

let filterCondition     = 
    ws 
    >>. attempt unaryCondition 
        <|> attempt binaryCondition 
        <|> attempt betweenCondition 
        <|> inCondition
    .>> ws

let pwhere =
    ws
    >>. skipStringCI_ws "where"
    >>. (sepBy1 filterCondition (ws >>. and')
        |>> (fun filterLst -> filterLst |> Where))
    .>> ws

let plimit = ws >>. skipStringCI_ws "limit" >>. pint32_ws |>> Limit

// parser for a query
let pquery = tuple4 pselect pfrom (opt pwhere) (opt plimit)
             |>> (fun (select, from, where, limit) -> 
                    { Select = select; From = from; Where = where; Limit = limit })

let parseDynamoQuery str = match run pquery str with
                           | Success(result, _, _) -> result
                           | Failure(errStr, _, _) -> raise <| InvalidQuery errStr