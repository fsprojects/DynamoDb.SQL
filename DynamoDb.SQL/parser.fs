// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL

open System
open FParsec
open DynamoDb.SQL

exception InvalidTableName  of string
exception InvalidQuery      of string
exception InvalidScan       of string

[<AutoOpen>]
module Common =
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
    let pfloat_ws               = pfloat .>> ws
    let pint32_ws               = pint32 .>> ws

    open FParsec.Internals

    // FParsec only supports up to pipe5, extend it by piping the result of the first 5 parser through a second pipe
    let pipe6 (p1: Parser<'a,'u>) (p2: Parser<'b,'u>) (p3: Parser<'c,'u>) (p4: Parser<'d,'u>) (p5: Parser<'e,'u>) (p6: Parser<'f, 'u>) g =
        pipe2 (pipe5 p1 p2 p3 p4 p5 (fun a b c d e -> a, b, c, d, e)) p6 (fun (a, b, c, d, e) f -> g a b c d e f)

    let tuple6 p1 p2 p3 p4 p5 p6 = pipe6 p1 p2 p3 p4 p5 p6 (fun a b c d e f -> (a, b, c, d, e, f))

    let (<&&>) f g x = (f x) && (g x)
    let (<||>) f g x = (f x) || (g x)

    // parsers for identifiers
    let hashkey     = stringCIReturn_ws "@hashkey" HashKey
    let rangekey    = stringCIReturn_ws "@rangekey" RangeKey
    let asterisk    = stringCIReturn_ws "*" Asterisk

    let isAttrName  = isLetter <||> isDigit
    let attributeName   : Parser<_> = many1SatisfyL isAttrName "attribute name"
    let attribute   = attributeName .>> ws |>> Attribute

    // only allow explicit attribute name and asterisk in select
    let selectAttributes = choice [ asterisk; attribute ]
    let pselect = skipStringCI_ws "select" 
                  >>. (sepBy1 selectAttributes (pstring_ws ",") |>> Select)

    // count action cannot specify a list of attributes
    let pcount = stringCIReturn_ws "count *" Count

    // an action is either select or count
    let paction = ws >>. choice [pselect; pcount] .>> ws

    // parser for table names
    let isTableName = isLetter <||> isDigit
    let pfrom =
        ws
        >>. skipStringCI_ws "from"
        >>. ((many1SatisfyL isTableName "table name")             
            |>> (function | name when name.Equals("where", StringComparison.CurrentCultureIgnoreCase)
                                    -> raise <| InvalidTableName name
                          | name when name.Equals("limit", StringComparison.CurrentCultureIgnoreCase)
                                    -> raise <| InvalidTableName name
                          | name -> From name))
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

    let openParentheses   = skipString_ws "("
    let closeParentheses  = skipString_ws ")"
    let comma             = pstring_ws ","

    // parser for the operant (string or numeric value)
    let operant = ws >>. choiceL [ (stringLiteral |>> S); (pfloat |>> N) ] "String or Numeric value" .>> ws

    let plimit = ws >>. skipStringCI_ws "limit" >>. pint32_ws |>> Limit

    // parser for the order directions
    let porder = 
        ws 
        >>. choice [ stringCIReturn_ws "order asc" Asc
                     stringCIReturn_ws "order desc" Desc ]
        .>> ws

module Parser =
    /// Query V1 - supports only query by hash and range key
    [<RequireQualifiedAccess>]
    module QueryV1Parser =
        // allow @hashkey, @rangekey in the Where clause for a QUERY
        let whereAttributes     = choice [ hashkey; rangekey ]

        // parsers for binary/between conditions allowed in a QUERY
        let binaryOperators     = choice [ stringReturn_ws "="  Equal
                                           stringReturn_ws ">=" GreaterThanOrEqual
                                           stringReturn_ws ">"  GreaterThan
                                           stringReturn_ws "<=" LessThanOrEqual
                                           stringReturn_ws "<"  LessThan
                                           stringCIReturn_ws "begins with" BeginsWith ]
        let binaryCondition     = pipe3 whereAttributes binaryOperators operant (fun id op v -> id, op v)

        let between             = stringCIReturn_ws "between" Between
        let and'                = skipStringCI_ws "and"
        let betweenCondition    = pipe5 whereAttributes between operant and' operant (fun id op v1 _ v2 -> id, op(v1, v2))

        let pcondition          = choice [ attempt binaryCondition; attempt betweenCondition ]
        let filterConditions    = sepBy1 (ws >>. pcondition .>> ws) (ws >>. and')
        let pwhere = ws >>. skipStringCI_ws "where" >>. (filterConditions |>> Where) .>> ws

        let consistentRead      = stringCIReturn_ws "noconsistentread" NoConsistentRead
        let queryPageSize       = skipStringCI_ws "pagesize" >>. openParentheses >>. pint32_ws .>> closeParentheses |>> QueryPageSize
        let queryOption         = choice [ consistentRead; queryPageSize ]
        let queryOptions        = sepBy1 queryOption comma |>> List.toArray
        let pwith = ws >>. skipStringCI_ws "with" >>. openParentheses >>. queryOptions .>> closeParentheses

        // parser for a query
        let pquery : Parser<DynamoQuery, unit> = 
            tuple6 paction pfrom pwhere (opt porder) (opt plimit) (opt pwith)
            |>> (fun (action, from, where, order, limit, with') -> 
                    { Action = action; From = from; Where = where; Limit = limit; Order = order; Options = with' })

    /// Query V2 - supports the use of Local Secondary Index, but due to API changes no longer supports the use of @HashKey and @RangeKey
    [<RequireQualifiedAccess>]
    module QueryV2Parser =
        let whereAttributes     = attribute

        // parsers for binary/between conditions allowed in a QUERY
        let binaryOperators     = choice [ stringReturn_ws "="  Equal
                                           stringReturn_ws ">=" GreaterThanOrEqual
                                           stringReturn_ws ">"  GreaterThan
                                           stringReturn_ws "<=" LessThanOrEqual
                                           stringReturn_ws "<"  LessThan
                                           stringCIReturn_ws "begins with" BeginsWith ]
        let binaryCondition     = pipe3 whereAttributes binaryOperators operant (fun id op v -> id, op v)

        let between             = stringCIReturn_ws "between" Between
        let and'                = skipStringCI_ws "and"
        let betweenCondition    = pipe5 whereAttributes between operant and' operant (fun id op v1 _ v2 -> id, op(v1, v2))

        let pcondition          = choice [ attempt binaryCondition; attempt betweenCondition ]
        let filterConditions    = sepBy1 (ws >>. pcondition .>> ws) (ws >>. and')
        let pwhere = ws >>. skipStringCI_ws "where" >>. (filterConditions |>> Where) .>> ws

        let consistentRead      = stringCIReturn_ws "noconsistentread" NoConsistentRead
        let queryPageSize       = skipStringCI_ws "pagesize" >>. openParentheses >>. pint32_ws .>> closeParentheses |>> QueryPageSize

        // DynamoDB allows a-z, A-Z, 0-9, _, - and . in the index name
        let isValidChar c       = isAsciiLetter c || isDigit c || c = '_' || c = '-' || c = '.'
        let pIndexName          = ws >>. identifier (IdentifierOptions(isAsciiIdStart = isValidChar, isAsciiIdContinue = isValidChar)) .>> ws
    
        let pTrue, pFalse       = stringCIReturn_ws "true" true, stringCIReturn_ws "false" false
        let index               = skipStringCI_ws "index" 
                                  >>. openParentheses 
                                  >>. (tuple3 pIndexName comma (choice [ pTrue; pFalse ])
                                       |>> (fun (idxName, _, isAll) -> idxName, isAll))
                                  .>> closeParentheses 
                                  |>> Index

        let noReturnedCapacity  = stringCIReturn_ws "NoReturnedCapacity" QueryNoReturnedCapacity

        let queryOption         = choice [ consistentRead; queryPageSize; index; noReturnedCapacity ]
        let queryOptions        = sepBy1 queryOption comma |>> List.toArray
        let pwith = ws >>. skipStringCI_ws "with" >>. openParentheses >>. queryOptions .>> closeParentheses

        // parser for a query
        let pquery : Parser<DynamoQuery, unit> = 
            tuple6 paction pfrom pwhere (opt porder) (opt plimit) (opt pwith)
            |>> (fun (action, from, where, order, limit, with') -> 
                    { Action = action; From = from; Where = where; Limit = limit; Order = order; Options = with' })

    [<RequireQualifiedAccess>]
    module ScanV1Parser = 
        // only allow attributes in the Where clause for a SCAN
        let whereAttributes     = attribute

        // parsers for binary/unary/between conditions allowed in a SCAN
        let binaryOperators     = choice [ stringReturn_ws "="  Equal
                                           stringReturn_ws "!=" NotEqual
                                           stringReturn_ws ">=" GreaterThanOrEqual
                                           stringReturn_ws ">"  GreaterThan
                                           stringReturn_ws "<=" LessThanOrEqual
                                           stringReturn_ws "<"  LessThan
                                           stringCIReturn_ws "contains" Contains
                                           stringCIReturn_ws "not contains" NotContains
                                           stringCIReturn_ws "begins with" BeginsWith ]
        let binaryCondition     = pipe3 whereAttributes binaryOperators operant (fun id op v -> id, op v)

        let unaryOperators      = choice [ stringCIReturn_ws "is null" Null; 
                                           stringCIReturn_ws "is not null" NotNull ]
        let unaryCondition      = pipe2 whereAttributes unaryOperators (fun id op -> id, op)

        let between             = stringCIReturn_ws "between" Between
        let and'                = skipStringCI_ws "and"
        let betweenCondition    = pipe5 whereAttributes between operant and' operant (fun id op v1 _ v2 -> id, op(v1, v2))

        let in'                 = stringCIReturn_ws "in" In    
        let operantLst          = sepBy1 operant (ws >>. skipString_ws ",")
        let inCondition         = pipe5 whereAttributes in' openParentheses operantLst closeParentheses (fun id op _ lst _ -> id, op(lst))

        let pcondition          = choice [ attempt unaryCondition;   attempt binaryCondition;
                                           attempt betweenCondition; inCondition ]
        let filterConditions    = sepBy1 (ws >>. pcondition .>> ws) (ws >>. and')

        let pwhere = ws >>. skipStringCI_ws "where" >>. (filterConditions |>> Where) .>> ws

        let scanPageSize        = skipStringCI_ws "pagesize" >>. openParentheses >>. pint32_ws .>> closeParentheses |>> ScanPageSize
        let scanOptions         = sepBy1 scanPageSize comma |>> List.toArray

        let pwith = ws >>. skipStringCI_ws "with" >>. openParentheses >>. scanOptions .>> closeParentheses

        // parser for a scan
        let pscan = tuple5 paction pfrom (opt pwhere) (opt plimit) (opt pwith)
                    |>> (fun (action, from, where, limit, with') -> 
                            { Action = action; From = from; Where = where; Limit = limit; Options = with' })

    [<RequireQualifiedAccess>]
    module ScanV2Parser = 
        // only allow attributes in the Where clause for a SCAN
        let whereAttributes     = attribute

        // parsers for binary/unary/between conditions allowed in a SCAN
        let binaryOperators     = choice [ stringReturn_ws "="  Equal
                                           stringReturn_ws "!=" NotEqual
                                           stringReturn_ws ">=" GreaterThanOrEqual
                                           stringReturn_ws ">"  GreaterThan
                                           stringReturn_ws "<=" LessThanOrEqual
                                           stringReturn_ws "<"  LessThan
                                           stringCIReturn_ws "contains" Contains
                                           stringCIReturn_ws "not contains" NotContains
                                           stringCIReturn_ws "begins with" BeginsWith ]
        let binaryCondition     = pipe3 whereAttributes binaryOperators operant (fun id op v -> id, op v)

        let unaryOperators      = choice [ stringCIReturn_ws "is null" Null; 
                                           stringCIReturn_ws "is not null" NotNull ]
        let unaryCondition      = pipe2 whereAttributes unaryOperators (fun id op -> id, op)

        let between             = stringCIReturn_ws "between" Between
        let and'                = skipStringCI_ws "and"
        let betweenCondition    = pipe5 whereAttributes between operant and' operant (fun id op v1 _ v2 -> id, op(v1, v2))

        let in'                 = stringCIReturn_ws "in" In    
        let operantLst          = sepBy1 operant (ws >>. skipString_ws ",")
        let inCondition         = pipe5 whereAttributes in' openParentheses operantLst closeParentheses (fun id op _ lst _ -> id, op(lst))

        let pcondition          = choice [ attempt unaryCondition;   attempt binaryCondition;
                                           attempt betweenCondition; inCondition ]
        let filterConditions    = sepBy1 (ws >>. pcondition .>> ws) (ws >>. and')

        let pwhere = ws >>. skipStringCI_ws "where" >>. (filterConditions |>> Where) .>> ws

        let scanPageSize        = skipStringCI_ws "pagesize" >>. openParentheses >>. pint32_ws .>> closeParentheses |>> ScanPageSize
        let scanSegments        = skipStringCI_ws "segments" >>. openParentheses >>. pint32_ws .>> closeParentheses |>> ScanSegments
        let noReturnedCapacity  = stringCIReturn_ws "NoReturnedCapacity" ScanNoReturnedCapacity

        let scanOption          = choice [ scanPageSize; scanSegments; noReturnedCapacity ]
        let scanOptions         = sepBy1 scanOption comma |>> List.toArray

        let pwith = ws >>. skipStringCI_ws "with" >>. openParentheses >>. scanOptions .>> closeParentheses

        // parser for a scan
        let pscan = tuple5 paction pfrom (opt pwhere) (opt plimit) (opt pwith)
                    |>> (fun (action, from, where, limit, with') -> 
                            { Action = action; From = from; Where = where; Limit = limit; Options = with' })

    let parseDynamoQueryV1 str = match run QueryV1Parser.pquery str with
                                 | Success(result, _, _) -> result
                                 | Failure(errStr, _, _) -> raise <| InvalidQuery errStr

    let parseDynamoQueryV2 str = match run QueryV2Parser.pquery str with
                                 | Success(result, _, _) -> result
                                 | Failure(errStr, _, _) -> raise <| InvalidQuery errStr

    let parseDynamoScanV1 str = match run ScanV1Parser.pscan str with
                                | Success(result, _, _) -> result
                                | Failure(errStr, _, _) -> raise <| InvalidScan errStr

    let parseDynamoScanV2 str = match run ScanV2Parser.pscan str with
                                | Success(result, _, _) -> result
                                | Failure(errStr, _, _) -> raise <| InvalidScan errStr