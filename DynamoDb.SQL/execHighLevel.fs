// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

namespace DynamoDb.SQL.Execution

open Amazon.DynamoDB.DataModel
open Amazon.DynamoDB.DocumentModel
open DynamoDb.SQL.Parser

module HighLevel =
    let execQuery (cxt : DynamoDBContext) (query : string) =
        let dynamoQuery = parseDynamoQuery query
        cxt.FromQuery(new QueryOperationConfig())


