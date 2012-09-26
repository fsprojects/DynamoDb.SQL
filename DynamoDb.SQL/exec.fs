// Copyright (c) Yan Cui 2012

// Email : theburningmonk@gmail.com
// Blog  : http://theburningmonk.com

module DynamoDb.SQL.Execution

open DynamoDb.SQL.Ast
open DynamoDb.SQL.Parser
open Amazon.DynamoDB
open Amazon.DynamoDB.Model
open Amazon.DynamoDB.DataModel

//let loadAsync<'a> (cxt : DynamoDBContext) = Async.FromBeginEnd(cxt.BeginLoad<'a>, cxt.EndLoad)

//let execQuery (client : AmazonDynamoDBClient) (query : string) =
//    let parserRes = parse query
//
//    parserRes
