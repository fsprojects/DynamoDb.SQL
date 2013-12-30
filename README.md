# DynamoDb.SQL ([@DynamoDbSQL](https://twitter.com/DynamoDbSQL))

This library gives you the capability to execute [**query**](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html) and [**scan**](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Scan.html) operations against [**Amazon DynamoDB**](http://aws.amazon.com/dynamodb/) using a **SQL**-like syntax by extending the existing functionalities of `AmazonDynamoDBClient` and `DynamoDBContext` classes found in the standard .Net AWS SDK.

This guide contains the following sections:
- [Basics](#basics) - how to get started using this library
- [Features](#features) - what you can do with this library
- [Examples](#examples) - links to examples in F# and C#

You can view the release notes [here](https://github.com/theburningmonk/DynamoDb.SQL/wiki/Release-Notes), and check out the [Wiki](https://github.com/theburningmonk/DynamoDb.SQL/wiki).





## Basics

#### Getting Started

Download and install **DynamoDb.SQL** using [NuGet](https://nuget.org/packages/DynamoDb.SQL).

<a href="https://nuget.org/packages/DynamoDb.SQL"><img src="http://theburningmonk.com/images/dynamodb.sql-nuget-install.png" alt="NuGet package"/></a>



Now suppose we have a DynamoDB tabled called `GameScores` like the following:

![!table](http://dynamodb.sql.s3.amazonaws.com/dynamodb-sql-table.png)

We can find all the scores for the player with `UserId` "theburningmonk-1" we can simply execute the query like the following:

```fsharp

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel

open DynamoDbV2.SQL.Execution 	// you need to open this namespace to get the ExecQuery extension method

let awsKey      = "MY-AWS-KEY"
let awsSecret   = "MY-AWS-SECRET"
let region      = RegionEndpoint.USEast1

let client = new AmazonDynamoDBClient(awsKey, awsSecret, region)
let ctx    = new DynamoDBContext(client)

let query  = "SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\""
let scores = ctx.ExecQuery<GameScore>(selectQuery)
```

whilst the above example is in F#, the same extension methods are accessible from C# too, check out the full range of examples in the [Examples](#examples) section.

For a **detailed run-down of the syntax** please refer to this [**page**](https://github.com/theburningmonk/DynamoDb.SQL/wiki/Getting-Started-V2#syntax).


## Features


#### Simple SQL-like syntax

This library lets you use a SQL-like syntax for performing *query* and *scan* operations against *DynamoDB*. If you're new to *DynamoDB* and is not clear on the difference between the two, please refer to the *DynamoDB* documentation [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html) and guidelines [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html).

Using the appropriate extension methods on `AmazonDynamoDBClient` and `DynamoDBContext` you will be able to query/scan your *DynamoDB* table like this:

```fsharp
let selectQuery = "SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\""
let response    = client.Query(selectQuery)
```

Whilst the syntax for both *query* and *scan* operations are similar, there are minor differences and some comparisons (such as `CONTAINS` and `IN (...)`) are only supported in *scan* operations by *DynamoDB*.
 
For a **detailed run-down of the syntax** please refer to this [**page**](https://github.com/theburningmonk/DynamoDb.SQL/wiki/Getting-Started-V2#syntax).

#### Count-only queries

If you only want to find out the number of items that matches some conditions and not the items themselves, then you can save yourself some bandwidth and read capacity units by using a **COUNT** query:

```fsharp
let countQuery    = "COUNT * FROM GameScores WHERE UserId = \"theburningmonk-1\""
let countResponse = client.Query(countQuery)
```
> **Note**: count queries are **only supported** when you're working with the `AmazonDynamoDBCient`.


#### Selecting specific attributes

If you only want to retrieve certain attributes from your query, then you can save yourself some bandwidth and potentially processing power by specifying exactly which attributes you want to retrieve:

```fsharp
let selectQuery = "SELECT UserId, GameTitle, Wins FROM GameScores WHERE UserId = \"theburningmonk-1\""
let response    = client.Query(selectQuery)
```


#### Ordering and Limiting

Often you will want to retrieve only the top or bottom X number of items based on the natural sorting order of the range key values. Therefore it often makes sense to combine the **ORDER** and **LIMIT** clauses in your query.

For example, in our `GameScores` table (see above), to find the top 3 scoring games for a given user we can write:

```fsharp
let selectQuery = "SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" ORDER DESC LIMIT 3"
let response    = client.Query(selectQuery)
```

> **Note**: in *DynamoDB*, you can only order on the range key values hence why there's no option for you to specify what column to order on in the query syntax.  


#### Throttling

As stated in the DynamoDB [best practices guide](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BestPractices.html), you should [avoid sudden bursts of read activity](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html#QueryAndScanGuidelines.BurstsOfActivity).

To throttle your *query* or *scan* operation, you can use the **PageSize** option in your query to throttle the amount of read capacity units that your query consumes in one go:

```fsharp
let selectQuery = "SELECT * FROM GameScores WHERE UserId = \"theburningmonk-1\" WITH (PageSize(10))"
let response    = client.Query(selectQuery)
```
this query will fetch 10 results at a time, if there are more than 10 results available then additional requests will be made behind the scene until all available results have been retrieved. 

> **Note**: using the *PageSize* option means your *query* or *scan* will take longer to complete and require more individual requests to *DynamoDB*.


#### Parallel scans

If your table is sufficiently large (*DynamoDB* documentations suggests 20GB or larger), [it's recommended](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html#QueryAndScanGuidelines.ParallelScan) that you take advantage of the parallel scans support in *DynamoDB* to speed up the scan operations.

To use parallel scans, you can use the **Segments** option in your *scan* query:

```fsharp
let selectQuery = "SELECT * FROM GameScores WHERE GameTitle = \"Starship X\" WITH (Segments(10))"
let response    = client.Scan(selectQuery)
```
this query will make ten parallel scan requests against *DynamoDB* and the operation will complete when all ten 'segments' have completed and returned all their results.

> **Note**: using parallel scan will consume large amounts of read capacity units in a short burst, so you should plan ahead and up the throughput of your table accordingly before starting the parallel scan!


#### Local Secondary Index support

> *AWS* [announced](http://aws.typepad.com/aws/2013/04/local-secondary-indexes-for-amazon-dynamodb.html) support for *Local Secondary Indexes* on April 18, 2013, for more details please refer to the *DynamoDB* documentations page [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html) and [guidelines](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GuidelinesForLSI.html) for using *Local Secondary Indexes*.

Support for local secondary index is available since version 1.2.1 using the **INDEX** option inside the **WITH** clause.

For example, suppose the aforementioned `GameScores` table has a local secondary index called **TopScoreIndex**:

![table-indexes](http://dynamodb.sql.s3.amazonaws.com/dynamodb-sql-indexes.png)

We can query the table using this index and optionally specify whether to retrieve all attributes or just the attributes projected into the index (any attributes that are not on the index will be retrieved from the table using extra read capacity):

```fsharp
let selectQuery = "SELECT * FROM GameScores 
                   WHERE UserId = \"theburningmonk-1\" 
                   AND TopScore >= 1000 
                   WITH(Index(TopScoreIndex, true))"
let response = client.Query(selectQuery)
```

For more details, please read [this post](http://theburningmonk.com/2013/05/dynamodb-sql-1-2-1-now-supports-local-secondary-index/).


#### Global Secondary Indexes support

> *AWS* [announced](http://aws.typepad.com/aws/2013/12/now-available-global-secondary-indexes-for-amazon-dynamodb.html) support for *Global Secondary Indexes* on December 12, 2013, for more details please refer to the *DynamoDB* documentations page [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html) and [guidelines](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GuidelinesForGSI.html) for using *Global Secondary Indexes*.

*Global Secondary Indexes*, or *GSI* is supported through the same **INDEX** option as local secondary index above, the index name specified in the *INDEX* option can be any local or global index on the table.

For example, to query against the global secondary index **GameTitleIndex** on our `GameScores` table (see above):  

```fsharp
let selectQuery = "SELECT * FROM GameScores 
                   WHERE GameTitle = \"Starship X\" 
                   AND TopScore >= 1000
                   WITH(Index(GameTitleIndex, false), NoConsistentRead)"
let response = client.Query(selectQuery)
```

> **Important**: although the queries look identical, compared to local secondary indexes there are a couple of key differences you need to be aware of when querying against global secondary indexes:
> - you **must add the NoConsistentRead option** in your query as global secondary indexes only support eventually consistent reads, if you try to do a consistent read against a global secondary index it will result in an error from *DynamoDB*;
> - when you created the global secondary index, if you didn't choose **All Attributes** as the **Projected Attributes** for the index, then you must set the "all attributes" flag in the `Index` option to `false` (i.e. Index(IndexName, **false**))

## Examples

#### Query

<table>
	<tbody>
		<tr>
			<td>Get all rows for a hash key</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L68-L87>F#</a></td>
		</tr>
		<tr>
			<td>Query with range key</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L93-L118>F#</a></td>
		</tr>
		<tr>
			<td>Query with ORDER and LIMIT</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L122-L159>F#</a></td>
		</tr>
		<tr>
			<td>Disable consistent read</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L163-L175>F#</a></td>
		</tr>
		<tr>
			<td>Throttling</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L180-L193>F#</a></td>
		</tr>
		<tr>
			<td>Query with Local Secondary Index (all attributes)</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L237-L285>F#</a></td>
		</tr>
		<tr>
			<td>Query with Local Secondary Index (projected attributes)</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L292-L327>F#</a></td>
		</tr>
		<tr>
			<td>Query with Global Secondary Index (projected attributes)</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L335-L384>F#</a></td>
		</tr>
	</tbody>
</table>

#### Scan


<table>
	<tbody>
		<tr>
			<td>Basic scan</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L414-L433>F#</a></td>
		</tr>
		<tr>
			<td>Scan with LIMIT</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L437-L456>F#</a></td>
		</tr>
		<tr>
			<td>Throttling</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L459-L474>F#</a></td>
		</tr>
		<tr>
			<td>Parallel scans</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L477-L492>F#</a></td>
		</tr>
		<tr>
			<td>Disable returning consumed capacity</td>
			<td>C#</td>
			<td><a href=https://github.com/theburningmonk/DynamoDb.SQL/blob/develop/examples/DynamoDb.SQL.ExampleFs/Examples.fsx#L497-L507>F#</a></td>
		</tr>
	</tbody>
</table>