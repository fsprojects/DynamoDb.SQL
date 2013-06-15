# DynamoDb.SQL

This library gives you the capability to execute [**query**](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Query.html) and [**scan**](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/API_Scan.html) operations against [**Amazon DynamoDB**](http://aws.amazon.com/dynamodb/) using a **SQL**-like syntax by extending the existing functionalities of `AmazonDynamoDBClient` and `DynamoDBContext` classes found in the standard .Net AWS SDK.

### Why?

Although there exists a number of different ways to query and scan a DynamoDB table with the .Net AWS SDK:
* with the low-level `AmazonDynamoDBClient` class (see [here](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/LowLevelDotNetQuerying.html) and [here](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/LowLevelDotNetScanning.html))
* with the `Table` helper class (see [here](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/QueryMidLevelDotNet.html) and [here](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/ScanMidLevelDotNet.html))
* with the `DynamoDBContext` class (see [here](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/QueryScanORMModelExample.html))

none of these ways of querying and scanning tables are easy to use, and the few attempts to use them in our codebase left a bad taste in my mouth and an external DSL is desperately needed to make it easier to express the query we'd like to perform against data stored in DynamoDB.

It is because of these limitations that I decided to add a SQL-like external DSL on top of existing functionalities to make it easier for .Net developers to work with DynamoDB, which is a great product despite the lack of built-in support for a good query language.

### Why F#?

Because it's a beautiful language! And it's super easy to build DSLs (internal or external) with F#, especially when you have awesome frameworks such as [**FParsec**](http://www.quanttec.com/fparsec/) to help you with.

A copy of the Simplified BSD license that FParsec is distributed under is included in the repository, see [here](https://github.com/theburningmonk/DynamoDb.SQL/blob/master/FParsec.LICENSE) for details.

### How about us C# guys?

The extension methods defined in this library are written in a way that makes them usable from your C# code too! In practice, you just need to reference the `DynamoDb.SQL.dll` dll in your project and open the `DynamoDb.SQL.Execution` namespace and you'll have access to the extension methods on the `AmazonDynamoDBClient` and `DynamoDBContext` classes.

For more details on using this library, please refer to the [**Wiki**](https://github.com/theburningmonk/DynamoDb.SQL/wiki).

### Local Secondary Index support

Support for LSI is available from version 1.2.1. For more details, please read [this post](http://theburningmonk.com/2013/05/dynamodb-sql-1-2-1-now-supports-local-secondary-index/).

### Nuget

Download and install **DynamoDb.SQL** using [NuGet](https://nuget.org/packages/DynamoDb.SQL).

<a href="https://nuget.org/packages/DynamoDb.SQL"><img src="http://theburningmonk.com/images/dynamodb.sql-nuget-install.png" alt="NuGet package"/></a>

You can view the release notes [here](https://github.com/theburningmonk/DynamoDb.SQL/wiki/Release-Notes).

### Keep in touch

Please follow the official twitter account [@DynamoDbSQL](https://twitter.com/DynamoDbSQL) for updates, and feel free to send me any feedbacks or questions you have about the project :-)
