namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyProductAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyDescriptionAttribute("A SQL-like external DSL for querying and scanning data in Amazon DynamoDB.")>]
[<assembly: AssemblyVersionAttribute("2.0.2")>]
[<assembly: AssemblyFileVersionAttribute("2.0.2")>]
[<assembly: InternalsVisibleToAttribute("DynamoDb.SQL.Tests")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "2.0.2"
