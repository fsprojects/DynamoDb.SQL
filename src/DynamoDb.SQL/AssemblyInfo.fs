namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyProductAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyDescriptionAttribute("A SQL-like external DSL for querying and scanning data in Amazon DynamoDB.")>]
[<assembly: AssemblyVersionAttribute("2.1.1")>]
[<assembly: AssemblyFileVersionAttribute("2.1.1")>]
[<assembly: InternalsVisibleToAttribute("DynamoDb.SQL.Tests")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "2.1.1"
