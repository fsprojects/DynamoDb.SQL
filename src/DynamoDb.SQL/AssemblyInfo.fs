namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: InternalsVisibleToAttribute("DynamoDb.SQL.Tests")>]
[<assembly: AssemblyTitleAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyProductAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyDescriptionAttribute("A SQL-like external DSL for querying and scanning data in Amazon DynamoDB.")>]
[<assembly: AssemblyVersionAttribute("2.1.2")>]
[<assembly: AssemblyFileVersionAttribute("2.1.2")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "2.1.2"
