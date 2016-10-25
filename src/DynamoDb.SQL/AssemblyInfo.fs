namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: InternalsVisibleToAttribute("DynamoDb.SQL.Tests")>]
[<assembly: AssemblyTitleAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyProductAttribute("DynamoDb.SQL")>]
[<assembly: AssemblyDescriptionAttribute("A SQL-like external DSL for querying and scanning data in Amazon DynamoDB.")>]
[<assembly: AssemblyVersionAttribute("3.1.0")>]
[<assembly: AssemblyFileVersionAttribute("3.1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "3.1.0"
