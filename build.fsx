// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#r "packages/FAKE.2.2.22.0/tools/FakeLib.dll"

open System
open System.IO
open Fake 
open Fake.AssemblyInfoFile
open Fake.Git

let buildDir  = "build/"
let testDir   = "test/"

// --------------------------------------------------------------------------------------
// Information about the project to be used at NuGet and in AssemblyInfo files
// --------------------------------------------------------------------------------------

let project = "DynamoDb.SQL"
let authors = ["Yan Cui"]
let summary = "A SQL-like external DSL for querying and scanning data in Amazon DynamoDB."
let description = """
  A SQL-like external DSL for querying and scanning data in Amazon DynamoDB.

  Although there exists a number of different ways to query and scan a DynamoDB table with the 
  .Net AWS SDK:

    - with the low-level AmazonDynamoDBClient class
    - with the Table helper class
    - with the DynamoDBContext class

  none of these ways of querying and scanning tables are easy to use, and an external DSL is 
  desperately needed to make it easier to express the query one would like to perform against 
  data stored in DynamoDB.

  It is because of these limitations that I decided to add a SQL-like external DSL on top of 
  existing functionalities of the AWS SDK to make it easier for .Net developers to work with 
  DynamoDB, which is a great product despite the lack of built-in support for a good 
  query language."""
let tags = "aws amazon cloud dynamodb parser fsharp f# c# csharp combinator sql dsl"

let gitHome = "https://github.com/theburningmonk"
let gitName = "DynamoDb.SQL"

// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = 
    File.ReadLines "RELEASE_NOTES.md" 
    |> ReleaseNotesHelper.parseReleaseNotes

let version = release.AssemblyVersion
let releaseNotes = release.Notes |> String.concat "\n"

// --------------------------------------------------------------------------------------
// Generate assembly info files with the right version & up-to-date information

Target "AssemblyInfo" (fun _ ->
    CreateFSharpAssemblyInfo "src/DynamoDb.SQL/AssemblyInfo.fs"
           [ Attribute.Title        project
             Attribute.Product      project
             Attribute.Description  summary
             Attribute.Version      version
             Attribute.FileVersion  version
             Attribute.InternalsVisibleTo "DynamoDb.SQL.Tests" ]
)

// --------------------------------------------------------------------------------------
// Clean build results

Target "Clean" (fun _ ->
    CleanDirs [ buildDir; testDir ]
)

Target "CleanDocs" (fun _ ->
    CleanDirs [ "docs/output" ]
)

// --------------------------------------------------------------------------------------
// Build Visual Studio solutions

let files includes = 
  { BaseDirectory = __SOURCE_DIRECTORY__
    Includes = includes
    Excludes = [] } 

Target "Build" (fun _ ->
    files [ "src/DynamoDb.SQL/DynamoDb.SQL.fsproj" ]
    |> MSBuildRelease buildDir "Rebuild"
    |> ignore
)

Target "BuildTests" (fun _ ->
    files [ "tests/DynamoDb.SQL.Tests/DynamoDb.SQL.Tests.fsproj" ]
    |> MSBuildDebug testDir "Rebuild"
    |> ignore
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "RunTests" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = sprintf "packages/NUnit.Runners.%s/Tools" nunitVersion
    ActivateFinalTarget "CloseTestRunner"

    files [ sprintf "%sDynamoDb.SQL.Tests*.dll" testDir ]
    |> NUnit (fun p ->
        { p with
            ToolPath = nunitPath
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            OutputFile = "TestResults.xml" })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target "NuGet" (fun _ ->
    // Format the description to fit on a single line (remove \r\n and double-spaces)
    let description = description.Replace("\r", "").Replace("\n", "").Replace("  ", " ")
    let nugetPath = ".nuget/nuget.exe"
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = project
            Summary = summary
            Description = description
            Version = version
            ReleaseNotes = releaseNotes
            Tags = tags
            OutputPath = "nuget"
            ToolPath = nugetPath
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey"
            DependenciesByFramework =
                [ { FrameworkVersion  = "net40"
                    Dependencies = 
                        [ "AWSSDK",  GetPackageVersion "packages" "AWSSDK"
                          "FParsec", GetPackageVersion "packages" "FParsec" ] }
                ] })
        "nuget/DynamoDb.SQL.nuspec"
)

Target "Release" DoNothing

"NuGet" ==> "Release"

// --------------------------------------------------------------------------------------
// Help

Target "Help" (fun _ ->
    printfn ""
    printfn "  Please specify the target by calling 'build <Target>'"
    printfn ""
    printfn "  Targets for building:"
    printfn "  * Build"
    printfn "  * BuildTests"
    printfn "  * RunTests"
    printfn "  * All (calls previous 3)"
    printfn ""
    printfn "  Targets for releasing:"
    printfn "  * NuGet (creates package only, doesn't publish)"
    printfn "  * Release (calls previous 1)"
    printfn "")

Target "All" DoNothing

"Clean" ==> "AssemblyInfo" ==> "Build"
"Build" ==> "All"
"BuildTests" ==> "All"
"RunTests" ==> "All"

RunTargetOrDefault "Help"