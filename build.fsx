// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#r @"packages/FAKE/tools/FakeLib.dll"
open Fake 
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper
open System

let buildDir = "bin/"
let testDir  = "test/"
let tempDir  = "temp/"

// --------------------------------------------------------------------------------------
// START TODO: Provide project-specific details below
// --------------------------------------------------------------------------------------

// Information about the project are used
//  - for version and project name in generated AssemblyInfo file
//  - by the generated NuGet package 
//  - to run tests and to publish documentation on GitHub gh-pages
//  - for documentation, you also need to edit info in "docs/tools/generate.fsx"

// The name of the project 
// (used by attributes in AssemblyInfo, name of a NuGet package and directory in 'src')
let project = "DynamoDb.SQL"

// Short summary of the project
// (used as description in AssemblyInfo and as a short summary for NuGet package)
let summary = "A SQL-like external DSL for querying and scanning data in Amazon DynamoDB."

// Longer description of the project
// (used as a description for NuGet package; line breaks are automatically cleaned up)
let projectFile  = "src/DynamoDb.SQL/DynamoDb.SQL.fsproj"
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

// List of author names (for NuGet package)
let authors = [ "Yan Cui" ]
// Tags for your project (for NuGet package)
let tags = "aws amazon cloud dynamodb parser fsharp f# c# csharp combinator sql dsl"

// File system information 
// Pattern specifying assemblies to be tested using NUnit
let testProject    = "tests/DynamoDb.SQL.Tests/DynamoDb.SQL.Tests.fsproj"
let testAssemblies = sprintf "%s/*Tests*.dll" testDir

// Git configuration (used for publishing documentation in gh-pages branch)
// The profile where the project is posted 
let gitHome = "https://github.com/theburningmonk"
let gitName = "DynamoDb.SQL"

// --------------------------------------------------------------------------------------
// END TODO: The rest of the file includes standard build steps 
// --------------------------------------------------------------------------------------

// Read additional information from the release notes document
let release = LoadReleaseNotes "RELEASE_NOTES.md"

// Helper active pattern for project types
let (|Fsproj|Csproj|Vbproj|) (projFileName:string) = 
    match projFileName with
    | f when f.EndsWith("fsproj") -> Fsproj
    | f when f.EndsWith("csproj") -> Csproj
    | f when f.EndsWith("vbproj") -> Vbproj
    | _                           -> failwith (sprintf "Project file %s not supported. Unknown project type." projFileName)

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let getAssemblyInfoAttributes projectName =
        [ Attribute.Title (projectName)
          Attribute.Product project
          Attribute.Description summary
          Attribute.Version release.AssemblyVersion
          Attribute.FileVersion release.AssemblyVersion ]
        |> fun attributes ->
            match projectName with
            | "DynamoDb.SQL" -> 
                Attribute.InternalsVisibleTo "DynamoDb.SQL.Tests"::attributes
            | _ -> attributes

    let getProjectDetails projectPath =
        let projectName = System.IO.Path.GetFileNameWithoutExtension(projectPath)
        ( projectPath, 
          projectName,
          System.IO.Path.GetDirectoryName(projectPath),
          (getAssemblyInfoAttributes projectName)
        )

    !! "src/**/*.??proj"
    |> Seq.map getProjectDetails
    |> Seq.iter (fun (projFileName, projectName, folderName, attributes) ->
        match projFileName with
        | Fsproj -> CreateFSharpAssemblyInfo (folderName @@ "AssemblyInfo.fs") attributes
        | Csproj -> CreateCSharpAssemblyInfo ((folderName @@ "Properties") @@ "AssemblyInfo.cs") attributes
        | Vbproj -> CreateVisualBasicAssemblyInfo ((folderName @@ "My Project") @@ "AssemblyInfo.vb") attributes
        )
)

// --------------------------------------------------------------------------------------
// Clean build results

Target "Clean" (fun _ ->
    CleanDirs ["bin"; "temp"]
)

Target "CleanDocs" (fun _ ->
    CleanDirs ["docs/output"]
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target "Build" (fun _ ->
    !! projectFile
    |> MSBuildRelease "" "Rebuild"
    |> ignore
)

Target "BuildTests" (fun _ ->
    !! testProject
    |> MSBuildRelease testDir "Rebuild"
    |> ignore
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "RunTests" (fun _ ->
    !! testAssemblies
    |> NUnit (fun p ->
        { p with
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            OutputFile = "TestResults.xml" })
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target "NuGet" (fun _ ->
    Paket.Pack(fun p -> 
        { p with
            OutputPath = buildDir
            Version = release.NugetVersion
            ReleaseNotes = toLines release.Notes})
)

Target "PublishNuget" (fun _ ->
    Paket.Push(fun p -> 
        { p with
            WorkingDir = buildDir })
)

// --------------------------------------------------------------------------------------
// Generate the documentation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

// --------------------------------------------------------------------------------------
// Release Scripts

Target "ReleaseDocs" (fun _ ->
    let ghPages      = "gh-pages"
    let ghPagesLocal = "temp/gh-pages"
    Repository.clone "temp" (gitHome + "/" + gitName + ".git") ghPages
    Branches.checkoutBranch ghPagesLocal ghPages
    fullclean ghPagesLocal
    CopyRecursive "docs/output" ghPagesLocal true |> printfn "%A"
    CommandHelper.runSimpleGitCommand ghPagesLocal "add ." |> printfn "%s"
    let cmd = sprintf """commit -a -m "Update generated documentation for version %s""" release.NugetVersion
    CommandHelper.runSimpleGitCommand ghPagesLocal cmd |> printfn "%s"
    Branches.push ghPagesLocal
)

Target "Release" DoNothing

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "All" DoNothing

"Clean"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "BuildTests"
  ==> "RunTests"
  ==> "All"

"All" 
//  ==> "CleanDocs"
//  ==> "GenerateDocs"
//  ==> "ReleaseDocs"
  ==> "NuGet"

"NuGet"
  ==> "PublishNuget"
  ==> "Release"

RunTargetOrDefault "All"