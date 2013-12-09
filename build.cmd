@echo off
if not exist packages\FAKE.2.2.22.0\tools\Fake.exe ( 
  .nuget\nuget.exe install FAKE -OutputDirectory packages -ExcludeVersion -Prerelease
)
packages\FAKE.2.2.22.0\tools\FAKE.exe build.fsx All
pause
