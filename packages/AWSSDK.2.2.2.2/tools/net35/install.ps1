param($installPath, $toolsPath, $package, $project)


function UpdateCurrentProjectsConfigFile([string]$name)
{
    $config = $project.ProjectItems | where {$_.Name -eq "Web.config"}
    if ($config -eq $null)
    {
        $config = $project.ProjectItems | where {$_.Name -eq "App.config"}
        if ($config -eq $null)
        {
            return
        }
    }
    $localPath = $config.Properties | where {$_.Name -eq "LocalPath"}
    UpdateConfigFile($localPath.Value, $name)
}

function UpdateConfigFile([string]$configFilePath, [string]$name)
{
    $xml = New-Object xml
    $xml.Load($configFilePath)

	$appSettingNode = $xml.SelectSingleNode("configuration/appSettings/add[@key = 'AWSProfileName']")
	If( $appSettingNode -ne $null)
	{
		Write-Host "AWSProfileName already exists in config file"
		return
	}
	
	Write-Host "Adding AWSProfileName appSetting to " $configFilePath
    $appSettingNode = $xml.CreateElement("add")
    $appSettingNode.SetAttribute("key", "AWSProfileName")
    $appSettingNode.SetAttribute("value", $name)

    $appSettingsNode = $xml.SelectSingleNode("configuration/appSettings")
    if ($appSettingsNode -eq $null)
    {
        $appSettingsNode = $xml.CreateElement("appSettings")
        $xml.DocumentElement.AppendChild($appSettingsNode)
    }
    $appSettingsNode.AppendChild($appSettingNode)

    if ($name -eq "")
    {    
        $comment = $xml.CreateComment("AWSProfileName is used to reference an account that has been registered with the SDK.`r`nIf using AWS Toolkit for Visual Studio then this value is the same value shown in the AWS Explorer.`r`nIt is also possible to registered an accounts using the <solution-dir>/packages/AWSSDK-X.X.X.X/tools/account-management.ps1 PowerShell script`r`nthat is bundled with the nuget package under the tools folder.")
        $appSettingsNode.InsertBefore($comment, $appSettingNode)
    }
    
    $xml.Save($configFilePath)
}


UpdateCurrentProjectsConfigFile ""