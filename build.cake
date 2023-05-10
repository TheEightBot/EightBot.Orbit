//////////////////////////////////////////////////////////////////////
// ADDINS
//////////////////////////////////////////////////////////////////////
#addin nuget:https://api.nuget.org/v3/index.json?package=Cake.FileHelpers
#addin nuget:https://api.nuget.org/v3/index.json?package=Cake.SemVer

//////////////////////////////////////////////////////////////////////
// TOOLS
//////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument<string>("target", "Default");

var version = Argument<string>("build_version", "0");

var environment = Argument<string>("environment", "");

var eightbotNugetApiKey = Argument<string>("nuget_api_key", "");

var buildType = Argument<string>("build_type", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////
var mainSolution = "EightBot.Orbit.sln";

var eightbotNugetUsername = "eightbot";
var eightbotNugetSourceName = "Eight-Bot_V2";
var eightbotNugetSourceUrl = "https://eightbot.pkgs.visualstudio.com/_packaging/Eight-Bot/nuget/v2";

///////////////////////////////////////////////////////////////////////////////
// SETUP / TEARDOWN
///////////////////////////////////////////////////////////////////////////////
Setup(context =>
{
    Information("Building Orbit");
    Information("Nuget API Key: "+ eightbotNugetApiKey);
});

Teardown(context =>
{
    // Executed AFTER the last task.
});

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////
Task ("Clean")
.Does (() => 
{
	CleanDirectories ("./EightBot.Orbit*/bin");
	CleanDirectories ("./EightBot.Orbit*/obj");
});

Task ("RestorePackages")
.Does (() => 
{
	NuGetRestore(mainSolution);
});

Task ("BuildCore")
.IsDependentOn("Clean")
.IsDependentOn("RestorePackages")
.Does (() => 
{
	if (IsRunningOnWindows())
	{
  		MSBuild(
			mainSolution, 
			new MSBuildSettings{}
				.WithProperty("Version", version)
				.WithProperty("ReleaseVersion", version)
				.WithProperty("PackageVersion", version)
				.SetMaxCpuCount(0)
				.SetVerbosity(Verbosity.Quiet)
				.UseToolVersion(MSBuildToolVersion.VS2019)
				.SetMSBuildPlatform(MSBuildPlatform.x86)
				.SetConfiguration(buildType)
				.SetPlatformTarget(PlatformTarget.MSIL));
	}
	else {
		MSBuild(
			mainSolution, 
			new MSBuildSettings{}
				.WithProperty("Version", version)
				.WithProperty("ReleaseVersion", version)
				.WithProperty("PackageVersion", version)
				.SetMaxCpuCount(0)
				.SetVerbosity(Verbosity.Quiet)
				.SetConfiguration(buildType)
				.SetPlatformTarget(PlatformTarget.MSIL));
	}
});

Task ("BuildNuGet")
.IsDependentOn("BuildCore")
.Does (() => 
{
    var nugetSpecs = GetFiles($"./*.nuspec");

    foreach(var nugetSpec in nugetSpecs) {

        var processArguments = new ProcessArgumentBuilder{};

        processArguments
            .Append("pack")
			.Append(nugetSpec.FullPath)
            .Append("-Version")
            .Append(version + environment);

        using(var process = 
            StartAndReturnProcess(
                "nuget", 
                new ProcessSettings{ 
                    Arguments = processArguments
                }
            )) 
        {
            process.WaitForExit();
        }

    }
});

Task("NuGet")
.IsDependentOn("BuildNuGet")
.Does(() =>
{

    if(NuGetHasSource(source:eightbotNugetSourceUrl)) {
        NuGetRemoveSource(eightbotNugetSourceName, eightbotNugetSourceUrl);
    }

    NuGetAddSource(
        name: eightbotNugetSourceName, 
        source: eightbotNugetSourceUrl,
        settings:  new NuGetSourcesSettings
            {
                UserName = eightbotNugetUsername,
                Password = eightbotNugetApiKey,
                IsSensitiveSource = true,
                Verbosity = NuGetVerbosity.Detailed
            });

    var nugetPackages = GetFiles("./*.nupkg");

    foreach(var package in nugetPackages) {

        var processArguments = new ProcessArgumentBuilder{};

        processArguments
            .Append("push")
            .Append("-Source")
            .Append(eightbotNugetSourceName)
            .Append("-ApiKey")
            .Append(eightbotNugetApiKey)
            .Append(package.FullPath);


        using(var process = 
            StartAndReturnProcess(
                "nuget", 
                new ProcessSettings{ 
                    Arguments = processArguments
                }
            )) 
        {
            process.WaitForExit();
        }

    }
});

Task("Default")
.IsDependentOn("BuildNuGet")
.Does(() =>
{
    Information("Script Complete");
});

RunTarget(target);
