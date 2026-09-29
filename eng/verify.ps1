[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$projects = @("src/MirrorPulse.Adapter.Sdk/MirrorPulse.Adapter.Sdk.csproj", "src/MirrorPulse.Adapter.Local.Worker/MirrorPulse.Adapter.Local.Worker.csproj")
foreach ($project in $projects) {
    & dotnet restore $project --locked-mode
    if ($LASTEXITCODE -ne 0) { throw "Restore failed for $project." }
    & dotnet build $project --configuration Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw "Build failed for $project." }
}
