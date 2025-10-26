param(
    [Parameter(Mandatory=$true)]
    [string]$databaseName,
    [string]$serverName = "PGBLDVM-LDPP01",
    [System.Management.Automation.PSCredential]$Credential
)

# Simple version - generates script for each table individually
[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO') | Out-Null

# Configuration
$outputDirectory = "E:\script"
$outputPath = Join-Path $outputDirectory ("{0}.sql" -f $databaseName)

# Create server object
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)

# Use SQL Authentication if credential provided; otherwise use Windows Authentication
if ($Credential) {
    $server.ConnectionContext.LoginSecure = $false
    $server.ConnectionContext.Login = $Credential.UserName
    $server.ConnectionContext.SecurePassword = $Credential.Password
} else {
    $server.ConnectionContext.LoginSecure = $true
}

# Get database
$db = $server.Databases[$databaseName]

if ($null -eq $db) {
    Write-Error "Database not found: $databaseName"
    exit
}

Write-Host "Database: $($db.Name)"
Write-Host "Tables: $($db.Tables.Count)"

# Initialize scripter
$scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter($server)
$options = New-Object Microsoft.SqlServer.Management.Smo.ScriptingOptions

# Set options
$options.ScriptDrops = $false
$options.IncludeIfNotExists = $false
$options.Indexes = $true
$options.DriAllConstraints = $true
$options.DriPrimaryKey = $true
$options.DriUniqueKeys = $true
$options.DriForeignKeys = $true
$options.Triggers = $false
$options.FileName = $outputPath

$scripter.Options = $options

# Ensure output directory exists
if (-not (Test-Path $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

# Delete old file if exists
if (Test-Path $outputPath) {
    Remove-Item $outputPath
}

# Script each table individually
$tableCount = 0
foreach ($table in $db.Tables) {
    if (-not $table.IsSystemObject) {
        Write-Host "Scripting: $($table.Schema).$($table.Name)"
        
        # Script the table
        $scripter.Options.AppendToFile = ($tableCount -gt 0)
        $scripter.Script(@($table))
        
        $tableCount++
    }
}

# Remove unsupported trigger enable/disable statements for converter compatibility
if (Test-Path $outputPath) {
    $lines = Get-Content -Path $outputPath -ErrorAction SilentlyContinue
    if ($lines) {
        $lines = $lines | Where-Object { $_ -notmatch '^\s*ALTER\s+TABLE\s+.*\s+(ENABLE|DISABLE)\s+TRIGGER\s+.*$' }
        Set-Content -Path $outputPath -Value $lines
    }
}

Write-Host "`nCompleted! Scripted $tableCount tables to: $outputPath"
