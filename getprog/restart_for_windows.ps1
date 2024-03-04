$scriptName = "scraper.py"
$scriptDir = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$projectPath = $scriptDir  # Set this to the directory of the script
$scriptPath = Join-Path -Path $projectPath -ChildPath $scriptName
$idFile = Join-Path -Path $scriptDir -ChildPath "last_id.txt"  # File to store the last unique ID

try {
    while ($true) {
        $uniqueId = [guid]::NewGuid().ToString()  # Generate a unique identifier
        Write-Host "Running script with unique ID: $uniqueId"

        # Write the unique ID to the file
        Set-Content -Path $idFile -Value $uniqueId

        # Start a new instance of the script with the unique identifier
        Start-Process -FilePath "poetry" -ArgumentList "run python `"$scriptPath`" $uniqueId" -WorkingDirectory $projectPath -NoNewWindow

        # Wait for a specified time before restarting the loop
        Start-Sleep -Seconds 1800  # Adjust the time as needed

        # Find the process ID of the script and terminate it
        Get-Process | Where-Object { $_.CommandLine -like "*$uniqueId*" } | Stop-Process
    }
} finally {
    # Clean up and stop the last instance of the Python script
    Write-Host "Stopping the last instance of the Python script..."
    Get-Process | Where-Object { $_.CommandLine -like "*$uniqueId*" } | Stop-Process
}
