# Script to send a failure email and logfile to Paul.Charette@USherbrooke.ca
$Subject = "pydefsearch.py failed to run! If running from home, need VPN..."
$currentDirectory = (Get-Location).Path
$logfilename = $currentDirectory + "\pyrefsearch.log"
$attachments = @($logfilename)
& ".\shell_scripts\send_email.ps1" -EmailTo "Paul.Charette@USherbrooke.ca" -Subject $Subject -Body $Subject -Attachments $attachments
