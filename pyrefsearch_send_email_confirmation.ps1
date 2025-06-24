# PowerShell script to send completion email via smtp.gmail.com
# NB: when edting in Notepad++, use Encoding/ASCII

# Destination email address
$EmailTo = "paul.charette@usherbrooke.ca"

# Code to send email
$currentDirectory = (Get-Location).Path
$Body = "Résultats dans le répertoire '" + $currentDirectory + "\data'"
$EmailFrom = "pgcharette@gmail.com"
$Subject = "pyrefsearch terminé!" 
$filenameAndPath = "pyrefsearch.log"
$SMTPServer = "smtp.gmail.com" 
$SMTPMessage = New-Object System.Net.Mail.MailMessage($EmailFrom,$EmailTo,$Subject,$Body)
$attachment = New-Object System.Net.Mail.Attachment($filenameAndPath)
$SMTPMessage.Attachments.Add($attachment)
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587) 
$SMTPClient.EnableSsl = $true
$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("pgcharette@gmail.com", "iqgsfyhwlitpopzb"); 
$SMTPClient.Send($SMTPMessage)
