# In Notepad++, use Encoding/ASCII
$currentDirectory = (Get-Location).Path
$Body = "Résultats dans le répertoire '" + $currentDirectory + "\data'"
$EmailTo = "paul.charette@usherbrooke.ca"
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
