# PowerShell script to send completion email via smtp.gmail.com
# NB: when edting in Notepad++, use Encoding/ASCII

param (
    [string]$EmailTo,
	[string]$AttachmentFilename
)

$EmailFrom = "pgcharette@gmail.com"
$Subject = "Résultats de la recherche dans Scopus pour le mois précédant"
$Body = "Résultats de la recherche dans Scopus pour le mois précédant"
$SMTPServer = "smtp.gmail.com" 
$SMTPMessage = New-Object System.Net.Mail.MailMessage($EmailFrom,$EmailTo,$Subject,$Body)
$attachment = New-Object System.Net.Mail.Attachment($AttachmentFilename)
$SMTPMessage.Attachments.Add($attachment)
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587) 
$SMTPClient.EnableSsl = $true
$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("pgcharette@gmail.com", "iqgsfyhwlitpopzb"); 
$SMTPClient.Send($SMTPMessage)
