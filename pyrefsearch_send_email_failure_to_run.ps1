# Script to send confirmation emails to a list of recipients
# NB: the script is generated automatically by pyrefsearch.py

function send_email
{
	param(
		[string]$EmailTo,
		[string]$AttachmentFilename

	)
	$EmailFrom = "pgcharette@gmail.com"
	$Subject = "pydefsearch failed to run! If running from home, need VPN..."
	$Body = "pydefsearch failed to run! If running from home, need VPN..."
	$SMTPServer = "smtp.gmail.com"
	$SMTPMessage = New-Object System.Net.Mail.MailMessage($EmailFrom,$EmailTo,$Subject,$Body)
	$attachment = New-Object System.Net.Mail.Attachment($AttachmentFilename)
	$SMTPMessage.Attachments.Add($attachment)
	$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587)
	$SMTPClient.EnableSsl = $true
	$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("pgcharette@gmail.com", "iqgsfyhwlitpopzb");
	$SMTPClient.Send($SMTPMessage)
}

$currentDirectory = (Get-Location).Path
$logfilename = $currentDirectory + "\pyrefsearch.log"
send_email -EmailTo "Paul.Charette@USherbrooke.ca" -AttachmentFilename $logfilename
