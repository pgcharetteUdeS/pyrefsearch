# Script to send an email from pgcharette@gmail.com to a list of recipients

param(
	[string]$EmailTo,
	[string]$Subject,
	[string]$Body,
	[string]$AttachmentFilename

)

$EmailFrom = "pgcharette@gmail.com"
$SMTPServer = "smtp.gmail.com"
$SMTPMessage = New-Object System.Net.Mail.MailMessage($EmailFrom,$EmailTo,$Subject,$Body)
$attachment = New-Object System.Net.Mail.Attachment($AttachmentFilename)
$SMTPMessage.Attachments.Add($attachment)
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587)
$SMTPClient.EnableSsl = $true
$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("pgcharette@gmail.com", "iqgsfyhwlitpopzb");
$SMTPClient.Send($SMTPMessage)
