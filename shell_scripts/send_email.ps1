# Script to send an email from pgcharette@gmail.com to a list of recipients
# NB: An App password associated with Google account pgcharette@gmail.com is required

param(
	[string]$EmailTo,
	[string]$Subject,
	[string]$Body,
	[string[]]$Attachments
)

$EmailFrom = "pgcharette@gmail.com"
$SMTPServer = "smtp.gmail.com"
$SMTPMessage = New-Object System.Net.Mail.MailMessage($EmailFrom,$EmailTo,$Subject,$Body)
foreach ($Attachment in $Attachments) {
	$attachment = New-Object System.Net.Mail.Attachment($Attachment)
	$SMTPMessage.Attachments.Add($attachment)
}
$SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587)
$SMTPClient.EnableSsl = $true
$SMTPClient.Credentials = New-Object System.Net.NetworkCredential("pgcharette@gmail.com", "jumo ogyg yovx wlvz");
$SMTPClient.Send($SMTPMessage)
