@echo off

:: This task is run by the Windows Task Scheduler on CharetteXPS04.3it.usherbrooke.ca (ordi au bout de la rangée de Guillaume)
:: every 1srt of the month

:: Run the Scopus search
echo Running pyrefsearch.py...
set PYTHONDIR="C:\Users\chap1202\AppData\Local\Programs\Python\Python312"
set WORKINGDIR="C:\Users\chap1202\OneDrive - USherbrooke\Documents on OneDrive\Python\Pycharm\pyrefsearch"
cd %WORKINGDIR%
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch_diff.toml > pyrefsearch.log 2>&1

:: if the PowerShell script "pyrefsearch_send_email_confirmation.ps1" exists, pyrefsearch.py ran successfully
set EMAIL_POWERSHELL_SCRIPT="pyrefsearch_send_email_confirmation.ps1"
if exist %EMAIL_POWERSHELL_SCRIPT% GOTO pyrefsearch_success
GOTO pyrefsearch_failed

:: pydersearch.py ran correctly, send confirmation emails, delete the PowerShell script
:pyrefsearch_success
echo Sending email confirmations...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File %EMAIL_POWERSHELL_SCRIPT%
del %EMAIL_POWERSHELL_SCRIPT%
GOTO end

:: pyrefsearch.py failed to run, send email to Paul.Charette@USherbrooke.ca with logfile
:pyrefsearch_failed
echo pyrefsearch.py failed to run, send error log email...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File send_email_pyrefsearch_failure_to_run.ps1
GOTO end

:end
