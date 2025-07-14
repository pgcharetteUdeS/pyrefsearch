@echo off

echo Running pyrefsearch.py...

:: Set directory paths
if %COMPUTERNAME% == 3IT-CHAP-W022 GOTO running_locally
GOTO running_remote

:running_locally
set PYTHONDIR="C:\Program Files\Python\Python312"
set WORKINGDIR="C:\Users\%USERNAME%\OneDrive - USherbrooke\Documents on OneDrive\Python\Pycharm\pyrefsearch"
GOTO run_search

:running_remote
set PYTHONDIR="C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python312"
set WORKINGDIR="C:\Users\%USERNAME%\OneDrive - USherbrooke\Documents on OneDrive\Python\Pycharm\pyrefsearch-stable"
GOTO run_search

:run_search
:: Run the Scopus differential search
cd %WORKINGDIR%
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch_diff.toml > pyrefsearch.log 2>&1

:: if the PowerShell script "pyrefsearch_send_email_confirmation.ps1" exists, pyrefsearch.py ran successfully
set EMAIL_POWERSHELL_SCRIPT="shell_scripts\pyrefsearch_send_email_confirmation.ps1"
if exist %EMAIL_POWERSHELL_SCRIPT% GOTO pyrefsearch_success
GOTO pyrefsearch_failed

:: pydersearch.py ran correctly, send confirmation emails, delete the PowerShell script
:pyrefsearch_success
echo Sending email confirmations...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File %EMAIL_POWERSHELL_SCRIPT%
del %EMAIL_POWERSHELL_SCRIPT%

GOTO end

:: pyrefsearch.py failed to run, send email tp Paul.Charette@USherbrooke.ca with logfile
:pyrefsearch_failed
echo pyrefsearch.py failed to run, send error log email...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File shell_scripts\send_email_pyrefsearch_failure_to_run.ps1
GOTO end

:end
pause
