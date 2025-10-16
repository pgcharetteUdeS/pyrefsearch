@echo off

echo Running pyrefsearch.py...

:: Set working directory
set WORKINGDIR="C:\Users\%USERNAME%\OneDrive - USherbrooke\Documents on OneDrive\Python\Pycharm\pyrefsearch-stable"
cd %WORKINGDIR%

:: Fetch the most recent 3IT membership file
copy "C:\Users\%USERNAME%\USherbrooke\3IT - Gestion Centrale - Documents\General\Membres\Liste chercheurs-membres.xlsx" data

:: Set python.exe path (Paul's 3IT-CHAP-W022 laptop, versus others)
if %COMPUTERNAME% == 3IT-CHAP-W022 GOTO running_3IT_CHAP_W022
set PYTHONDIR="C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python312"
GOTO run_search
:running_3IT_CHAP_W022
set PYTHONDIR="C:\Program Files\Python\Python312"
GOTO run_search

:: Run the Scopus differential search
:run_search
set EMAIL_POWERSHELL_SCRIPT="shell_scripts\pyrefsearch_send_email_confirmation.ps1"
if exist  %EMAIL_POWERSHELL_SCRIPT% del /F  %EMAIL_POWERSHELL_SCRIPT%
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch_diff.toml > pyrefsearch.log 2>&1

:: if the PowerShell script %EMAIL_POWERSHELL_SCRIPT% exists, pyrefsearch.py ran successfully
if exist %EMAIL_POWERSHELL_SCRIPT% GOTO pyrefsearch_success
GOTO pyrefsearch_failed

:: pyrefsearch.py ran correctly, send confirmation emails, delete the PowerShell script
:pyrefsearch_success
echo Sending email confirmations...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File %EMAIL_POWERSHELL_SCRIPT%
del %EMAIL_POWERSHELL_SCRIPT%

GOTO end

:: pyrefsearch.py failed to run, send email to Paul.Charette@USherbrooke.ca with logfile
:pyrefsearch_failed
echo pyrefsearch.py failed to run, send error log email...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File shell_scripts\send_email_pyrefsearch_failure_to_run.ps1
GOTO end

:end
