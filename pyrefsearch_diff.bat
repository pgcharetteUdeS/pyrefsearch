@echo off

echo Running pyrefsearch...
set PYTHONDIR="C:\Program Files\Python\Python312"
set WORKINGDIR="C:\Users\chap1202\OneDrive - USherbrooke\Documents on OneDrive\Python\Pycharm\pyrefsearch"
cd %WORKINGDIR%
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch_diff.toml > pyrefsearch.log 2>&1

echo Sending email confirmation...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File pyrefsearch_send_email_confirmation.ps1
