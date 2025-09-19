@echo off

set PYTHONDIR="C:\Program Files\Python\Python312"
copy "C:\Users\%USERNAME%\USherbrooke\3IT - Gestion Centrale - Documents\General\Membres\Liste chercheurs-membres.xlsx" data
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch.toml
pause
