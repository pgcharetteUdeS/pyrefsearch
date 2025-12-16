@echo off

set PYTHONDIR="C:\Program Files\Python\Python312"
copy "C:\Users\%USERNAME%\USherbrooke\3IT-Gestion_Documents - 3IT-Gestion_Documents\M_Bases_donnees\M300_Membres\M320_Chercheurs_Membres\Liste chercheurs-membres.xlsx" data
echo Running pyrefsearch.py...
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch.toml > pyrefsearch.log 2>&1
%PYTHONDIR%\python.exe pyrefsearch\ansi_to_html_converter.py pyrefsearch.log pyrefsearch.html
%PYTHONDIR%\python.exe pyrefsearch\strip_ANSI_codes_from_file.py pyrefsearch.log pyrefsearch.log
pause
