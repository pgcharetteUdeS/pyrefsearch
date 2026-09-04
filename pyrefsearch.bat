@echo off

:: Copy over most recent 3IT membership file
copy "C:\Users\%USERNAME%\USherbrooke\3IT-Gestion_Documents - 3IT-Gestion_Documents\M_Bases_donnees\M300_Membres\M320_Chercheurs_Membres\Liste chercheurs-membres.xlsx" data

:: Set python.exe path
echo Set python.exe path...
if "%COMPUTERNAME%" == "FGEN-007356" GOTO FGEN-007356
if "%COMPUTERNAME%" == "FGEN-004012" GOTO FGEN_004012
set PYTHONDIR="C:\Program Files\Python\Python312"
GOTO run_search

:FGEN-007356
set PYTHONDIR="C:\Users\%USERNAME%\AppData\Local\Python\pythoncore-3.12-64"
GOTO run_search

:FGEN_004012
set PYTHONDIR="C:\Users\%USERNAME%\AppData\Roaming\Python\Python312"
GOTO run_search

:: Run search...
:run_search
echo Running pyrefsearch.py...
%PYTHONDIR%\python.exe pyrefsearch\pyrefsearch.py data\pyrefsearch.toml > pyrefsearch.log 2>&1
%PYTHONDIR%\python.exe pyrefsearch\ansi_to_html_converter.py pyrefsearch.log pyrefsearch.html
%PYTHONDIR%\python.exe pyrefsearch\strip_ANSI_codes_from_file.py pyrefsearch.log pyrefsearch.log
pause
