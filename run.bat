@echo off
REM Ensure working directory is the script location
cd /d %~dp0

REM Create virtual environment if it doesn't exist
if not exist venv (
    python -m venv venv
)

REM Activate virtual environment
call venv\Scripts\activate

REM Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

REM Run the main application, forwarding any arguments
python main.py %*
