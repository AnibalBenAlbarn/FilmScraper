# HDFull Scrapers

This project contains utilities to scrape data from the HDFull website and manage the collected information.

## Windows setup

A convenience batch script is included to create an isolated Python environment, install dependencies and launch the main application.

1. Double-click `run.bat` (or execute it from a command prompt).
2. The script will create a `venv` virtual environment if it does not yet exist.
3. Required packages from `requirements.txt` will be installed.
4. Finally the script runs `python main.py`. Any command-line arguments passed to the batch file are forwarded to the Python program.

Example:

```bat
run.bat --series --start-page 2
```

This executes the series scraper starting at page 2.

## Manual setup (non-Windows)

If you are running the project on another platform, perform the steps manually:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

## Database setup


When running `python main.py` without command-line options, an interactive menu is shown. Under **Database Setup** you can create the direct download and torrent databases individually or both at once. The menu also allows changing the paths of the direct and torrent databases and running custom SQL scripts. A separate **Run Scrapers** option exposes the previous Direct and Torrent scraper menus in a single place.


## Repository structure

- `main.py` – Entry point that displays the application menu or accepts command line arguments.
- `Scripts/` – Helper modules used by the scraper.
- `run.bat` – Windows helper to bootstrap the environment and execute `main.py`.

Logs and the virtual environment are ignored by git.

## Running scrapers in batch

The helper script `Scripts/run_all.py` executes multiple scrapers sequentially. The
`--scraper` option selects between the direct-download update scripts and the
torrent scrapers:

```bash
python Scripts/run_all.py --scraper direct
python Scripts/run_all.py --scraper torrent
```

Arguments like `--db-path`, `--max-pages` and `--max-workers` are only applicable
to the direct scrapers.

## Building a standalone executable

The project can be packaged into a single executable that runs from the console
using [PyInstaller](https://pyinstaller.org/).

1. Install PyInstaller (inside the virtual environment if desired):

   ```bash
   pip install pyinstaller
   ```

2. From the project root, build the application using the provided specification
   file:

   ```bash
   pyinstaller hdfull.spec
   ```

The generated executable will be located in `dist/HdfullScrappers/`. Execute it
from a command prompt to access the same menus available when running
`python main.py`.
