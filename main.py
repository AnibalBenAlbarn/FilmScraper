import os
import sys
import time
import argparse
import logging
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime

# Import scraper modules
from scraper_utils import setup_database, connect_db, PROJECT_ROOT, setup_logger

# Configure logger
logger = setup_logger("main", "main.log")


def clear_screen():
    """Clear the console screen."""
    os.system('cls' if os.name == 'nt' else 'clear')


def select_database_path():
    """Open a file dialog to select an existing database or create a new one."""
    root = tk.Tk()
    root.withdraw()  # Hide the main window

    # Ask user if they want to create a new database or select an existing one
    choice = messagebox.askquestion("Database Selection",
                                    "Do you want to create a new database?\\n\\n"
                                    "Select 'Yes' to create a new database.\\n"
                                    "Select 'No' to choose an existing database.")

    if choice == 'yes':
        # Create a new database
        file_path = filedialog.asksaveasfilename(
            title="Create New Database",
            filetypes=[("SQLite Database", "*.db")],
            defaultextension=".db",
            initialdir=PROJECT_ROOT
        )
    else:
        # Select an existing database
        file_path = filedialog.askopenfilename(
            title="Select Existing Database",
            filetypes=[("SQLite Database", "*.db")],
            initialdir=PROJECT_ROOT
        )

    root.destroy()

    if not file_path:
        return None

    return file_path


def setup_database_menu():
    """Menu for setting up the database."""
    clear_screen()
    print("\\n===== DATABASE SETUP =====")
    print("1. Setup database (create or select)")
    print("2. Run database script")
    print("3. Back to main menu")

    choice = input("\\nEnter your choice (1-3): ")

    if choice == '1':
        db_path = select_database_path()
        if db_path:
            # Update the global DB_PATH in scraper_utils
            from scraper_utils import DB_PATH, set_db_path
            set_db_path(db_path)

            # Setup the database
            if setup_database(logger, db_path):
                print(f"\\nDatabase setup successfully at: {db_path}")
                # Test connection
                try:
                    conn = connect_db(db_path)
                    conn.close()
                    print("Database connection test successful!")
                except Exception as e:
                    print(f"Error connecting to database: {e}")
            else:
                print("\\nFailed to setup database.")
        else:
            print("\\nDatabase setup cancelled.")

        input("\\nPress Enter to continue...")
        return setup_database_menu()

    elif choice == '2':
        # Run database script
        script_path = input("\\nEnter the path to the SQL script file: ")
        if os.path.exists(script_path):
            from scraper_utils import DB_PATH, execute_sql_script
            db_path = input("\\nEnter the path to the database (leave empty for default): ")
            if not db_path:
                db_path = DB_PATH

            if execute_sql_script(script_path, db_path, logger):
                print("\\nScript executed successfully!")
            else:
                print("\\nFailed to execute script.")
        else:
            print("\\nScript file not found.")

        input("\\nPress Enter to continue...")
        return setup_database_menu()

    elif choice == '3':
        return

    else:
        print("\\nInvalid choice. Please try again.")
        time.sleep(1)
        return setup_database_menu()


def series_scraper_menu():
    """Menu for series scraper options."""
    clear_screen()
    print("\\n===== SERIES SCRAPER =====")
    print("1. Start scraping from page 1")
    print("2. Start scraping from specific page")
    print("3. Scrape specific number of pages")
    print("4. Reset progress and start from page 1")
    print("5. Back to main menu")

    choice = input("\\nEnter your choice (1-5): ")

    if choice == '1':
        # Start from page 1
        process_all_series(start_page=1)
        input("\\nScraping completed. Press Enter to continue...")
        return series_scraper_menu()

    elif choice == '2':
        # Start from specific page
        try:
            start_page = int(input("\\nEnter starting page number: "))
            process_all_series(start_page=start_page)
            input("\\nScraping completed. Press Enter to continue...")
        except ValueError:
            print("\\nInvalid page number. Please enter a number.")
            time.sleep(1)

        return series_scraper_menu()

    elif choice == '3':
        # Scrape specific number of pages
        try:
            start_page = int(input("\\nEnter starting page number: "))
            max_pages = int(input("Enter maximum number of pages to scrape: "))
            process_all_series(start_page=start_page, max_pages=max_pages)
            input("\\nScraping completed. Press Enter to continue...")
        except ValueError:
            print("\\nInvalid input. Please enter numbers.")
            time.sleep(1)

        return series_scraper_menu()

    elif choice == '4':
        # Reset progress and start from page 1
        confirm = input("\\nThis will reset all progress. Are you sure? (y/n): ")
        if confirm.lower() == 'y':
            process_all_series(start_page=1, reset_progress=True)
            input("\\nScraping completed. Press Enter to continue...")

        return series_scraper_menu()

    elif choice == '5':
        return

    else:
        print("\\nInvalid choice. Please try again.")
        time.sleep(1)
        return series_scraper_menu()


def movie_scraper_menu():
    """Menu for movie scraper options."""
    clear_screen()
    print("\\n===== MOVIE SCRAPER =====")
    print("1. Start movie scraping from page 1")
    print("2. Start movie scraping from specific page")
    print("3. Scrape specific number of movie pages")
    print("4. Reset movie progress and start from page 1")
    print("5. Back to main menu")

    choice = input("\\nEnter your choice (1-5): ")

    if choice == '1':
        print("\\nMovie scraping not implemented yet.")
    elif choice == '2':
        print("\\nMovie scraping not implemented yet.")
    elif choice == '3':
        print("\\nMovie scraping not implemented yet.")
    elif choice == '4':
        print("\\nMovie scraping not implemented yet.")
    elif choice == '5':
        return
    else:
        print("\\nInvalid choice. Please try again.")
        time.sleep(1)

    input("\\nPress Enter to continue...")
    return movie_scraper_menu()


def settings_menu():
    """Menu for settings."""
    clear_screen()
    print("\\n===== SETTINGS =====")
    print("1. Change maximum workers")
    print("2. Change retry settings")
    print("3. Toggle cache")
    print("4. Back to main menu")

    choice = input("\\nEnter your choice (1-4): ")

    if choice == '1':
        try:
            from scraper_utils import MAX_WORKERS, set_max_workers
            current = MAX_WORKERS
            print(f"\\nCurrent maximum workers: {current}")
            new_value = int(input("Enter new maximum workers (2-8): "))
            if 2 <= new_value <= 8:
                set_max_workers(new_value)
                print(f"\\nMaximum workers changed to {new_value}")
            else:
                print("\\nInvalid value. Must be between 2 and 8.")
        except ValueError:
            print("\\nInvalid input. Please enter a number.")

    elif choice == '2':
        try:
            from scraper_utils import MAX_RETRIES, set_max_retries
            current = MAX_RETRIES
            print(f"\\nCurrent maximum retries: {current}")
            new_value = int(input("Enter new maximum retries (1-10): "))
            if 1 <= new_value <= 10:
                set_max_retries(new_value)
                print(f"\\nMaximum retries changed to {new_value}")
            else:
                print("\\nInvalid value. Must be between 1 and 10.")
        except ValueError:
            print("\\nInvalid input. Please enter a number.")

    elif choice == '3':
        from scraper_utils import CACHE_ENABLED, toggle_cache
        current = "Enabled" if CACHE_ENABLED else "Disabled"
        print(f"\\nCache is currently: {current}")
        new_value = input("Toggle cache (y/n): ")
        if new_value.lower() == 'y':
            toggle_cache()
            new_status = "Enabled" if CACHE_ENABLED else "Disabled"
            print(f"\\nCache is now: {new_status}")

    elif choice == '4':
        return

    else:
        print("\\nInvalid choice. Please try again.")
        time.sleep(1)

    input("\\nPress Enter to continue...")
    return settings_menu()


def main_menu():
    """Main menu of the application."""
    while True:
        clear_screen()
        print("\\n===== HDFULL SCRAPER =====")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\\n1. Database Setup")
        print("2. Series Scraper")
        print("3. Movie Scraper")
        print("4. Settings")
        print("5. Exit")

        choice = input("\\nEnter your choice (1-5): ")

        if choice == '1':
            setup_database_menu()
        elif choice == '2':
            series_scraper_menu()
        elif choice == '3':
            movie_scraper_menu()
        elif choice == '4':
            settings_menu()
        elif choice == '5':
            print("\\nExiting program. Goodbye!")
            sys.exit(0)
        else:
            print("\\nInvalid choice. Please try again.")
            time.sleep(1)


if __name__ == "__main__":
    # Check if running with command line arguments
    parser = argparse.ArgumentParser(description='HDFull Scraper')
    parser.add_argument('--series', action='store_true', help='Run series scraper')
    parser.add_argument('--start-page', type=int, default=1, help='Starting page for scraping')
    parser.add_argument('--max-pages', type=int, help='Maximum number of pages to scrape')
    parser.add_argument('--reset', action='store_true', help='Reset progress')
    parser.add_argument('--db-path', type=str, help='Path to database file')

    args = parser.parse_args()

    if args.series:
        # Run series scraper with command line arguments
        if args.db_path:
            from scraper_utils import set_db_path

            set_db_path(args.db_path)

        process_all_series(
            start_page=args.start_page,
            max_pages=args.max_pages,
            reset_progress=args.reset
        )
        sys.exit(0)

    # If no command line arguments, show the interactive menu
    main_menu()