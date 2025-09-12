import os
import sys
import time
import argparse
import logging
from datetime import datetime

# Import scraper modules from the Scripts package
from Scripts import scraper_utils
from Scripts.scraper_utils import (
    setup_database,
    connect_db,
    PROJECT_ROOT,
    setup_logger,
    set_db_path,
    set_torrent_db_path,
)

# Menu for running the different scrapers
import menu_scrapers

# Direct series scraper is still available via CLI arguments
from Scripts.direct_dw_series_scraper import process_all_series

# Configure logger
logger = setup_logger("main", "main.log")


def clear_screen():
    """Clear the console screen."""
    os.system('cls' if os.name == 'nt' else 'clear')
    

def select_database_path(current_path, default_name):
    """Prompt the user to select or create a database file."""

    print("\n--- Database Path Selection ---")
    print(f"Current database path: {current_path}")
    new_path = input("Enter new database path or press Enter to keep current: ").strip()

    if not new_path:
        logger.debug(f"Using existing database path: {current_path}")
        return current_path

    # Ensure absolute path and .db extension
    new_path = os.path.abspath(new_path)
    if not new_path.endswith(".db"):
        new_path = os.path.join(new_path, default_name)

    logger.debug(f"Selected database path: {new_path}")
    return new_path


def select_direct_db_path():
    return select_database_path(scraper_utils.DB_PATH, "direct_dw_db.db")


def select_torrent_db_path():
    return select_database_path(scraper_utils.TORRENT_DB_PATH, "torrent_dw_db.db")


def setup_database_menu():
    """Menu for setting up the database."""
    clear_screen()
    print("\n===== DATABASE SETUP =====")
    print("1. Create database(s)")

    print("2. Set database path(s)")
    print("3. Run database script")
    print("4. Back to main menu")

    choice = input("\nEnter your choice (1-4): ")

    if choice == '1':
        # Submenu to select which database(s) to create
        clear_screen()
        print("\n--- SELECT DATABASE TO CREATE ---")
        print("1. Direct database")
        print("2. Torrent database")
        print("3. Both databases")
        print("4. Back")

        db_choice = input("\nEnter your choice (1-4): ")

        if db_choice == '1':
            from db_setup import create_direct_db

            if create_direct_db(scraper_utils.DB_PATH) and setup_database(logger, scraper_utils.DB_PATH):
                print(f"\nDatabase created successfully at: {scraper_utils.DB_PATH}")
                # Test connection
                try:
                    conn = connect_db(scraper_utils.DB_PATH)

                    conn.close()
                    print("Database connection test successful!")
                except Exception as e:
                    print(f"Error connecting to database: {e}")
            else:
                print("\nFailed to create database.")

        elif db_choice == '2':

            from db_setup import create_torrent_db
            import sqlite3

            if create_torrent_db(scraper_utils.TORRENT_DB_PATH):
                print(f"\nTorrent database created successfully at: {scraper_utils.TORRENT_DB_PATH}")
                try:
                    conn = sqlite3.connect(scraper_utils.TORRENT_DB_PATH)
                    conn.close()
                    print("Torrent database connection test successful!")
                except Exception as e:
                    print(f"Error connecting to torrent database: {e}")
            else:
                print("\nFailed to create torrent database.")

        elif db_choice == '3':
            from db_setup import create_direct_db, create_torrent_db
            import sqlite3

            direct_ok = create_direct_db(scraper_utils.DB_PATH) and setup_database(logger, scraper_utils.DB_PATH)
            torrent_ok = create_torrent_db(scraper_utils.TORRENT_DB_PATH)

            if direct_ok:
                print(f"\nDatabase created successfully at: {scraper_utils.DB_PATH}")
                try:
                    conn = connect_db(scraper_utils.DB_PATH)
                    conn.close()
                    print("Database connection test successful!")
                except Exception as e:
                    print(f"Error connecting to database: {e}")
            else:
                print("\nFailed to create database.")

            if torrent_ok:

                print(f"\nTorrent database created successfully at: {scraper_utils.TORRENT_DB_PATH}")
                try:
                    conn = sqlite3.connect(scraper_utils.TORRENT_DB_PATH)

                    conn.close()
                    print("Torrent database connection test successful!")
                except Exception as e:
                    print(f"Error connecting to torrent database: {e}")
            else:
                print("\nFailed to create torrent database.")

        elif db_choice == '4':
            return setup_database_menu()
        else:
            print("\nInvalid choice. Please try again.")
            time.sleep(1)
            return setup_database_menu()

        input("\nPress Enter to continue...")
        return setup_database_menu()

    elif choice == '2':
        # Submenu to set database paths
        clear_screen()
        print("\n--- SET DATABASE PATHS ---")
        print("1. Direct database path")
        print("2. Torrent database path")
        print("3. Both")
        print("4. Back")

        path_choice = input("\nEnter your choice (1-4): ")

        if path_choice == '1':
            db_path = select_direct_db_path()
            if db_path:
                set_db_path(db_path)
                print(f"\nDatabase path set to: {db_path}")
        elif path_choice == '2':
            torrent_path = select_torrent_db_path()
            if torrent_path:
                set_torrent_db_path(torrent_path)
                print(f"\nTorrent database path set to: {torrent_path}")
        elif path_choice == '3':
            db_path = select_direct_db_path()
            if db_path:
                set_db_path(db_path)
                print(f"\nDatabase path set to: {db_path}")
            torrent_path = select_torrent_db_path()
            if torrent_path:
                set_torrent_db_path(torrent_path)
                print(f"\nTorrent database path set to: {torrent_path}")
        else:
            pass

        input("\nPress Enter to continue...")
        return setup_database_menu()

    elif choice == '3':
        # Run database script
        script_path = input("\nEnter the path to the SQL script file: ")
        if os.path.exists(script_path):
            from scraper_utils import execute_sql_script
            db_path = input("\nEnter the path to the database (leave empty for default): ")
            if not db_path:
                db_path = scraper_utils.DB_PATH

            if execute_sql_script(script_path, db_path, logger):
                print("\nScript executed successfully!")
            else:
                print("\nFailed to execute script.")
        else:
            print("\nScript file not found.")

        input("\nPress Enter to continue...")
        return setup_database_menu()

    elif choice == '4':
        return

    else:
        print("\nInvalid choice. Please try again.")
        time.sleep(1)
        return setup_database_menu()


def settings_menu():
    """Menu for settings."""
    clear_screen()
    print("\n===== SETTINGS =====")
    print("1. Change maximum workers")
    print("2. Change retry settings")
    print("3. Toggle cache")
    print("4. Back to main menu")

    choice = input("\nEnter your choice (1-4): ")

    if choice == '1':
        try:
            from scraper_utils import MAX_WORKERS, set_max_workers
            current = MAX_WORKERS
            print(f"\nCurrent maximum workers: {current}")
            new_value = int(input("Enter new maximum workers (2-8): "))
            if 2 <= new_value <= 8:
                set_max_workers(new_value)
                print(f"\nMaximum workers changed to {new_value}")
            else:
                print("\nInvalid value. Must be between 2 and 8.")
        except ValueError:
            print("\nInvalid input. Please enter a number.")

    elif choice == '2':
        try:
            from scraper_utils import MAX_RETRIES, set_max_retries
            current = MAX_RETRIES
            print(f"\nCurrent maximum retries: {current}")
            new_value = int(input("Enter new maximum retries (1-10): "))
            if 1 <= new_value <= 10:
                set_max_retries(new_value)
                print(f"\nMaximum retries changed to {new_value}")
            else:
                print("\nInvalid value. Must be between 1 and 10.")
        except ValueError:
            print("\nInvalid input. Please enter a number.")

    elif choice == '3':
        from scraper_utils import CACHE_ENABLED, toggle_cache
        current = "Enabled" if CACHE_ENABLED else "Disabled"
        print(f"\nCache is currently: {current}")
        new_value = input("Toggle cache (y/n): ")
        if new_value.lower() == 'y':
            toggle_cache()
            new_status = "Enabled" if CACHE_ENABLED else "Disabled"
            print(f"\nCache is now: {new_status}")

    elif choice == '4':
        return

    else:
        print("\nInvalid choice. Please try again.")
        time.sleep(1)

    input("\nPress Enter to continue...")
    return settings_menu()


def main_menu():
    """Main menu of the application."""
    while True:
        clear_screen()
        print("\n===== HDFULL SCRAPER =====")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("1. Database Setup")
        print("2. Run Scrapers")
        print("3. Settings")
        print("4. Exit")

        choice = input("\nEnter your choice (1-4): ")

        if choice == '1':
            setup_database_menu()
        elif choice == '2':
            menu_scrapers.main()
        elif choice == '3':
            settings_menu()
        elif choice == '4':
            print("\nExiting program. Goodbye!")
            sys.exit(0)
        else:
            print("\nInvalid choice. Please try again.")
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

