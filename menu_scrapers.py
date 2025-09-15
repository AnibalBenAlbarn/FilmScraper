import os
import sys
import subprocess

# Import script modules so PyInstaller includes them
import Scripts.direct_dw_films_scraper  # noqa: F401
import Scripts.direct_dw_series_scraper  # noqa: F401
import Scripts.update_movies_premiere  # noqa: F401
import Scripts.update_movies_updated  # noqa: F401
import Scripts.update_episodes_premiere  # noqa: F401
import Scripts.update_episodes_updated  # noqa: F401
import Scripts.torrent_dw_films_scraper  # noqa: F401
import Scripts.torrent_dw_series_scraper  # noqa: F401

# Base directory for scripts (unused when packaged, kept for compatibility)
SCRIPT_DIR = os.path.join(os.path.dirname(__file__), "Scripts")


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def pause():
    input("\nPulsa ENTER para continuar...")


def run_script(module_name, extra_args=None):
    """Run a scraper module in a separate process.

    Using ``-m`` ensures compatibility when the project is packaged with
    PyInstaller, as the modules are loaded from the bundled archive.
    """

    cmd = [sys.executable, "-m", f"Scripts.{module_name}"]
    if extra_args:
        cmd.extend(extra_args)
    print(f"\n▶ Ejecutando: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        print("[OK] Finalizado.")
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] El script terminó con un error: {exc}")
    pause()


def direct_movies_menu():
    while True:
        clear_screen()
        print("Películas (DIRECT)")
        print("1) Ejecutar normal")
        print("2) Empezar desde página específica")
        print("0) Volver")
        choice = input('> ').strip()
        if choice == '1':
            run_script('direct_dw_films_scraper')
        elif choice == '2':
            start_page = input('\n¿Desde qué página quieres empezar?\n> ').strip()
            run_script('direct_dw_films_scraper', ['--start-page', start_page])
        elif choice == '0':
            return


def direct_series_menu():
    while True:
        clear_screen()
        print("Series (DIRECT)")
        print("1) Ejecutar normal")
        print("2) Empezar desde página específica")
        print("0) Volver")
        choice = input('> ').strip()
        if choice == '1':
            run_script('direct_dw_series_scraper')
        elif choice == '2':
            start_page = input('\n¿Desde qué página quieres empezar?\n> ').strip()
            run_script('direct_dw_series_scraper', ['--start-page', start_page])
        elif choice == '0':
            return


def direct_menu():
    while True:
        clear_screen()
        print("==== DIRECT ====")
        print("1) Películas — scraper")
        print("2) Series — scraper")
        print("3) Actualizar películas (estrenos)")
        print("4) Actualizar películas (actualizadas)")
        print("5) Actualizar series (estrenos)")
        print("6) Actualizar series (actualizadas)")
        print("0) Volver")
        choice = input('> ').strip()
        if choice == '1':
            direct_movies_menu()
        elif choice == '2':
            direct_series_menu()
        elif choice == '3':
            run_script('update_movies_premiere')
        elif choice == '4':
            run_script('update_movies_updated')
        elif choice == '5':
            run_script('update_episodes_premiere')
        elif choice == '6':
            run_script('update_episodes_updated')
        elif choice == '0':
            return


def torrent_movies_menu():
    while True:
        clear_screen()
        print("Películas (TORRENT)")
        print("1) Reanudar/Actualizar desde último progreso (recomendado)")
        print("2) Empezar desde página específica")
        print("0) Volver")
        choice = input('> ').strip()
        if choice == '1':
            run_script('torrent_dw_films_scraper', ['--resume'])
        elif choice == '2':
            start_page = input('\n¿Desde qué página quieres empezar?\n> ').strip()
            run_script('torrent_dw_films_scraper', ['--start-page', start_page])
        elif choice == '0':
            return


def torrent_series_menu():
    while True:
        clear_screen()
        print("Series (TORRENT)")
        print("1) Reanudar/Actualizar desde último progreso (recomendado)")
        print("2) Empezar desde página específica")
        print("0) Volver")
        choice = input('> ').strip()
        if choice == '1':
            run_script('torrent_dw_series_scraper', ['--resume'])
        elif choice == '2':
            start_page = input('\n¿Desde qué página quieres empezar?\n> ').strip()
            run_script('torrent_dw_series_scraper', ['--start-page', start_page])
        elif choice == '0':
            return


def torrent_menu():
    while True:
        clear_screen()
        print("==== TORRENT ====")
        print("1) Películas — scraper")
        print("2) Series — scraper")
        print("0) Volver")
        choice = input('> ').strip()
        if choice == '1':
            torrent_movies_menu()
        elif choice == '2':
            torrent_series_menu()
        elif choice == '0':
            return


def main():
    while True:
        clear_screen()
        print("============================")
        print("  MENÚ SCRAPERS / UPDATES")
        print("============================")
        print("1) DIRECT (web directa)")
        print("2) TORRENT")
        print("0) Salir")
        choice = input('> ').strip()
        if choice == '1':
            direct_menu()
        elif choice == '2':
            torrent_menu()
        elif choice == '0':
            print("\n¡Hasta luego!")
            break


if __name__ == "__main__":
    main()
