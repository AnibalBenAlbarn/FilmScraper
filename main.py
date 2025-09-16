import os
import sys
import time
import argparse
import subprocess
from datetime import datetime

from Scripts import scraper_utils
from Scripts.scraper_utils import (
    setup_database,
    connect_db,
    setup_logger,
    set_db_path,
    set_torrent_db_path,
)

from Scripts.direct_dw_series_scraper import process_all_series

# Importaciones necesarias para que PyInstaller incluya todos los scrapers
import Scripts.direct_dw_films_scraper  # noqa: F401
import Scripts.direct_dw_series_scraper  # noqa: F401
import Scripts.update_movies_premiere  # noqa: F401
import Scripts.update_movies_updated  # noqa: F401
import Scripts.update_episodes_premiere  # noqa: F401
import Scripts.update_episodes_updated  # noqa: F401
import Scripts.torrent_dw_films_scraper  # noqa: F401
import Scripts.torrent_dw_series_scraper  # noqa: F401


logger = setup_logger("main", "main.log")


def clear_screen():
    """Limpia la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')


def pause():
    """Pausa la ejecución hasta que el usuario presione ENTER."""
    input("\nPulsa ENTER para continuar...")


def run_script(module_name, extra_args=None):
    """Ejecuta un módulo scraper en un proceso independiente."""
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
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


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
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


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
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


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
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


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
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


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
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


def run_scrapers_menu():
    while True:
        clear_screen()
        print("============================")
        print("  MENÚ SCRAPERS / UPDATES")
        print("============================")
        print("1) DIRECT (web directa)")
        print("2) TORRENT")
        print("0) Volver al menú principal")
        choice = input('> ').strip()
        if choice == '1':
            direct_menu()
        elif choice == '2':
            torrent_menu()
        elif choice == '0':
            return
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


def select_database_path(current_path, default_name):
    """Solicita al usuario una ruta para la base de datos."""
    print("\n--- Selección de ruta de base de datos ---")
    print(f"Ruta actual: {current_path}")
    new_path = input("Introduce una nueva ruta o pulsa ENTER para mantener la actual: ").strip()

    if not new_path:
        logger.debug(f"Se mantiene la ruta de base de datos: {current_path}")
        return current_path

    new_path = os.path.abspath(new_path)
    if not new_path.endswith(".db"):
        new_path = os.path.join(new_path, default_name)

    logger.debug(f"Ruta de base de datos seleccionada: {new_path}")
    return new_path


def select_direct_db_path():
    return select_database_path(scraper_utils.DB_PATH, "direct_dw_db.db")


def select_torrent_db_path():
    return select_database_path(scraper_utils.TORRENT_DB_PATH, "torrent_dw_db.db")


def setup_database_menu():
    """Menú para la configuración de bases de datos."""
    while True:
        clear_screen()
        print("\n===== CONFIGURACIÓN DE BASES DE DATOS =====")
        print("1. Crear base(s) de datos")
        print("2. Establecer ruta(s) de base de datos")
        print("3. Ejecutar script SQL")
        print("0. Volver al menú principal")

        choice = input("\nSelecciona una opción (0-3): ").strip()

        if choice == '1':
            while True:
                clear_screen()
                print("\n--- Selecciona base de datos a crear ---")
                print("1. Base directa")
                print("2. Base torrent")
                print("3. Ambas bases")
                print("0. Volver")

                db_choice = input("\nSelecciona una opción (0-3): ").strip()

                if db_choice == '1':
                    from Scripts.db_setup import create_direct_db

                    if create_direct_db(scraper_utils.DB_PATH) and setup_database(logger, scraper_utils.DB_PATH):
                        print(f"\nBase de datos creada correctamente en: {scraper_utils.DB_PATH}")
                        try:
                            conn = connect_db(scraper_utils.DB_PATH)
                            conn.close()
                            print("Conexión con la base de datos exitosa.")
                        except Exception as exc:
                            print(f"Error al conectar con la base de datos: {exc}")
                    else:
                        print("\nNo se pudo crear la base de datos.")
                    pause()

                elif db_choice == '2':
                    from Scripts.db_setup import create_torrent_db
                    import sqlite3

                    if create_torrent_db(scraper_utils.TORRENT_DB_PATH):
                        print(f"\nBase torrent creada correctamente en: {scraper_utils.TORRENT_DB_PATH}")
                        try:
                            conn = sqlite3.connect(scraper_utils.TORRENT_DB_PATH)
                            conn.close()
                            print("Conexión con la base torrent exitosa.")
                        except Exception as exc:
                            print(f"Error al conectar con la base torrent: {exc}")
                    else:
                        print("\nNo se pudo crear la base torrent.")
                    pause()

                elif db_choice == '3':
                    from Scripts.db_setup import create_direct_db, create_torrent_db
                    import sqlite3

                    direct_ok = create_direct_db(scraper_utils.DB_PATH) and setup_database(logger, scraper_utils.DB_PATH)
                    torrent_ok = create_torrent_db(scraper_utils.TORRENT_DB_PATH)

                    if direct_ok:
                        print(f"\nBase de datos creada correctamente en: {scraper_utils.DB_PATH}")
                        try:
                            conn = connect_db(scraper_utils.DB_PATH)
                            conn.close()
                            print("Conexión con la base de datos exitosa.")
                        except Exception as exc:
                            print(f"Error al conectar con la base de datos: {exc}")
                    else:
                        print("\nNo se pudo crear la base de datos.")

                    if torrent_ok:
                        print(f"\nBase torrent creada correctamente en: {scraper_utils.TORRENT_DB_PATH}")
                        try:
                            conn = sqlite3.connect(scraper_utils.TORRENT_DB_PATH)
                            conn.close()
                            print("Conexión con la base torrent exitosa.")
                        except Exception as exc:
                            print(f"Error al conectar con la base torrent: {exc}")
                    else:
                        print("\nNo se pudo crear la base torrent.")
                    pause()

                elif db_choice == '0':
                    break

                else:
                    print("\nOpción inválida. Inténtalo de nuevo.")
                    time.sleep(1)

        elif choice == '2':
            while True:
                clear_screen()
                print("\n--- Establecer rutas de bases de datos ---")
                print("1. Base directa")
                print("2. Base torrent")
                print("3. Ambas")
                print("0. Volver")

                path_choice = input("\nSelecciona una opción (0-3): ").strip()

                if path_choice == '1':
                    db_path = select_direct_db_path()
                    if db_path:
                        set_db_path(db_path)
                        print(f"\nRuta de la base directa establecida en: {db_path}")
                    pause()

                elif path_choice == '2':
                    torrent_path = select_torrent_db_path()
                    if torrent_path:
                        set_torrent_db_path(torrent_path)
                        print(f"\nRuta de la base torrent establecida en: {torrent_path}")
                    pause()

                elif path_choice == '3':
                    db_path = select_direct_db_path()
                    if db_path:
                        set_db_path(db_path)
                        print(f"\nRuta de la base directa establecida en: {db_path}")
                    torrent_path = select_torrent_db_path()
                    if torrent_path:
                        set_torrent_db_path(torrent_path)
                        print(f"\nRuta de la base torrent establecida en: {torrent_path}")
                    pause()

                elif path_choice == '0':
                    break

                else:
                    print("\nOpción inválida. Inténtalo de nuevo.")
                    time.sleep(1)

        elif choice == '3':
            clear_screen()
            script_path = input("\nIntroduce la ruta del archivo SQL: ").strip()
            if os.path.exists(script_path):
                db_path = input("\nIntroduce la ruta de la base de datos (ENTER para usar la predeterminada): ").strip()
                if not db_path:
                    db_path = scraper_utils.DB_PATH

                if scraper_utils.execute_sql_script(script_path, db_path, logger):
                    print("\nScript ejecutado correctamente.")
                else:
                    print("\nNo se pudo ejecutar el script.")
            else:
                print("\nNo se encontró el archivo SQL especificado.")
            pause()

        elif choice == '0':
            return

        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


def settings_menu():
    """Menú de ajustes generales."""
    while True:
        clear_screen()
        print("\n===== AJUSTES =====")
        print("1. Cambiar máximo de workers")
        print("2. Cambiar número máximo de reintentos")
        print("3. Activar/Desactivar caché")
        print("0. Volver al menú principal")

        choice = input("\nSelecciona una opción (0-3): ").strip()

        if choice == '1':
            try:
                current = scraper_utils.MAX_WORKERS
                print(f"\nWorkers actuales: {current}")
                new_value = int(input("Introduce el nuevo máximo de workers (2-8): "))
                if 2 <= new_value <= 8:
                    scraper_utils.set_max_workers(new_value)
                    print(f"\nMáximo de workers actualizado a {new_value}.")
                else:
                    print("\nValor inválido. Debe estar entre 2 y 8.")
            except ValueError:
                print("\nEntrada inválida. Introduce un número válido.")
            pause()

        elif choice == '2':
            try:
                current = scraper_utils.MAX_RETRIES
                print(f"\nReintentos actuales: {current}")
                new_value = int(input("Introduce el nuevo máximo de reintentos (1-10): "))
                if 1 <= new_value <= 10:
                    scraper_utils.set_max_retries(new_value)
                    print(f"\nMáximo de reintentos actualizado a {new_value}.")
                else:
                    print("\nValor inválido. Debe estar entre 1 y 10.")
            except ValueError:
                print("\nEntrada inválida. Introduce un número válido.")
            pause()

        elif choice == '3':
            current = "Activada" if scraper_utils.CACHE_ENABLED else "Desactivada"
            print(f"\nLa caché está actualmente: {current}")
            new_value = input("¿Deseas cambiar el estado de la caché? (s/n): ").strip().lower()
            if new_value == 's':
                scraper_utils.toggle_cache()
                new_status = "Activada" if scraper_utils.CACHE_ENABLED else "Desactivada"
                print(f"\nLa caché ahora está: {new_status}")
            pause()

        elif choice == '0':
            return

        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


def main_menu():
    """Menú principal de la aplicación."""
    while True:
        clear_screen()
        print("\n===== HDFULL SCRAPER =====")
        print(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("1. Configuración de bases de datos")
        print("2. Scrapers y actualizaciones")
        print("3. Ajustes de ejecución")
        print("0. Salir")

        choice = input("\nSelecciona una opción (0-3): ").strip()

        if choice == '1':
            setup_database_menu()
        elif choice == '2':
            run_scrapers_menu()
        elif choice == '3':
            settings_menu()
        elif choice == '0':
            print("\nSaliendo del programa. ¡Hasta luego!")
            sys.exit(0)
        else:
            print("\nOpción inválida. Inténtalo de nuevo.")
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='HDFull Scraper')
    parser.add_argument('--series', action='store_true', help='Ejecutar scraper de series directas')
    parser.add_argument('--start-page', type=int, default=1, help='Página inicial para el scraper')
    parser.add_argument('--max-pages', type=int, help='Número máximo de páginas a scrapear')
    parser.add_argument('--reset', action='store_true', help='Reiniciar el progreso almacenado')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos')

    args = parser.parse_args()

    if args.series:
        if args.db_path:
            set_db_path(args.db_path)

        process_all_series(
            start_page=args.start_page,
            max_pages=args.max_pages,
            reset_progress=args.reset
        )
        sys.exit(0)

    main_menu()
