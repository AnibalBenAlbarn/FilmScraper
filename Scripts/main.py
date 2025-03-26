import os
import sys
import sqlite3
import subprocess
import shutil
import re
import logging
from pathlib import Path

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/main.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Rutas predeterminadas para las bases de datos
DEFAULT_DIRECT_DB_PATH = os.path.join(os.getcwd(), "direct_dw_db.db")
DEFAULT_TORRENT_DB_PATH = os.path.join(os.getcwd(), "torrent_dw_db.db")

# Rutas actuales (se actualizarán cuando el usuario las cambie)
direct_db_path = DEFAULT_DIRECT_DB_PATH
torrent_db_path = DEFAULT_TORRENT_DB_PATH

# Scripts disponibles
SCRIPTS = {
    "1": {
        "name": "Direct Download Films Scraper",
        "file": "direct_dw_films_scraper.py",
        "description": "Extrae información de películas para descarga directa"
    },
    "2": {
        "name": "Direct Download Series Scraper",
        "file": "direct_dw_series_scraper.py",
        "description": "Extrae información de series para descarga directa"
    },
    "3": {
        "name": "Torrent Download Films Scraper",
        "file": "torrent_dw_films_scraper.py",
        "description": "Extrae información de películas para descarga por torrent"
    },
    "4": {
        "name": "Torrent Download Series Scraper",
        "file": "torrent_dw_series_scraper.py",
        "description": "Extrae información de series para descarga por torrent"
    }
}


def clear_screen():
    """Limpia la pantalla de la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')


def print_header():
    """Imprime el encabezado del programa."""
    clear_screen()
    print("=" * 80)
    print("                      SISTEMA DE GESTIÓN DE SCRAPERS")
    print("=" * 80)
    print()


def print_menu():
    """Imprime el menú principal."""
    print_header()
    print("MENÚ PRINCIPAL:")
    print("-" * 80)
    print("1. Instalar Bases de Datos")
    print("2. Ejecutar Script")
    print("3. Ver Configuración Actual")
    print("4. Salir")
    print("-" * 80)
    return input("Seleccione una opción (1-4): ")


def print_scripts_menu():
    """Imprime el menú de scripts disponibles."""
    print_header()
    print("SCRIPTS DISPONIBLES:")
    print("-" * 80)
    for key, script in SCRIPTS.items():
        print(f"{key}. {script['name']}")
        print(f"   - {script['description']}")
        print()
    print("5. Volver al menú principal")
    print("-" * 80)
    return input("Seleccione un script para ejecutar (1-5): ")


def create_database(db_path, schema):
    """Crea una base de datos SQLite con el esquema proporcionado."""
    try:
        # Asegurarse de que el directorio existe
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        # Crear la base de datos y ejecutar el esquema
        conn = sqlite3.connect(db_path)
        conn.executescript(schema)
        conn.close()
        logger.info(f"Base de datos creada correctamente en: {db_path}")
        return True
    except Exception as e:
        logger.error(f"Error al crear la base de datos: {e}")
        return False


def install_databases():
    """Gestiona la instalación de las bases de datos."""
    global direct_db_path, torrent_db_path

    print_header()
    print("INSTALACIÓN DE BASES DE DATOS")
    print("-" * 80)
    print("Esta opción creará las bases de datos SQLite necesarias para los scrapers.")
    print("Puede especificar las rutas donde desea instalar las bases de datos.")
    print()

    # Esquema para direct_dw_db.db
    direct_schema = """
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS "links_files_download" (
        "id" INTEGER,
        "movie_id" INTEGER,
        "server_id" INTEGER,
        "language" TEXT,
        "link" TEXT,
        "quality_id" INTEGER,
        "episode_id" INTEGER,
        "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY("id" AUTOINCREMENT),
        FOREIGN KEY("episode_id") REFERENCES "series_episodes"("id"),
        FOREIGN KEY("movie_id") REFERENCES "media_downloads"("id") ON DELETE CASCADE,
        FOREIGN KEY("quality_id") REFERENCES "qualities"("quality_id"),
        FOREIGN KEY("server_id") REFERENCES "servers"("id")
    );
    CREATE TABLE IF NOT EXISTS "media_downloads" (
        "id" INTEGER,
        "title" TEXT,
        "year" INTEGER,
        "imdb_rating" REAL,
        "genre" TEXT,
        "type" TEXT CHECK("type" IN ('movie', 'serie')),
        "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
        "updated_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY("id" AUTOINCREMENT)
    );
    CREATE TABLE IF NOT EXISTS "qualities" (
        "quality_id" INTEGER,
        "quality" TEXT,
        PRIMARY KEY("quality_id" AUTOINCREMENT)
    );
    CREATE TABLE IF NOT EXISTS "series_episodes" (
        "id" INTEGER,
        "season_id" INTEGER,
        "episode" INTEGER,
        "title" TEXT,
        PRIMARY KEY("id" AUTOINCREMENT),
        FOREIGN KEY("season_id") REFERENCES "series_seasons"("id")
    );
    CREATE TABLE IF NOT EXISTS "series_seasons" (
        "id" INTEGER,
        "movie_id" INTEGER,
        "season" INTEGER,
        PRIMARY KEY("id" AUTOINCREMENT),
        FOREIGN KEY("movie_id") REFERENCES "media_downloads"("id")
    );
    CREATE TABLE IF NOT EXISTS "servers" (
        "id" INTEGER,
        "name" TEXT UNIQUE,
        PRIMARY KEY("id" AUTOINCREMENT)
    );
    CREATE TABLE IF NOT EXISTS "update_stats" (
        "update_date" DATE,
        "duration_minutes" REAL,
        "updated_movies" INTEGER,
        "new_links" INTEGER,
        "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY("update_date")
    );
    COMMIT;
    """

    # Esquema para torrent_dw_db.db
    torrent_schema = """
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS "qualities" (
        "id" INTEGER,
        "quality" TEXT NOT NULL UNIQUE,
        PRIMARY KEY("id" AUTOINCREMENT)
    );
    CREATE TABLE IF NOT EXISTS "series_episodes" (
        "id" INTEGER,
        "season_id" INTEGER NOT NULL,
        "episode_number" INTEGER NOT NULL,
        "title" TEXT NOT NULL,
        PRIMARY KEY("id" AUTOINCREMENT),
        FOREIGN KEY("season_id") REFERENCES "series_seasons"("id") ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS "series_seasons" (
        "id" INTEGER,
        "series_id" INTEGER NOT NULL,
        "season_number" INTEGER NOT NULL,
        PRIMARY KEY("id" AUTOINCREMENT),
        FOREIGN KEY("series_id") REFERENCES "torrent_downloads"("id") ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS "torrent_downloads" (
        "id" INTEGER,
        "title" TEXT NOT NULL,
        "year" INTEGER NOT NULL,
        "genre" TEXT,
        "director" TEXT,
        "type" TEXT NOT NULL CHECK("type" IN ('movie', 'series')),
        "added_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY("id" AUTOINCREMENT)
    );
    CREATE TABLE IF NOT EXISTS "torrent_files" (
        "id" INTEGER,
        "torrent_id" INTEGER,
        "episode_id" INTEGER,
        "quality_id" INTEGER NOT NULL,
        "torrent_link" TEXT NOT NULL,
        PRIMARY KEY("id" AUTOINCREMENT),
        FOREIGN KEY("episode_id") REFERENCES "series_episodes"("id") ON DELETE CASCADE,
        FOREIGN KEY("quality_id") REFERENCES "qualities"("id") ON DELETE CASCADE,
        FOREIGN KEY("torrent_id") REFERENCES "torrent_downloads"("id") ON DELETE CASCADE
    );
    COMMIT;
    """

    # Solicitar ruta para direct_dw_db.db
    print(f"Ruta actual para direct_dw_db.db: {direct_db_path}")
    new_direct_path = input("Introduzca la nueva ruta (deje en blanco para mantener la actual): ")
    if new_direct_path:
        direct_db_path = os.path.abspath(new_direct_path)
        if not direct_db_path.endswith("direct_dw_db.db"):
            direct_db_path = os.path.join(direct_db_path, "direct_dw_db.db")

    # Solicitar ruta para torrent_dw_db.db
    print(f"Ruta actual para torrent_dw_db.db: {torrent_db_path}")
    new_torrent_path = input("Introduzca la nueva ruta (deje en blanco para mantener la actual): ")
    if new_torrent_path:
        torrent_db_path = os.path.abspath(new_torrent_path)
        if not torrent_db_path.endswith("torrent_dw_db.db"):
            torrent_db_path = os.path.join(torrent_db_path, "torrent_dw_db.db")

    # Crear las bases de datos
    print("\\nCreando bases de datos...")
    direct_success = create_database(direct_db_path, direct_schema)
    torrent_success = create_database(torrent_db_path, torrent_schema)

    if direct_success and torrent_success:
        print("\\n¡Bases de datos creadas correctamente!")
        # Actualizar las rutas en los scripts
        update_db_paths_in_scripts()
    else:
        print("\\nHubo errores al crear las bases de datos. Revise el archivo de log para más detalles.")

    input("\\nPresione Enter para continuar...")


def update_db_paths_in_scripts():
    """Actualiza las rutas de las bases de datos en los scripts."""
    try:
        # Actualizar scripts de direct download
        for script_key in ["1", "2"]:
            script_file = SCRIPTS[script_key]["file"]
            if os.path.exists(script_file):
                update_direct_db_path_in_script(script_file)

        # Actualizar scripts de torrent download
        for script_key in ["3", "4"]:
            script_file = SCRIPTS[script_key]["file"]
            if os.path.exists(script_file):
                update_torrent_db_path_in_script(script_file)

        logger.info("Rutas de bases de datos actualizadas en todos los scripts")
    except Exception as e:
        logger.error(f"Error al actualizar rutas en scripts: {e}")


def update_direct_db_path_in_script(script_file):
    """Actualiza la ruta de la base de datos direct_dw_db.db en un script."""
    try:
        # Normalizar la ruta para usar barras normales y raw string
        normalized_path = direct_db_path.replace('\\', '/')
        raw_db_path = f"r'{normalized_path}'"

        with open(script_file, 'r', encoding='utf-8') as file:
            content = file.read()

        # Reemplazar la conexión MySQL por SQLite
        new_content = re.sub(
            r"def connect_db\(\).*?return connection",
            f"""def connect_db():
    try:
        connection = sqlite3.connect({raw_db_path})
        connection.row_factory = sqlite3.Row
        logger.debug("Conexión a la base de datos establecida correctamente")
        return connection
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {{e}}")
        raise""",
            content,
            flags=re.DOTALL
        )

        # Resto del código igual...
        # Asegurarse de que se importa sqlite3
        if "import sqlite3" not in new_content:
            new_content = new_content.replace("import pymysql", "import sqlite3")
        else:
            new_content = new_content.replace("import pymysql", "")

        with open(script_file, 'w', encoding='utf-8') as file:
            file.write(new_content)

        logger.info(f"Ruta de base de datos actualizada en {script_file}")
    except Exception as e:
        logger.error(f"Error al actualizar ruta en {script_file}: {e}")


def update_torrent_db_path_in_script(script_file):
    """Actualiza la ruta de la base de datos torrent_dw_db.db en un script."""
    try:
        # Normalizar la ruta para usar barras normales y raw string
        normalized_path = torrent_db_path.replace('\\', '/')
        raw_db_path = f"r'{normalized_path}'"

        with open(script_file, 'r', encoding='utf-8') as file:
            content = file.read()

        # Reemplazar la ruta de la base de datos
        new_content = re.sub(
            r"db_path\s*=\s*r'[^']*'",
            f"db_path = {raw_db_path}",
            content
        )

        with open(script_file, 'w', encoding='utf-8') as file:
            file.write(new_content)

        logger.info(f"Ruta de base de datos actualizada en {script_file}")
    except Exception as e:
        logger.error(f"Error al actualizar ruta en {script_file}: {e}")

def show_config():
    """Muestra la configuración actual."""
    print_header()
    print("CONFIGURACIÓN ACTUAL")
    print("-" * 80)
    print(f"Ruta de la base de datos direct_dw_db.db: {direct_db_path}")
    print(f"Ruta de la base de datos torrent_dw_db.db: {torrent_db_path}")
    print("-" * 80)

    # Verificar si las bases de datos existen
    direct_exists = os.path.exists(direct_db_path)
    torrent_exists = os.path.exists(torrent_db_path)

    print(f"Estado de direct_dw_db.db: {'Instalada' if direct_exists else 'No instalada'}")
    print(f"Estado de torrent_dw_db.db: {'Instalada' if torrent_exists else 'No instalada'}")
    print("-" * 80)

    # Verificar si los scripts existen
    print("Estado de los scripts:")
    for key, script in SCRIPTS.items():
        script_exists = os.path.exists(script["file"])
        print(f"{script['name']}: {'Disponible' if script_exists else 'No disponible'}")

    input("\\nPresione Enter para continuar...")


def run_script(script_key):
    """Ejecuta el script seleccionado en una nueva consola con logging visible."""
    if script_key not in SCRIPTS:
        print("Opción no válida.")
        return

    script = SCRIPTS[script_key]
    script_file = script["file"]

    if not os.path.exists(script_file):
        print(f"Error: El archivo {script_file} no existe.")
        input("\nPresione Enter para continuar...")
        return

    print_header()
    print(f"Ejecutando: {script['name']}")
    print("-" * 80)
    print(f"Descripción: {script['description']}")
    print(f"Archivo: {script_file}")
    print("-" * 80)
    print("Iniciando ejecución en nueva ventana...")
    print(f"Python ejecutable: {sys.executable}")  # Debe mostrar la ruta correcta
    print(f"Script path: {os.path.abspath(script_file)}")  # Debe mostrar la ruta completa

    try:
        if os.name == 'nt':  # Windows
            # Obtener rutas absolutas
            python_exec = os.path.abspath(sys.executable)
            script_abs_path = os.path.abspath(script_file)
            project_root = os.path.abspath(os.getcwd())  # Directorio raíz del proyecto
            print(f"Project Path: {project_root}\.venv")
            print(f"python exe{python_exec}")
            # Construir comando completo
            command = (
                f'start cmd /k "'
                f'cd /D "{project_root}" && '
                f'"{python_exec}" "{script_abs_path}"'
                f'"'
            )

            subprocess.Popen(command, shell=True)

        else:  # Linux/Mac
            command = f'x-terminal-emulator -e "{sys.executable} {script_file}"'
            subprocess.Popen(command, shell=True)

        logger.info(f"Script {script['name']} iniciado correctamente")
        print("\n¡Proceso iniciado! Verifique la nueva ventana.")

    except Exception as e:
        logger.error(f"Error al ejecutar script: {str(e)}", exc_info=True)
        print(f"\nError: {str(e)}")

    input("\nPresione Enter para continuar...")


def create_db_setup_script():
    """Crea un script para configurar las bases de datos."""
    script_content = f"""import os
import sqlite3
import logging

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_setup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Rutas de las bases de datos
DIRECT_DB_PATH = r'{direct_db_path}'
TORRENT_DB_PATH = r'{torrent_db_path}'

def create_direct_db():
    \"\"\"Crea la base de datos direct_dw_db.db.\"\"\"
    try:
        # Asegurarse de que el directorio existe
        os.makedirs(os.path.dirname(os.path.abspath(DIRECT_DB_PATH)), exist_ok=True)

        conn = sqlite3.connect(DIRECT_DB_PATH)
        cursor = conn.cursor()

        # Crear tablas
        cursor.executescript('''
        BEGIN TRANSACTION;
        CREATE TABLE IF NOT EXISTS "links_files_download" (
            "id" INTEGER,
            "movie_id" INTEGER,
            "server_id" INTEGER,
            "language" TEXT,
            "link" TEXT,
            "quality_id" INTEGER,
            "episode_id" INTEGER,
            "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY("id" AUTOINCREMENT),
            FOREIGN KEY("episode_id") REFERENCES "series_episodes"("id"),
            FOREIGN KEY("movie_id") REFERENCES "media_downloads"("id") ON DELETE CASCADE,
            FOREIGN KEY("quality_id") REFERENCES "qualities"("quality_id"),
            FOREIGN KEY("server_id") REFERENCES "servers"("id")
        );
        CREATE TABLE IF NOT EXISTS "media_downloads" (
            "id" INTEGER,
            "title" TEXT,
            "year" INTEGER,
            "imdb_rating" REAL,
            "genre" TEXT,
            "type" TEXT CHECK("type" IN ('movie', 'serie')),
            "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
            "updated_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY("id" AUTOINCREMENT)
        );
        CREATE TABLE IF NOT EXISTS "qualities" (
            "quality_id" INTEGER,
            "quality" TEXT,
            PRIMARY KEY("quality_id" AUTOINCREMENT)
        );
        CREATE TABLE IF NOT EXISTS "series_episodes" (
            "id" INTEGER,
            "season_id" INTEGER,
            "episode" INTEGER,
            "title" TEXT,
            PRIMARY KEY("id" AUTOINCREMENT),
            FOREIGN KEY("season_id") REFERENCES "series_seasons"("id")
        );
        CREATE TABLE IF NOT EXISTS "series_seasons" (
            "id" INTEGER,
            "movie_id" INTEGER,
            "season" INTEGER,
            PRIMARY KEY("id" AUTOINCREMENT),
            FOREIGN KEY("movie_id") REFERENCES "media_downloads"("id")
        );
        CREATE TABLE IF NOT EXISTS "servers" (
            "id" INTEGER,
            "name" TEXT UNIQUE,
            PRIMARY KEY("id" AUTOINCREMENT)
        );
        CREATE TABLE IF NOT EXISTS "update_stats" (
            "update_date" DATE,
            "duration_minutes" REAL,
            "updated_movies" INTEGER,
            "new_links" INTEGER,
            "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY("update_date")
        );
        COMMIT;
        ''')

        conn.close()
        logger.info(f"Base de datos direct_dw_db.db creada correctamente en: {{DIRECT_DB_PATH}}")
        return True
    except Exception as e:
        logger.error(f"Error al crear la base de datos direct_dw_db.db: {{e}}")
        return False

def create_torrent_db():
    \"\"\"Crea la base de datos torrent_dw_db.db.\"\"\"
    try:
        # Asegurarse de que el directorio existe
        os.makedirs(os.path.dirname(os.path.abspath(TORRENT_DB_PATH)), exist_ok=True)

        conn = sqlite3.connect(TORRENT_DB_PATH)
        cursor = conn.cursor()

        # Crear tablas
        cursor.executescript('''
        BEGIN TRANSACTION;
        CREATE TABLE IF NOT EXISTS "qualities" (
            "id" INTEGER,
            "quality" TEXT NOT NULL UNIQUE,
            PRIMARY KEY("id" AUTOINCREMENT)
        );
        CREATE TABLE IF NOT EXISTS "series_episodes" (
            "id" INTEGER,
            "season_id" INTEGER NOT NULL,
            "episode_number" INTEGER NOT NULL,
            "title" TEXT NOT NULL,
            PRIMARY KEY("id" AUTOINCREMENT),
            FOREIGN KEY("season_id") REFERENCES "series_seasons"("id") ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS "series_seasons" (
            "id" INTEGER,
            "series_id" INTEGER NOT NULL,
            "season_number" INTEGER NOT NULL,
            PRIMARY KEY("id" AUTOINCREMENT),
            FOREIGN KEY("series_id") REFERENCES "torrent_downloads"("id") ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS "torrent_downloads" (
            "id" INTEGER,
            "title" TEXT NOT NULL,
            "year" INTEGER NOT NULL,
            "genre" TEXT,
            "director" TEXT,
            "type" TEXT NOT NULL CHECK("type" IN ('movie', 'series')),
            "added_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY("id" AUTOINCREMENT)
        );
        CREATE TABLE IF NOT EXISTS "torrent_files" (
            "id" INTEGER,
            "torrent_id" INTEGER,
            "episode_id" INTEGER,
            "quality_id" INTEGER NOT NULL,
            "torrent_link" TEXT NOT NULL,
            PRIMARY KEY("id" AUTOINCREMENT),
            FOREIGN KEY("episode_id") REFERENCES "series_episodes"("id") ON DELETE CASCADE,
            FOREIGN KEY("quality_id") REFERENCES "qualities"("id") ON DELETE CASCADE,
            FOREIGN KEY("torrent_id") REFERENCES "torrent_downloads"("id") ON DELETE CASCADE
        );
        COMMIT;
        ''')

        conn.close()
        logger.info(f"Base de datos torrent_dw_db.db creada correctamente en: {{TORRENT_DB_PATH}}")
        return True
    except Exception as e:
        logger.error(f"Error al crear la base de datos torrent_dw_db.db: {{e}}")
        return False

def main():
    print("Configurando bases de datos...")
    direct_success = create_direct_db()
    torrent_success = create_torrent_db()

    if direct_success and torrent_success:
        print("¡Bases de datos creadas correctamente!")
    else:
        print("Hubo errores al crear las bases de datos. Revise el archivo de log para más detalles.")

    input("Presione Enter para salir...")

if __name__ == "__main__":
    main()
"""

    with open("db_setup.py", "w", encoding="utf-8") as f:
        f.write(script_content)

    logger.info("Script de configuración de bases de datos creado")

def main():
    """Función principal del programa."""
    while True:
        option = print_menu()

        if option == "1":
            install_databases()
        elif option == "2":
            script_option = print_scripts_menu()
            if script_option in SCRIPTS:
                run_script(script_option)
            elif script_option == "5":
                continue
            else:
                print("Opción no válida.")
                input("Presione Enter para continuar...")
        elif option == "3":
            show_config()
        elif option == "4":
            print("\\n¡Gracias por usar el Sistema de Gestión de Scrapers!")
            break
        else:
            print("\\nOpción no válida. Por favor, seleccione una opción del 1 al 4.")
            input("Presione Enter para continuar...")


if __name__ == "__main__":
    try:
        # Verificar si los scripts existen
        missing_scripts = []
        for key, script in SCRIPTS.items():
            if not os.path.exists(script["file"]):
                missing_scripts.append(script["name"])

        if missing_scripts:
            print("ADVERTENCIA: Los siguientes scripts no se encuentran en el directorio actual:")
            for script in missing_scripts:
                print(f"- {script}")
            print("\\nAsegúrese de que todos los scripts estén en el mismo directorio que este programa.")
            input("Presione Enter para continuar de todos modos...")

        # Crear script de configuración de bases de datos
        create_db_setup_script()

        # Iniciar el programa
        main()
    except Exception as e:
        logger.critical(f"Error crítico en el programa principal: {e}")
        print(f"\\nError crítico: {e}")
        print("Consulte el archivo de log para más detalles.")
        input("Presione Enter para salir...")