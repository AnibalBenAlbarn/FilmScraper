import os
import sys
import sqlite3
import subprocess
import shutil
import re
import logging
from pathlib import Path
import argparse
from datetime import datetime

# Obtener el directorio raíz del proyecto
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Crear directorios necesarios si no existen
os.makedirs(os.path.join(PROJECT_ROOT, "logs"), exist_ok=True)
os.makedirs(os.path.join(PROJECT_ROOT, "progress"), exist_ok=True)
os.makedirs(os.path.join(PROJECT_ROOT, "Scripts"), exist_ok=True)

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, "logs", "main.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Rutas predeterminadas para las bases de datos
DEFAULT_DIRECT_DB_PATH = os.path.join(PROJECT_ROOT, "Scripts", "direct_dw_db.db")
DEFAULT_TORRENT_DB_PATH = os.path.join(PROJECT_ROOT, "Scripts", "torrent_dw_db.db")

# Rutas actuales (se actualizarán cuando el usuario las cambie)
direct_db_path = DEFAULT_DIRECT_DB_PATH
torrent_db_path = DEFAULT_TORRENT_DB_PATH

# Credenciales de inicio de sesión
USERNAME = 'rolankor'
PASSWORD = 'Rolankor_09'

# Scripts disponibles - Usar rutas relativas al directorio raíz
SCRIPTS = {
    # Scripts originales
    "1": {
        "name": "Direct Download Films Scraper",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "direct_dw_films_scraper.py"),
        "description": "Extrae información de películas para descarga directa"
    },
    "2": {
        "name": "Direct Download Series Scraper",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "direct_dw_series_scraper.py"),
        "description": "Extrae información de series para descarga directa"
    },
    "3": {
        "name": "Torrent Download Films Scraper",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "torrent_dw_films_scraper.py"),
        "description": "Extrae información de películas para descarga por torrent"
    },
    "4": {
        "name": "Torrent Download Series Scraper",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "torrent_dw_series_scraper.py"),
        "description": "Extrae información de series para descarga por torrent"
    },
    # Scripts optimizados
    "5": {
        "name": "Películas de Estreno (Optimizado)",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_movies_premiere.py"),
        "description": "Extrae información de películas de estreno (versión optimizada)"
    },
    "6": {
        "name": "Películas Actualizadas (Optimizado)",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_movies_updated.py"),
        "description": "Extrae información de películas actualizadas (versión optimizada)"
    },
    "7": {
        "name": "Episodios de Estreno (Optimizado)",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_episodes_premiere.py"),
        "description": "Extrae información de episodios de estreno (versión optimizada)"
    },
    "8": {
        "name": "Episodios Actualizados (Optimizado)",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_episodes_updated.py"),
        "description": "Extrae información de episodios actualizados (versión optimizada)"
    },
    # Opciones adicionales
    "9": {
        "name": "Ejecutar Todos los Scripts Optimizados",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "run_all.py"),
        "description": "Ejecuta todos los scripts optimizados en secuencia"
    },
    "10": {
        "name": "Crear Tareas Programadas",
        "description": "Crea tareas programadas para todos los scripts optimizados"
    }
}


def clear_screen():
    """Limpia la pantalla de la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')


def print_header():
    """Imprime el encabezado del programa."""
    clear_screen()
    print("=" * 80)
    print("SISTEMA DE SCRAPING HDFULL".center(80))
    print("=" * 80)
    print()


def print_menu():
    """Imprime el menú principal."""
    print_header()
    print("MENÚ PRINCIPAL:")
    print("-" * 80)
    print("1. Configurar bases de datos")
    print("2. Ejecutar scripts")
    print("3. Ver configuración y estadísticas")
    print("4. Salir")
    print("-" * 80)
    return input("Seleccione una opción (1-4): ")


def print_scripts_menu():
    """Imprime el menú de scripts disponibles."""
    print_header()
    print("SCRIPTS DISPONIBLES:")
    print("-" * 80)
    print("Scripts Originales:")
    for key in ["1", "2", "3", "4"]:
        script = SCRIPTS[key]
        print(f"{key}. {script['name']} - {script['description']}")

    print("\\nScripts Optimizados:")
    for key in ["5", "6", "7", "8"]:
        script = SCRIPTS[key]
        print(f"{key}. {script['name']} - {script['description']}")

    print("\\nOpciones Adicionales:")
    for key in ["9", "10"]:
        script = SCRIPTS[key]
        print(f"{key}. {script['name']} - {script['description']}")

    print("-" * 80)
    return input("Seleccione un script para ejecutar (1-10): ")


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
    print("1. Usar rutas predeterminadas")
    print("2. Especificar rutas personalizadas")
    print("3. Volver al menú principal")
    print("-" * 80)
    option = input("Seleccione una opción (1-3): ")

    if option == "1":
        direct_db_path = DEFAULT_DIRECT_DB_PATH
        torrent_db_path = DEFAULT_TORRENT_DB_PATH
        logger.info("Usando rutas predeterminadas para las bases de datos")
    elif option == "2":
        print("\\nIngrese las rutas completas para las bases de datos:")
        direct_db_path = input("Ruta para direct_dw_db.db: ").strip() or DEFAULT_DIRECT_DB_PATH
        torrent_db_path = input("Ruta para torrent_dw_db.db: ").strip() or DEFAULT_TORRENT_DB_PATH
        logger.info(f"Rutas personalizadas configuradas: {direct_db_path}, {torrent_db_path}")
    elif option == "3":
        return
    else:
        print("Opción no válida.")
        input("Presione Enter para continuar...")
        return

    # Ejecutar el script de configuración de bases de datos
    create_db_setup_script()

    try:
        print("\\nCreando bases de datos...")
        subprocess.run([sys.executable, os.path.join(PROJECT_ROOT, "Scripts", "db_setup.py")], check=True)

        # Actualizar rutas en los scripts
        print("\\nActualizando rutas en los scripts...")
        for key, script in SCRIPTS.items():
            if "file" not in script:
                continue

            script_file = script["file"]
            if os.path.exists(script_file):
                if "direct_dw" in script_file or "update_" in script_file:
                    update_direct_db_path_in_script(script_file)
                elif "torrent_dw" in script_file:
                    update_torrent_db_path_in_script(script_file)

        # Actualizar la ruta en el módulo de utilidades si existe
        utils_file = os.path.join(PROJECT_ROOT, "Scripts", "scraper_utils.py")
        if os.path.exists(utils_file):
            update_utils_db_path(utils_file)

        logger.info("Rutas de bases de datos actualizadas en todos los scripts")
        print("\\nBases de datos configuradas correctamente.")
    except Exception as e:
        logger.error(f"Error al actualizar rutas en scripts: {e}")
        print(f"\\nError: {e}")

    input("\\nPresione Enter para continuar...")


def update_direct_db_path_in_script(script_file):
    """Actualiza la ruta de la base de datos direct_dw_db.db en un script."""
    try:
        # Verificar si el archivo existe
        if not os.path.exists(script_file):
            logger.error(f"El archivo {script_file} no existe")
            return

        # Normalizar la ruta para usar barras normales y raw string
        normalized_path = direct_db_path.replace('\\', '/')
        raw_db_path = f"r'{normalized_path}'"

        with open(script_file, 'r', encoding='utf-8') as file:
            content = file.read()

        # Reemplazar la ruta de la base de datos
        if "update_" in script_file:
            # Para los scripts optimizados, actualizar el parámetro db_path
            new_content = re.sub(
                r"DB_PATH\s*=\s*.*",
                f"DB_PATH = {raw_db_path}",
                content
            )
        else:
            # Para los scripts originales, reemplazar la función connect_db
            new_content = re.sub(
                r"def connect_db\(\):.+?return connection",
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
        # Verificar si el archivo existe
        if not os.path.exists(script_file):
            logger.error(f"El archivo {script_file} no existe")
            return

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


def update_utils_db_path(utils_file):
    """Actualiza la ruta de la base de datos en el módulo de utilidades."""
    try:
        # Verificar si el archivo existe
        if not os.path.exists(utils_file):
            logger.error(f"El archivo {utils_file} no existe")
            return

        # Normalizar la ruta para usar barras normales y raw string
        normalized_path = direct_db_path.replace('\\', '/')
        raw_db_path = f"r'{normalized_path}'"

        with open(utils_file, 'r', encoding='utf-8') as file:
            content = file.read()

        # Reemplazar la ruta de la base de datos
        new_content = re.sub(
            r"DB_PATH\s*=\s*.*",
            f"DB_PATH = {raw_db_path}",
            content
        )

        # Actualizar credenciales
        new_content = re.sub(
            r"USERNAME\s*=\s*'[^']*'",
            f"USERNAME = '{USERNAME}'",
            new_content
        )

        new_content = re.sub(
            r"PASSWORD\s*=\s*'[^']*'",
            f"PASSWORD = '{PASSWORD}'",
            new_content
        )

        with open(utils_file, 'w', encoding='utf-8') as file:
            file.write(new_content)

        logger.info(f"Ruta de base de datos y credenciales actualizadas en {utils_file}")
    except Exception as e:
        logger.error(f"Error al actualizar ruta en {utils_file}: {e}")


def run_script(script_key):
    """Ejecuta un script seleccionado."""
    if script_key == "10":
        # Crear tareas programadas
        create_scheduled_tasks()
        return

    script = SCRIPTS.get(script_key)
    if not script:
        print("Script no válido.")
        input("Presione Enter para continuar...")
        return

    script_file = script.get("file")
    if not script_file or not os.path.exists(script_file):
        print(f"\\nError: El archivo {script_file} no existe.")
        logger.error(f"El script {script_file} no existe")
        input("\\nPresione Enter para continuar...")
        return

    print_header()
    print(f"EJECUTANDO: {script['name']}")
    print("-" * 80)

    # Preguntar por parámetros adicionales para scripts optimizados
    cmd_args = []
    if script_key in ["5", "6", "7", "8"]:
        db_path = input("\\nIntroduzca la ruta de la base de datos (deje en blanco para usar la ruta actual): ")
        if db_path:
            cmd_args.extend(["--db-path", db_path])

        if script_key in ["5", "6"]:  # Solo para películas
            max_pages = input("Número máximo de páginas a procesar (deje en blanco para procesar todas): ")
            if max_pages:
                cmd_args.extend(["--max-pages", max_pages])

        max_workers = input(
            "Número máximo de workers para procesamiento paralelo (deje en blanco para usar el valor por defecto): ")
        if max_workers:
            cmd_args.extend(["--max-workers", max_workers])

    print("\\nIniciando ejecución en nueva ventana...")

    try:
        if os.name == 'nt':  # Windows
            # Obtener rutas absolutas
            python_exec = os.path.abspath(sys.executable)
            script_abs_path = os.path.abspath(script_file)

            # Construir comando completo
            cmd_parts = [f'cd /D "{PROJECT_ROOT}" && "{python_exec}" "{script_abs_path}"']
            if cmd_args:
                cmd_parts[0] += ' ' + ' '.join(cmd_args)

            command = f'start cmd /k "{" ".join(cmd_parts)}"'

            subprocess.Popen(command, shell=True)

        else:  # Linux/Mac
            cmd_parts = [sys.executable, script_file] + cmd_args
            command = f'x-terminal-emulator -e "{" ".join(cmd_parts)}"'
            subprocess.Popen(command, shell=True)

        logger.info(f"Script {script['name']} iniciado correctamente")
        print("\\n¡Proceso iniciado! Verifique la nueva ventana.")

    except Exception as e:
        logger.error(f"Error al ejecutar script: {str(e)}", exc_info=True)
        print(f"\\nError: {str(e)}")

    input("\\nPresione Enter para continuar...")


def create_scheduled_tasks():
    """Crea tareas programadas para los scripts optimizados."""
    print_header()
    print("CREACIÓN DE TAREAS PROGRAMADAS")
    print("-" * 80)

    # Crear un script temporal para crear las tareas programadas
    temp_script_path = os.path.join(PROJECT_ROOT, "temp.py")

    with open(temp_script_path, "w") as f:
        f.write(f"""import subprocess
import sys
import os

scripts = [
    {{"file": "{os.path.join(PROJECT_ROOT, "Scripts", "update_movies_premiere.py")}", "time": "03:00:00"}},
    {{"file": "{os.path.join(PROJECT_ROOT, "Scripts", "update_movies_updated.py")}", "time": "06:00:00"}},
    {{"file": "{os.path.join(PROJECT_ROOT, "Scripts", "update_episodes_premiere.py")}", "time": "04:00:00"}},
    {{"file": "{os.path.join(PROJECT_ROOT, "Scripts", "update_episodes_updated.py")}", "time": "05:00:00"}}
]

print("="*80)
print("CREACIÓN DE TAREAS PROGRAMADAS")
print("="*80)

for script_info in scripts:
    script = script_info["file"]
    print(f"\\\nCreando tarea programada para: {{os.path.basename(script)}}")
    print(f"Hora programada: {{script_info['time']}}")

    try:
        result = subprocess.run([sys.executable, script, "--create-scheduler"], check=True)
        print("Tarea programada creada correctamente.")
        print()
    except subprocess.CalledProcessError as e:
        print(f"ERROR: No se pudo crear la tarea programada para {{script}}. Código de error: {{e.returncode}}")
        print()
    except Exception as e:
        print(f"ERROR: {{str(e)}}")
        print()

print("="*80)

input("\\\nPresione Enter para salir...")
""")

    try:
        if os.name == 'nt':  # Windows
            # Obtener rutas absolutas
            python_exec = os.path.abspath(sys.executable)
            script_abs_path = os.path.abspath(temp_script_path)

            # Construir comando completo
            command = (
                f'start cmd /k "'
                f'cd /D "{PROJECT_ROOT}" && '
                f'"{python_exec}" "{script_abs_path}"'
                f'"'
            )

            subprocess.Popen(command, shell=True)

        else:  # Linux/Mac
            command = f'x-terminal-emulator -e "{sys.executable} {temp_script_path}"'
            subprocess.Popen(command, shell=True)

        logger.info("Creación de tareas programadas iniciada correctamente")
        print("\\n¡Proceso iniciado! Verifique la nueva ventana.")

    except Exception as e:
        logger.error(f"Error al crear tareas programadas: {str(e)}", exc_info=True)
        print(f"\\nError: {str(e)}")

    input("\\nPresione Enter para continuar...")


def show_config():
    """Muestra la configuración actual."""
    print_header()
    print("CONFIGURACIÓN ACTUAL")
    print("-" * 80)
    print(f"Ruta de la base de datos direct_dw_db.db: {direct_db_path}")
    print(f"Ruta de la base de datos torrent_dw_db.db: {torrent_db_path}")
    print(f"Usuario HDFull: {USERNAME}")
    print(f"Contraseña HDFull: {'*' * len(PASSWORD)}")

    # Mostrar estadísticas de la base de datos si existe
    if os.path.exists(direct_db_path):
        try:
            conn = sqlite3.connect(direct_db_path)
            cursor = conn.cursor()

            # Contar películas
            cursor.execute("SELECT COUNT(*) FROM media_downloads WHERE type='movie'")
            movie_count = cursor.fetchone()[0]

            # Contar series
            cursor.execute("SELECT COUNT(*) FROM media_downloads WHERE type='serie'")
            series_count = cursor.fetchone()[0]

            # Contar temporadas
            cursor.execute("SELECT COUNT(*) FROM series_seasons")
            seasons_count = cursor.fetchone()[0]

            # Contar episodios
            cursor.execute("SELECT COUNT(*) FROM series_episodes")
            episodes_count = cursor.fetchone()[0]

            # Contar enlaces
            cursor.execute("SELECT COUNT(*) FROM links_files_download")
            links_count = cursor.fetchone()[0]

            print("\\nEstadísticas de la base de datos:")
            print(f"- Películas: {movie_count}")
            print(f"- Series: {series_count}")
            print(f"- Temporadas: {seasons_count}")
            print(f"- Episodios: {episodes_count}")
            print(f"- Enlaces: {links_count}")

            # Mostrar últimas actualizaciones
            print("\\nÚltimas actualizaciones de películas:")
            cursor.execute("""
                SELECT update_date, updated_movies, new_links 
                FROM update_stats 
                ORDER BY update_date DESC 
                LIMIT 5
            """)
            movie_updates = cursor.fetchall()

            if movie_updates:
                for update in movie_updates:
                    print(f"- {update[0]}: {update[1]} películas, {update[2]} enlaces")
            else:
                print("- No hay actualizaciones registradas")

            # Verificar si existe la tabla de estadísticas de episodios
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='episode_update_stats'")
            if cursor.fetchone():
                print("\\nÚltimas actualizaciones de episodios:")
                cursor.execute("""
                    SELECT update_date, new_episodes, new_links 
                    FROM episode_update_stats 
                    ORDER BY update_date DESC 
                    LIMIT 5
                """)
                episode_updates = cursor.fetchall()

                if episode_updates:
                    for update in episode_updates:
                        print(f"- {update[0]}: {update[1]} episodios, {update[2]} enlaces")
                else:
                    print("- No hay actualizaciones registradas")

            conn.close()
        except Exception as e:
            print(f"\\nError al obtener estadísticas: {e}")

    print("\\nScripts disponibles:")
    print("Scripts Originales:")
    for key in ["1", "2", "3", "4"]:
        script = SCRIPTS[key]
        script_exists = os.path.exists(script["file"])
        print(f"- {script['name']}: {'Disponible' if script_exists else 'No disponible'}")

    print("\\nScripts Optimizados:")
    for key in ["5", "6", "7", "8"]:
        script = SCRIPTS[key]
        script_exists = os.path.exists(script["file"])
        print(f"- {script['name']}: {'Disponible' if script_exists else 'No disponible'}")

    print("\\nOpciones Adicionales:")
    for key in ["9", "10"]:
        script = SCRIPTS[key]
        if "file" in script:
            script_exists = os.path.exists(script["file"])
            print(f"- {script['name']}: {'Disponible' if script_exists else 'No disponible'}")
        else:
            print(f"- {script['name']}")

    print()
    print(f"Directorio del proyecto: {PROJECT_ROOT}")
    print(f"Directorio de logs: {os.path.join(PROJECT_ROOT, 'logs')}")
    print(f"Directorio de progreso: {os.path.join(PROJECT_ROOT, 'progress')}")
    print(f"Directorio de scripts: {os.path.join(PROJECT_ROOT, 'Scripts')}")

    input("\\nPresione Enter para continuar...")


def create_db_setup_script():
    """Crea un script para configurar las bases de datos."""
    script_path = os.path.join(PROJECT_ROOT, "Scripts", "db_setup.py")

    # Asegurarse de que el directorio existe
    os.makedirs(os.path.dirname(script_path), exist_ok=True)

    script_content = f"""import os
import sqlite3
import logging

# Configuración del logging
os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "db_setup.log")),
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
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY("update_date")
        );
        CREATE TABLE IF NOT EXISTS "episode_update_stats" (
            "update_date" DATE,
            "duration_minutes" REAL,
            "new_series" INTEGER,
            "new_seasons" INTEGER,
            "new_episodes" INTEGER,
            "new_links" INTEGER,
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY("update_date")
        );

        -- Crear índices para mejorar el rendimiento
        CREATE INDEX IF NOT EXISTS idx_media_downloads_title ON media_downloads(title, type);
        CREATE INDEX IF NOT EXISTS idx_series_seasons_movie_id ON series_seasons(movie_id);
        CREATE INDEX IF NOT EXISTS idx_series_episodes_season_id ON series_episodes(season_id);
        CREATE INDEX IF NOT EXISTS idx_links_movie_id ON links_files_```
        CREATE INDEX IF NOT EXISTS idx_links_movie_id ON links_files_download(movie_id);
        CREATE INDEX IF NOT EXISTS idx_links_episode_id ON links_files_download(episode_id);

        COMMIT;
        ''')

        conn.close()
        logger.info(f"Base de datos direct_dw_db.db creada correctamente en: {DIRECT_DB_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error al crear la base de datos direct_dw_db.db: {e}")
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
        logger.info(f"Base de datos torrent_dw_db.db creada correctamente en: {TORRENT_DB_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error al crear la base de datos torrent_dw_db.db: {e}")
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

    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script_content)

    logger.info("Script de configuración de bases de datos creado")


def check_and_create_optimized_scripts():
    """Verifica si los scripts optimizados existen y los crea si es necesario."""
    scripts_dir = os.path.join(PROJECT_ROOT, "Scripts")
    os.makedirs(scripts_dir, exist_ok=True)

    # Lista de scripts optimizados
    optimized_scripts = {
        "update_movies_premiere.py": "Películas de Estreno",
        "update_movies_updated.py": "Películas Actualizadas",
        "update_episodes_premiere.py": "Episodios de Estreno",
        "update_episodes_updated.py": "Episodios Actualizados",
        "run_all.py": "Ejecutar Todos los Scripts",
        "scraper_utils.py": "Utilidades Compartidas"
    }

    missing_scripts = []
    for script, description in optimized_scripts.items():
        script_path = os.path.join(scripts_dir, script)
        if not os.path.exists(script_path):
            missing_scripts.append((script, description))

    if missing_scripts:
        print_header()
        print("SCRIPTS OPTIMIZADOS NO ENCONTRADOS")
        print("-" * 80)
        print("Los siguientes scripts optimizados no se encontraron en el directorio de scripts:")
        for script, description in missing_scripts:
            print(f"- {script} ({description})")

        print("\\nEstos scripts ofrecen mejor rendimiento y más funcionalidades.")
        print("¿Desea descargar e instalar los scripts optimizados?")
        print("1. Sí, descargar e instalar")
        print("2. No, continuar sin ellos")

        option = input("\\nSeleccione una opción (1-2): ")

        if option == "1":
            print("\\nDescargando e instalando scripts optimizados...")
            # Aquí iría el código para descargar los scripts
            # Por ahora, simplemente mostramos un mensaje
            print("\\nFuncionalidad no implementada aún.")
            print("Por favor, copie manualmente los scripts optimizados al directorio:")
            print(f"{scripts_dir}")
            input("\\nPresione Enter para continuar...")

    return len(missing_scripts) == 0


def main():
    """Función principal del programa."""
    # Verificar si los scripts optimizados existen
    has_optimized_scripts = check_and_create_optimized_scripts()

    while True:
        option = print_menu()

        if option == "1":
            install_databases()
        elif option == "2":
            script_option = print_scripts_menu()
            if script_option in SCRIPTS:
                run_script(script_option)
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
        # Verificar argumentos de línea de comandos
        parser = argparse.ArgumentParser(description='Sistema de Gestión de Scrapers HDFull')
        parser.add_argument('--run-script', type=str, help='Ejecutar un script específico (1-10)')
        parser.add_argument('--db-path', help='Establecer la ruta de la base de datos direct_dw_db.db')
        parser.add_argument('--torrent-db-path', help='Establecer la ruta de la base de datos torrent_dw_db.db')

        args = parser.parse_args()

        # Procesar argumentos
        if args.db_path:
            direct_db_path = args.db_path
            logger.info(f"Ruta de base de datos direct_dw_db.db establecida por línea de comandos: {direct_db_path}")

        if args.torrent_db_path:
            torrent_db_path = args.torrent_db_path
            logger.info(f"Ruta de base de datos torrent_dw_db.db establecida por línea de comandos: {torrent_db_path}")

        if args.run_script and args.run_script in SCRIPTS:
            run_script(args.run_script)
            sys.exit(0)

        # Verificar si los scripts existen
        missing_scripts = []
        for key, script in SCRIPTS.items():
            if "file" in script and not os.path.exists(script["file"]):
                missing_scripts.append(script["name"])

        if missing_scripts:
            print("ADVERTENCIA: Los siguientes scripts no se encuentran en el directorio esperado:")
            for script in missing_scripts:
                print(f"- {script}")
            print("\\nAsegúrese de que todos los scripts estén en la carpeta 'Scripts' del proyecto.")
            print("La estructura esperada es:")
            print("- HdfullScrappers/")
            print("  |- main.py (este archivo)")
            print("  |- Scripts/")
            print("     |- direct_dw_films_scraper.py")
            print("     |- direct_dw_series_scraper.py")
            print("     |- torrent_dw_films_scraper.py")
            print("     |- torrent_dw_series_scraper.py")
            print("     |- update_movies_premiere.py")
            print("     |- update_movies_updated.py")
            print("     |- update_episodes_premiere.py")
            print("     |- update_episodes_updated.py")
            print("     |- run_all.py")
            print("     |- scraper_utils.py")
            input("Presione Enter para continuar de todos modos...")

        # Iniciar el programa
        main()
    except Exception as e:
        logger.critical(f"Error crítico en el programa principal: {e}")
        print(f"\\nError crítico: {e}")
        print("Consulte el archivo de log para más detalles.")
        input("Presione Enter para salir...")