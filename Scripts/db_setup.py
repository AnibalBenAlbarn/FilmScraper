import os
import sqlite3
import logging

# Determinar la ruta raíz del proyecto sin depender de `main`
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, "logs", "db_setup.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Rutas de las bases de datos
DIRECT_DB_PATH = os.path.join(PROJECT_ROOT, "Scripts", "direct_dw_db.db")
TORRENT_DB_PATH = os.path.join(PROJECT_ROOT, "Scripts", "torrent_dw_db.db")


def create_direct_db(db_path=None):
    """Crea la base de datos direct_dw_db.db."""
    if db_path is None:
        db_path = DIRECT_DB_PATH

    try:
        # Asegurarse de que el directorio existe
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        conn = sqlite3.connect(db_path)
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
            "update_date" DATE PRIMARY KEY,
            "duration_minutes" REAL,
            "new_series" INTEGER,
            "new_seasons" INTEGER,
            "new_episodes" INTEGER,
            "new_links" INTEGER,
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        COMMIT;
        ''')

        conn.close()
        logger.info(f"Base de datos direct_dw_db.db creada correctamente en: {db_path}")
        return True
    except Exception as e:
        logger.error(f"Error al crear la base de datos direct_dw_db.db: {e}")
        return False


def create_torrent_db(db_path=None):
    """Crea la base de datos torrent_dw_db.db."""
    if db_path is None:
        db_path = TORRENT_DB_PATH

    try:
        # Asegurarse de que el directorio existe
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        conn = sqlite3.connect(db_path)
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
        logger.info(f"Base de datos torrent_dw_db.db creada correctamente en: {db_path}")
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