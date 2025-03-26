import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import re
import time
import random
from requests.exceptions import RequestException, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuración del registro
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/series_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Path to the database
db_path = r'D:/Workplace/HdfullScrappers/Scripts/torrent_dw_db.db'

# Headers (para evitar ser bloqueado)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0"
}

# Create a session with retry mechanism
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))


def initialize_database():
    """Inicializa la base de datos y crea las tablas si no existen."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

    conn.commit()
    conn.close()
    logger.info("Base de datos inicializada correctamente")


def check_if_series_exists(title):
    """Verifica si la serie ya existe en la base de datos."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM torrent_downloads WHERE title = ? AND type = 'series'", (title,))
    result = cursor.fetchone()

    conn.close()
    return result is not None


def get_soup(url, retries=3):
    """Obtiene el contenido HTML de una URL y lo convierte en un objeto BeautifulSoup."""
    for attempt in range(retries):
        try:
            logger.debug(f"Solicitando URL: {url} (intento {attempt + 1}/{retries})")
            response = session.get(url, timeout=10, headers=headers)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except (RequestException, HTTPError) as e:
            logger.error(f"Error al solicitar URL: {e}. Reintentando...")
            time.sleep(2 + attempt * 2)  # Incrementar tiempo de espera en cada intento
    return None


def scrape_series_details(url):
    """Extrae los detalles de una serie desde su URL."""
    logger.info(f"Extrayendo detalles de la serie en URL: {url}")
    soup = get_soup(url)
    if not soup:
        logger.warning(f"No se pudo obtener contenido de {url}")
        return None, "Unknown", []

    # Buscar el título
    title_element = soup.find('h2', class_='position-relative ml-2 descargarTitulo')
    if not title_element:
        logger.warning(f"No se encontró título en {url}")
        return None, "Unknown", []

    title = title_element.text.strip()

    # Buscar detalles
    details_div = soup.find('div', class_='d-inline-block ml-2')
    if not details_div:
        logger.warning(f"No se encontraron detalles en {url}")
        return title, "Unknown", []

    quality = "Unknown"
    episodes_count = 0

    for p_tag in details_div.find_all('p', class_='m-1'):
        b_tag = p_tag.find('b')
        if not b_tag:
            continue
        if 'Formato:' in b_tag.text:
            quality = p_tag.text.split(':')[-1].strip()
        elif 'Episodios:' in b_tag.text:
            try:
                episodes_count = int(p_tag.text.split(':')[-1].strip())
            except ValueError:
                episodes_count = 0

    # Buscar enlaces de torrent
    episodes = []
    table = soup.find('table', class_='table-striped')
    if not table:
        logger.warning(f"No se encontró tabla de episodios en {url}")
        return title, quality, []

    for row in table.select('tbody tr'):
        try:
            episode_range = row.select_one('td:nth-child(1)').text.strip()
            torrent_link = row.select_one('a#download_torrent')
            if torrent_link and 'href' in torrent_link.attrs:
                full_torrent_link = f"https:{torrent_link['href']}" if torrent_link['href'].startswith("//") else \
                torrent_link['href']
                episodes.append((episode_range, full_torrent_link))
        except (AttributeError, IndexError) as e:
            logger.error(f"Error al procesar fila de episodio: {e}")

    logger.info(f"Se extrajeron {len(episodes)} enlaces de torrent para la serie '{title}'.")
    return title, quality, episodes


def insert_data(db_conn, title, quality, episodes):
    """Inserta los datos de una serie en la base de datos."""
    if check_if_series_exists(title):
        logger.info(f"La serie '{title}' ya existe en la base de datos")
        return False

    cursor = db_conn.cursor()
    try:
        # Insertar calidad si no existe
        cursor.execute("INSERT OR IGNORE INTO qualities (quality) VALUES (?)", (quality,))
        cursor.execute("SELECT id FROM qualities WHERE quality = ?", (quality,))
        quality_id = cursor.fetchone()[0]

        # Insertar serie
        cursor.execute("INSERT INTO torrent_downloads (title, year, genre, director, type) VALUES (?, ?, ?, ?, ?)",
                       (title, 2025, 'Unknown', 'Unknown', 'series'))
        series_id = cursor.lastrowid

        # Insertar temporada
        cursor.execute("INSERT INTO series_seasons (series_id, season_number) VALUES (?, ?)", (series_id, 1))
        season_id = cursor.lastrowid

        # Insertar episodios
        for episode_range, torrent_link in episodes:
            episode_num_match = re.findall(r'\d+', episode_range)
            episode_num = int(episode_num_match[0]) if episode_num_match else 1
            episode_title = f"{title} - {episode_range}"

            cursor.execute("INSERT INTO series_episodes (season_id, episode_number, title) VALUES (?, ?, ?)",
                           (season_id, episode_num, episode_title))
            episode_id = cursor.lastrowid

            cursor.execute(
                "INSERT INTO torrent_files (torrent_id, episode_id, quality_id, torrent_link) VALUES (?, ?, ?, ?)",
                (series_id, episode_id, quality_id, torrent_link))

        db_conn.commit()
        logger.info(f"Datos insertados en la base de datos para la serie '{title}'.")
        return True
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Error al insertar datos: {e}")
        return False
    finally:
        cursor.close()


def scrape_series(start_id=1, max_consecutive_failures=10):
    """Itera sobre los IDs de las series y extrae los datos."""
    conn = sqlite3.connect(db_path)
    series_id = start_id
    consecutive_failures = 0
    total_saved = 0

    try:
        while True:
            series_url = f"https://dontorrent.schule/serie/{series_id}/{series_id}/"
            logger.info(f"Extrayendo: {series_url}")

            success = False
            for attempt in range(3):
                title, quality, episodes = scrape_series_details(series_url)
                if title and title != "Desconocido" and episodes:
                    if insert_data(conn, title, quality, episodes):
                        logger.info(f"Guardado: {title}")
                        total_saved += 1
                        consecutive_failures = 0  # Reiniciar contador de fallos
                    success = True
                    break
                else:
                    logger.warning(f"Intento {attempt + 1} fallido para ID: {series_id}")
                    time.sleep(1 + attempt)  # Incrementar tiempo de espera en cada intento

            if not success:
                consecutive_failures += 1
                logger.warning(f"Serie no encontrada para ID: {series_id}. Fallos consecutivos: {consecutive_failures}")

                if consecutive_failures >= max_consecutive_failures:
                    logger.error(
                        f"Se alcanzó el límite de {max_consecutive_failures} fallos consecutivos. Finalizando el script.")
                    break

            # Pausa aleatoria para evitar bloqueos (entre 1 y 3 segundos)
            sleep_time = 1 + random.random() * 2
            time.sleep(sleep_time)

            series_id += 1
    except KeyboardInterrupt:
        logger.info("Script interrumpido por el usuario")
    except Exception as e:
        logger.critical(f"Error crítico: {str(e)}")
    finally:
        logger.info(f"Proceso completado. Se guardaron {total_saved} series en la base de datos.")
        conn.close()


def main():
    try:
        # Inicializar la base de datos
        initialize_database()

        # Ejecutar el scrapeo
        scrape_series(start_id=1, max_consecutive_failures=10)
    except Exception as e:
        logger.critical(f"Error crítico en main: {str(e)}")
    finally:
        logger.info("Script finalizado")


if __name__ == '__main__':
    main()