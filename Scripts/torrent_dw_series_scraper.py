import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import re
import time
import random
import os
import json
from requests.exceptions import RequestException, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

#ver:1.05
# Obtener la ruta del proyecto desde las utilidades compartidas
from scraper_utils import PROJECT_ROOT

# Asegurarse de que existe el directorio de progreso
progress_dir = os.path.join(PROJECT_ROOT, "progress")
os.makedirs(progress_dir, exist_ok=True)

# Archivo de progreso
progress_file = os.path.join(PROJECT_ROOT, "progress", "series_torrent_progress.json")

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

# URL base del sitio Dontorrent para series
BASE_URL = "https://dontorrent.lighting/serie/"

# Path to the database (shared configuration)
try:  # pragma: no cover - compatible con ejecución como script o módulo
    from .scraper_utils import TORRENT_DB_PATH
except ImportError:  # pragma: no cover
    from scraper_utils import TORRENT_DB_PATH

db_path = TORRENT_DB_PATH

# Count existing torrent files for a given type
def get_total_saved_count(content_type):
    """Return number of torrent_files records for a given content type."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(tf.id)
            FROM torrent_files tf
            JOIN torrent_downloads td ON tf.torrent_id = td.id
            WHERE td.type = ?
            """,
            (content_type,),
        )
        count = cursor.fetchone()[0]
    except Exception:
        count = 0
    finally:
        if 'conn' in locals():
            conn.close()
    return count
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
session.verify = False  # Disable SSL verification for self-signed certificates
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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


def load_progress():
    """Carga el progreso guardado desde el archivo JSON y sincroniza el total con la base de datos."""
    db_total = get_total_saved_count('series')
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                progress_data = json.load(f)
                progress_data['total_saved'] = db_total
                logger.info(
                    f"Progreso cargado: ID actual = {progress_data.get('current_id', 1)}, Total guardado = {db_total}")
                return progress_data
        except Exception as e:
            logger.error(f"Error al cargar el archivo de progreso: {str(e)}")

    # Si no hay archivo o hay un error, devolver valores predeterminados
    return {"current_id": 1, "total_saved": db_total, "last_update": time.strftime("%Y-%m-%d %H:%M:%S")}


def save_progress(current_id, total_saved):
    """Guarda el progreso actual en un archivo JSON."""
    progress_data = {
        "current_id": current_id,
        "total_saved": total_saved,
        "last_update": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        with open(progress_file, 'w') as f:
            json.dump(progress_data, f, indent=4)
        logger.info(f"Progreso guardado: ID actual = {current_id}, Total guardado = {total_saved}")
    except Exception as e:
        logger.error(f"Error al guardar el archivo de progreso: {str(e)}")


def get_quality_id(conn, quality):
    """Obtiene el ID de una calidad, insertándola si no existe."""
    cursor = conn.cursor()

    # Verificar si la calidad ya existe
    cursor.execute("SELECT id FROM qualities WHERE quality = ?", (quality,))
    result = cursor.fetchone()

    if result:
        return result[0]

    # Si no existe, insertarla
    cursor.execute("INSERT INTO qualities (quality) VALUES (?)", (quality,))
    conn.commit()

    # Obtener el ID de la calidad recién insertada
    cursor.execute("SELECT id FROM qualities WHERE quality = ?", (quality,))
    result = cursor.fetchone()

    return result[0]


def check_if_series_exists(conn, title):
    """Verifica si la serie ya existe en la base de datos y devuelve su ID."""
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM torrent_downloads WHERE title = ? AND type = 'series'", (title,))
    result = cursor.fetchone()

    return result[0] if result else None


def check_if_episode_exists(conn, season_id, episode_number, episode_title):
    """Verifica si un episodio ya existe y devuelve su ID."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id FROM series_episodes 
        WHERE season_id = ? AND episode_number = ? AND title = ?
    """, (season_id, episode_number, episode_title))

    result = cursor.fetchone()
    return result[0] if result else None


def check_if_quality_link_exists(conn, episode_id, quality_id, torrent_link):
    """Verifica si ya existe un enlace de torrent para un episodio y calidad específicos."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id FROM torrent_files 
        WHERE episode_id = ? AND quality_id = ? AND torrent_link = ?
    """, (episode_id, quality_id, torrent_link))

    result = cursor.fetchone()
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


def extract_series_info(full_title):
    """Extrae el título de la serie y el número de temporada del título completo."""
    # Patrón para buscar "- Xª Temporada" o similar
    season_pattern = r'[-–]\s*(\d+)[ªa°]?\s*[Tt]emporada'

    # Buscar el patrón en el título
    season_match = re.search(season_pattern, full_title)

    if season_match:
        # Obtener el número de temporada
        season_number = int(season_match.group(1))

        # Obtener el título de la serie (todo lo que está antes del patrón)
        series_title = full_title[:season_match.start()].strip()

        return series_title, season_number
    else:
        # Si no se encuentra el patrón, asumir que es la temporada 1
        return full_title.strip(), 1


def parse_episode_range(episode_text):
    """Parsea el texto del episodio para obtener el número o rango de episodios."""
    # Buscar patrones como "1x01", "1x01 al 1x03", etc.
    single_episode_pattern = r'(\d+)x(\d+)'
    range_pattern = r'(\d+)x(\d+)\s*(?:al|a|hasta)\s*(?:\d+x)?(\d+)'

    # Primero intentar con el patrón de rango
    range_match = re.search(range_pattern, episode_text)
    if range_match:
        season = int(range_match.group(1))
        start_episode = int(range_match.group(2))
        end_episode = int(range_match.group(3))
        return start_episode, f"{season}x{start_episode} al {season}x{end_episode}"

    # Si no es un rango, buscar un episodio individual
    single_match = re.search(single_episode_pattern, episode_text)
    if single_match:
        season = int(single_match.group(1))
        episode = int(single_match.group(2))
        return episode, f"{season}x{episode}"

    # Si no se encuentra ningún patrón, devolver 0 como número de episodio
    return 0, episode_text


def scrape_series_details(url):
    """Extrae los detalles de una serie desde su URL."""
    logger.info(f"Extrayendo detalles de la serie en URL: {url}")
    soup = get_soup(url)
    if not soup:
        logger.warning(f"No se pudo obtener contenido de {url}")
        return None, None, None, []

    # Buscar el título completo (incluye nombre de la serie y temporada)
    title_element = soup.find('h2', class_='position-relative ml-2 descargarTitulo')
    if not title_element:
        logger.warning(f"No se encontró título en {url}")
        return None, None, None, []

    full_title = title_element.text.strip()

    # Extraer el título de la serie y el número de temporada
    series_title, season_number = extract_series_info(full_title)

    logger.info(f"Serie: '{series_title}', Temporada: {season_number}")

    # Buscar la calidad en el div específico
    quality = "Unknown"
    format_div = soup.select_one('div.d-inline-block')
    if format_div:
        format_p = format_div.find('p')
        if format_p and 'Formato:' in format_p.text:
            quality = format_p.text.replace('Formato:', '').strip()
            logger.info(f"Calidad encontrada: {quality}")

    # Si no se encontró en el div específico, buscar en cualquier parte
    if quality == "Unknown":
        for p_tag in soup.find_all('p'):
            if p_tag.find('b') and 'Formato:' in p_tag.text:
                quality = p_tag.text.replace('Formato:', '').strip()
                logger.info(f"Calidad encontrada (búsqueda alternativa): {quality}")
                break

    # Buscar el número total de episodios
    episodes_count = 0
    for p_tag in soup.find_all('p'):
        if 'Episodios:' in p_tag.text:
            try:
                episodes_count = int(p_tag.text.split(':')[-1].strip())
                logger.info(f"Número de episodios: {episodes_count}")
                break
            except ValueError:
                pass

    # Buscar enlaces de torrent en la tabla
    episodes = []
    table = soup.find('table', class_='table-striped')
    if not table:
        logger.warning(f"No se encontró tabla de episodios en {url}")
        return series_title, season_number, quality, []

    for row in table.select('tbody tr'):
        try:
            # Obtener el texto del episodio (puede ser un rango)
            episode_text = row.select_one('td:nth-child(1)').text.strip()

            # Parsear el número o rango de episodios
            episode_number, episode_display = parse_episode_range(episode_text)

            # Obtener el enlace del torrent
            torrent_link = row.select_one('a#download_torrent')
            if torrent_link and 'href' in torrent_link.attrs:
                full_torrent_link = f"https:{torrent_link['href']}" if torrent_link['href'].startswith("//") else \
                torrent_link['href']

                # Crear el título completo del episodio
                episode_title = f"{series_title} - {season_number}ª Temporada [{quality}] - {episode_display}"

                episodes.append((episode_number, episode_title, full_torrent_link))
        except (AttributeError, IndexError) as e:
            logger.error(f"Error al procesar fila de episodio: {e}")

    logger.info(f"Se extrajeron {len(episodes)} enlaces de torrent para la serie '{series_title}'.")
    return series_title, season_number, quality, episodes


def insert_data(db_conn, series_title, season_number, quality, episodes):
    """Inserta los datos de una serie en la base de datos."""
    if not series_title or not episodes:
        logger.warning("No hay suficientes datos para insertar")
        return False

    try:
        # Verificar si la serie ya existe
        series_id = check_if_series_exists(db_conn, series_title)

        # Obtener el ID de la calidad
        quality_id = get_quality_id(db_conn, quality)

        cursor = db_conn.cursor()

        if not series_id:
            # Insertar serie nueva
            cursor.execute("INSERT INTO torrent_downloads (title, year, genre, director, type) VALUES (?, ?, ?, ?, ?)",
                           (series_title, 2025, 'Unknown', 'Unknown', 'series'))
            series_id = cursor.lastrowid
            logger.info(f"Nueva serie añadida: '{series_title}'")

        # Insertar temporada (siempre crear una nueva temporada para cada calidad)
        cursor.execute("INSERT INTO series_seasons (series_id, season_number) VALUES (?, ?)",
                       (series_id, season_number))
        season_id = cursor.lastrowid
        logger.info(f"Nueva temporada añadida: {season_number} para serie '{series_title}' con calidad {quality}")

        # Contador para episodios añadidos
        episodes_added = 0

        # Insertar episodios
        for episode_number, episode_title, torrent_link in episodes:
            # Verificar si el episodio ya existe con este título exacto
            existing_episode_id = check_if_episode_exists(db_conn, season_id, episode_number, episode_title)

            if existing_episode_id:
                episode_id = existing_episode_id
            else:
                # Crear episodio
                cursor.execute("INSERT INTO series_episodes (season_id, episode_number, title) VALUES (?, ?, ?)",
                               (season_id, episode_number, episode_title))
                episode_id = cursor.lastrowid

            # Verificar si ya existe este enlace de torrent para este episodio y calidad
            if check_if_quality_link_exists(db_conn, episode_id, quality_id, torrent_link):
                logger.info(f"El episodio {episode_title} ya tiene enlace para calidad {quality}")
                continue

            # Insertar enlace de torrent con la calidad correspondiente
            cursor.execute(
                "INSERT INTO torrent_files (torrent_id, episode_id, quality_id, torrent_link) VALUES (?, ?, ?, ?)",
                (series_id, episode_id, quality_id, torrent_link))

            episodes_added += 1

        db_conn.commit()
        logger.info(f"Se añadieron {episodes_added} episodios para la serie '{series_title}' con calidad {quality}.")
        return episodes_added
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Error al insertar datos: {e}")
        return 0


def scrape_series(start_id=1, max_consecutive_failures=10):
    """Itera sobre los IDs de las series y extrae los datos."""
    # Cargar progreso anterior si existe
    progress_data = load_progress()
    current_id = progress_data.get("current_id", start_id)
    total_saved = progress_data.get("total_saved", 0)
    next_id = current_id

    logger.info(f"Iniciando scraping desde ID: {current_id}, archivos guardados anteriormente: {total_saved}")

    conn = sqlite3.connect(db_path)
    consecutive_failures = 0

    try:
        while True:
            series_url = f"{BASE_URL}{current_id}/{current_id}/"
            logger.info(f"Extrayendo: {series_url}")

            success = False
            try:
                for attempt in range(3):
                    series_title, season_number, quality, episodes = scrape_series_details(series_url)
                    if series_title and episodes:
                        episodes_added = insert_data(conn, series_title, season_number, quality, episodes)
                        if episodes_added:
                            logger.info(f"Guardado: {series_title} - Temporada {season_number} con calidad {quality}")
                            total_saved += episodes_added
                            consecutive_failures = 0
                        else:
                            logger.info(f"No guardado: {series_title} - Temporada {season_number} con calidad {quality}")
                        success = True
                        break
                    else:
                        logger.warning(f"Intento {attempt + 1} fallido para ID: {current_id}")
                        time.sleep(1 + attempt)
                if not success:
                    consecutive_failures += 1
                    logger.warning(
                        f"Serie no encontrada para ID: {current_id}. Fallos consecutivos: {consecutive_failures}")
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(
                            f"Se alcanzó el límite de {max_consecutive_failures} fallos consecutivos. Finalizando el script.")
                        break
            except Exception as e:
                logger.error(f"Error al procesar la serie ID {current_id}: {e}")
            finally:
                next_id = current_id + 1
                save_progress(next_id, total_saved)
                sleep_time = 1 + random.random() * 2
                time.sleep(sleep_time)
                current_id = next_id
    except KeyboardInterrupt:
        logger.info("Script interrumpido por el usuario")
    except Exception as e:
        logger.critical(f"Error crítico: {str(e)}")
    finally:
        # Guardar progreso final
        save_progress(next_id, total_saved)
        logger.info(f"Proceso completado o interrumpido. Se guardaron {total_saved} archivos de torrent en la base de datos.")
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