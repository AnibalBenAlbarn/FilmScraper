import argparse
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
from .scraper_utils import (
    PROJECT_ROOT,
    get_shutdown_event,
    TORRENT_DB_PATH,
    is_stop_requested,
    clear_stop_request,
)

shutdown_event = get_shutdown_event()

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
        logging.FileHandler(os.path.join(PROJECT_ROOT, "logs", "series_scraper.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# URL base del sitio Dontorrent para series
BASE_URL = "https://dontorrent.lighting/serie/"

# Path to the database (shared configuration)
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


def normalize_quality_label(quality):
    """Normaliza el texto de la calidad para evitar duplicados accidentales."""
    if not quality:
        return "Unknown"
    return quality.strip()


def normalize_torrent_link(link):
    """Normaliza enlaces de torrents para comparaciones consistentes."""
    if not link:
        return ""
    return link.strip()


def find_existing_series(conn, title):
    """Busca una serie existente por nombre (ignorando mayúsculas/minúsculas)."""
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id
        FROM torrent_downloads
        WHERE type = 'series' AND lower(title) = lower(?)
    """,
        (title,),
    )

    result = cursor.fetchone()
    return result[0] if result else None


def find_existing_season(conn, series_id, season_number):
    """Busca una temporada existente para una serie dada."""
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id
        FROM series_seasons
        WHERE series_id = ? AND season_number = ?
    """,
        (series_id, season_number),
    )

    result = cursor.fetchone()
    if result:
        return result[0], True
    return None, False


def get_or_create_episode(conn, season_id, episode_number, episode_title):
    """Obtiene el ID de un episodio, actualizando el título si es necesario."""
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, title
        FROM series_episodes
        WHERE season_id = ? AND episode_number = ?
    """,
        (season_id, episode_number),
    )

    result = cursor.fetchone()
    if result:
        episode_id, stored_title = result
        if stored_title != episode_title:
            cursor.execute(
                "UPDATE series_episodes SET title = ? WHERE id = ?",
                (episode_title, episode_id),
            )
        return episode_id, False

    cursor.execute(
        "INSERT INTO series_episodes (season_id, episode_number, title) VALUES (?, ?, ?)",
        (season_id, episode_number, episode_title),
    )
    return cursor.lastrowid, True


def evaluate_episode_duplicate_state(conn, episode_id, quality_id, torrent_link):
    """Determina el estado de duplicado basado en serie, episodio, calidad y enlace."""
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT torrent_link
        FROM torrent_files
        WHERE episode_id = ? AND quality_id = ?
    """,
        (episode_id, quality_id),
    )

    rows = cursor.fetchall()
    if not rows:
        return "missing_quality"

    normalized_new_link = normalize_torrent_link(torrent_link).lower()
    for (existing_link,) in rows:
        if normalize_torrent_link(existing_link).lower() == normalized_new_link:
            return "exact_duplicate"

    return "quality_match"


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

    quality = normalize_quality_label(quality)

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

                episodes.append(
                    (
                        episode_number,
                        episode_title,
                        normalize_torrent_link(full_torrent_link),
                    )
                )
        except (AttributeError, IndexError) as e:
            logger.error(f"Error al procesar fila de episodio: {e}")

    logger.info(f"Se extrajeron {len(episodes)} enlaces de torrent para la serie '{series_title}'.")
    return series_title, season_number, quality, episodes


def insert_data(db_conn, series_title, season_number, quality, episodes):
    """Inserta los datos de una serie en la base de datos."""
    if not series_title or not episodes:
        logger.warning("No hay suficientes datos para insertar")
        return False

    # Normalizar datos antes de guardarlos
    normalized_quality = normalize_quality_label(quality)
    normalized_episodes = []
    for episode_number, episode_title, torrent_link in episodes:
        normalized_link = normalize_torrent_link(torrent_link)
        if not normalized_link:
            logger.debug(
                f"Episodio '{episode_title}' omitido por no tener enlace de torrent válido."
            )
            continue
        normalized_episodes.append((episode_number, episode_title, normalized_link))

    if not normalized_episodes:
        logger.warning(
            f"No hay episodios válidos para insertar en la serie '{series_title}'."
        )
        return False

    try:
        cursor = db_conn.cursor()

        # Verificar si la serie ya existe
        series_id = find_existing_series(db_conn, series_title)

        if series_id:
            logger.info(
                f"Verificación de duplicados para '{series_title}': coincidencia por nombre encontrada (ID {series_id})."
            )
        else:
            cursor.execute(
                "INSERT INTO torrent_downloads (title, year, genre, director, type) VALUES (?, ?, ?, ?, ?)",
                (series_title, 0, 'Unknown', 'Unknown', 'series'),
            )
            series_id = cursor.lastrowid
            logger.info(f"Nueva serie añadida: '{series_title}'")

        # Verificar si la temporada ya existe
        season_id, season_exists = find_existing_season(db_conn, series_id, season_number)
        if season_exists:
            logger.info(
                f"La serie '{series_title}' ya tiene registrada la temporada {season_number}. Se reutilizará."
            )
        else:
            cursor.execute(
                "INSERT INTO series_seasons (series_id, season_number) VALUES (?, ?)",
                (series_id, season_number),
            )
            season_id = cursor.lastrowid
            logger.info(
                f"Nueva temporada añadida: {season_number} para serie '{series_title}' con calidad {normalized_quality}"
            )

        # Obtener el ID de la calidad
        quality_id = get_quality_id(db_conn, normalized_quality)

        episodes_added = 0

        # Insertar episodios y enlaces
        for episode_number, episode_title, torrent_link in normalized_episodes:
            episode_id, created = get_or_create_episode(
                db_conn, season_id, episode_number, episode_title
            )

            if created:
                logger.info(
                    f"Episodio creado: {episode_title} (Temporada {season_number}, Episodio {episode_number})."
                )

            duplicate_state = evaluate_episode_duplicate_state(
                db_conn, episode_id, quality_id, torrent_link
            )

            if duplicate_state == "exact_duplicate":
                logger.info(
                    f"El episodio '{episode_title}' ya tiene la calidad {normalized_quality} con el mismo enlace .torrent."
                )
                continue

            if duplicate_state == "quality_match":
                logger.info(
                    f"El episodio '{episode_title}' coincide en serie y calidad {normalized_quality} "
                    "pero el enlace es nuevo. Se guardará como fuente adicional."
                )
            else:
                logger.info(
                    f"El episodio '{episode_title}' no tenía la calidad {normalized_quality}. Se añadirá el nuevo enlace."
                )

            cursor.execute(
                "INSERT INTO torrent_files (torrent_id, episode_id, quality_id, torrent_link) VALUES (?, ?, ?, ?)",
                (series_id, episode_id, quality_id, torrent_link),
            )
            episodes_added += 1

        db_conn.commit()
        logger.info(
            f"Se añadieron {episodes_added} enlaces para la serie '{series_title}' con calidad {normalized_quality}."
        )
        return episodes_added
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Error al insertar datos: {e}")
        return 0


def scrape_series(start_id=None, max_consecutive_failures=10, resume=True):
    """Itera sobre los IDs de las series y extrae los datos."""
    clear_stop_request()

    if resume:
        progress_data = load_progress()
    else:
        progress_data = {
            "current_id": 1,
            "total_saved": get_total_saved_count("series"),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    if start_id is not None:
        try:
            current_id = max(1, int(start_id))
        except (TypeError, ValueError):
            current_id = 1
    else:
        try:
            current_id = max(1, int(progress_data.get("current_id", 1)))
        except (TypeError, ValueError):
            current_id = 1

    total_saved = progress_data.get("total_saved", get_total_saved_count("series"))
    next_id = current_id

    logger.info(
        "Iniciando scraping desde ID: %s, archivos guardados anteriormente: %s (reanudar=%s)",
        current_id,
        total_saved,
        resume,
    )

    conn = sqlite3.connect(db_path)
    consecutive_failures = 0
    stop_requested = False

    try:
        while True:
            if is_stop_requested():
                stop_requested = True
                logger.info("Señal de parada detectada. Finalizando después del ID %s", current_id - 1)
                break

            series_url = f"{BASE_URL}{current_id}/{current_id}/"
            logger.info(f"Extrayendo: {series_url}")

            try:
                success = False
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
                        next_id = current_id + 1
                        break
            except Exception as e:
                logger.error(f"Error al procesar serie ID {current_id}: {e}")
            finally:
                next_id = current_id + 1
                save_progress(next_id, total_saved)

            if is_stop_requested():
                stop_requested = True
                logger.info("Solicitud de parada recibida. Se detendrá antes de continuar con el siguiente ID.")
                break

            sleep_time = 1 + random.random() * 2
            time.sleep(sleep_time)
            current_id = next_id
    except KeyboardInterrupt:
        logger.info("Script interrumpido por el usuario")
        shutdown_event.set()
        stop_requested = True
    except Exception as e:
        logger.critical(f"Error crítico: {str(e)}")
    finally:
        # Guardar progreso final
        save_progress(next_id, total_saved)
        if stop_requested:
            logger.info(
                "Proceso detenido por solicitud del usuario. Último ID procesado: %s",
                max(current_id, next_id - 1),
            )
        else:
            logger.info(
                f"Proceso completado o interrumpido. Se guardaron {total_saved} archivos de torrent en la base de datos."
            )
        conn.close()
        clear_stop_request()


def main():
    parser = argparse.ArgumentParser(description="Scraper de series torrent")
    parser.add_argument(
        "--start-page",
        type=int,
        help="ID/página inicial desde la que comenzar el procesamiento",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reanudar desde el último progreso guardado (por defecto se reanuda)",
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Eliminar el archivo de progreso antes de iniciar",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=10,
        help="Número máximo de fallos consecutivos permitidos",
    )

    args = parser.parse_args()

    try:
        if args.reset_progress and os.path.exists(progress_file):
            os.remove(progress_file)
            logger.info("Progreso reiniciado manualmente.")

        initialize_database()

        resume = True
        if args.reset_progress or args.start_page is not None:
            resume = False
        elif args.resume:
            resume = True

        scrape_series(
            start_id=args.start_page,
            max_consecutive_failures=args.max_failures,
            resume=resume,
        )
    except Exception as e:
        logger.critical(f"Error crítico en main: {str(e)}")
    finally:
        try:
            progress = load_progress()
            save_progress(progress.get("current_id", 1), progress.get("total_saved", 0))
        except Exception:
            pass
        logger.info("Script finalizado")


if __name__ == '__main__':
    main()