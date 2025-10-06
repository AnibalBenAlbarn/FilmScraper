import argparse
import os
import json
import requests
from bs4 import BeautifulSoup
import time
import sqlite3
import logging
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
progress_file = os.path.join(PROJECT_ROOT, "progress", "movies_torrent_progress.json")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, "logs", "direct_scraper_films.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Base URL del sitio Dontorrent para películas
BASE_URL = "https://dontorrent.lighting/pelicula/"

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
    db_total = get_total_saved_count('movie')
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


def find_existing_movie(conn, title, year):
    """Busca una película existente priorizando coincidencias exactas de año."""
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, year
        FROM torrent_downloads
        WHERE type = 'movie' AND lower(title) = lower(?)
    """,
        (title,),
    )

    matches = cursor.fetchall()
    if not matches:
        return None, False

    for movie_id, stored_year in matches:
        if stored_year == year:
            return movie_id, True

    # Si no hay coincidencia exacta de año, usar la primera coincidencia por título
    return matches[0][0], False


def evaluate_duplicate_state(conn, torrent_id, quality_id, torrent_link):
    """Determina el estado de duplicado basado en nombre, calidad y enlace."""
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT torrent_link
        FROM torrent_files
        WHERE torrent_id = ? AND quality_id = ? AND episode_id IS NULL
    """,
        (torrent_id, quality_id),
    )

    rows = cursor.fetchall()
    if not rows:
        return "missing_quality"

    normalized_new_link = normalize_torrent_link(torrent_link).lower()
    for (existing_link,) in rows:
        if normalize_torrent_link(existing_link).lower() == normalized_new_link:
            return "exact_duplicate"

    return "quality_match"


def get_movie_data(movie_url):
    """ Extrae los datos de una película específica. """
    try:
        response = session.get(movie_url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Error al acceder a {movie_url}: Código {response.status_code}")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        title_element = soup.find('h1', class_='position-relative ml-2 descargarTitulo')
        if not title_element:
            logger.warning(f"No se encontró título en {movie_url}")
            return None

        title = title_element.text.strip().replace("Descargar", "").replace("por Torrent", "").strip()

        year = genre = director = "Desconocido"
        quality = "Unknown"  # Valor por defecto

        details_div = soup.find('div', class_='d-inline-block ml-2')
        if details_div:
            for p_tag in details_div.find_all('p', class_='m-1'):
                b_tag = p_tag.find('b')
                if not b_tag:
                    continue
                if 'Año:' in b_tag.text:
                    year_element = p_tag.find('a')
                    year = year_element.text.strip() if year_element else "Desconocido"
                elif 'Género:' in b_tag.text:
                    genre_element = p_tag.find('a')
                    genre = genre_element.text.strip() if genre_element else "Desconocido"
                elif 'Dirección:' in b_tag.text:
                    director_element = p_tag.find('a')
                    director = director_element.text.strip() if director_element else "Desconocido"

        # Buscar el formato (calidad) en el div específico
        format_div = soup.select_one('div[style="margin-right: 0%;"].d-inline-block')
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

        torrent_element = soup.find('a', href=True, id="download_torrent")
        if not torrent_element:
            logger.warning(f"No se encontró enlace de torrent en {movie_url}")
            return None

        torrent_link = "https:" + torrent_element['href'] if torrent_element else "No disponible"

        quality = normalize_quality_label(quality)
        torrent_link = normalize_torrent_link(torrent_link)

        # Convertir el año a entero si es posible
        try:
            year = int(year)
        except ValueError:
            year = 0

        return {
            'title': title,
            'year': year,
            'genre': genre,
            'director': director,
            'quality': quality,
            'torrent_link': torrent_link
        }
    except Exception as e:
        logger.error(f"Error al procesar {movie_url}: {str(e)}")
        return None


def save_to_db(movie_data):
    """Guarda los datos de una película en la base de datos."""
    try:
        conn = sqlite3.connect(db_path)

        # Verificar si la película ya existe
        movie_id, matched_by_year = find_existing_movie(
            conn, movie_data['title'], movie_data['year']
        )

        # Obtener el ID de la calidad
        quality_id = get_quality_id(conn, movie_data['quality'])

        if movie_id:
            match_context = "nombre y año" if matched_by_year else "nombre"
            logger.info(
                f"Verificación de duplicados para '{movie_data['title']}': coincidencia por {match_context} encontrada (ID {movie_id})."
            )

            duplicate_state = evaluate_duplicate_state(
                conn, movie_id, quality_id, movie_data['torrent_link']
            )

            if duplicate_state == "exact_duplicate":
                logger.info(
                    f"La película '{movie_data['title']}' ya tiene la calidad {movie_data['quality']} "
                    "con el mismo enlace .torrent."
                )
                conn.close()
                return False

            if duplicate_state == "quality_match":
                logger.info(
                    f"La película '{movie_data['title']}' coincide en nombre y calidad {movie_data['quality']} "
                    "pero el enlace es nuevo. Se guardará como fuente adicional."
                )
            else:
                logger.info(
                    f"La película '{movie_data['title']}' coincide en nombre pero no tenía la calidad {movie_data['quality']}."
                )

            # Añadir nuevo enlace de torrent para esta calidad o un enlace alternativo
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO torrent_files (torrent_id, episode_id, quality_id, torrent_link) VALUES (?, NULL, ?, ?)",
                (movie_id, quality_id, movie_data['torrent_link'])
            )
            conn.commit()
            logger.info(
                f"Añadido enlace de torrent para '{movie_data['title']}' con calidad {movie_data['quality']}"
            )
        else:
            # Insertar nueva película
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO torrent_downloads (title, year, genre, director, type) VALUES (?, ?, ?, ?, ?)",
                (movie_data['title'], movie_data['year'], movie_data['genre'], movie_data['director'], 'movie')
            )
            movie_id = cursor.lastrowid

            # Insertar enlace de torrent con la calidad
            cursor.execute(
                "INSERT INTO torrent_files (torrent_id, episode_id, quality_id, torrent_link) VALUES (?, NULL, ?, ?)",
                (movie_id, quality_id, movie_data['torrent_link'])
            )
            conn.commit()
            logger.info(f"Nueva película añadida: '{movie_data['title']}' con calidad {movie_data['quality']}")

        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error al guardar en la base de datos: {str(e)}")
        if conn:
            conn.close()
        return False


def scrape_movies(start_id=None, end_id=35000, max_consecutive_failures=100, resume=True):
    """Itera sobre los IDs de las películas y extrae los datos."""
    clear_stop_request()

    if resume:
        progress_data = load_progress()
    else:
        progress_data = {
            "current_id": 1,
            "total_saved": get_total_saved_count("movie"),
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

    total_saved = progress_data.get("total_saved", get_total_saved_count("movie"))
    next_id = current_id

    logger.info(
        "Iniciando scraping desde ID: %s, archivos guardados anteriormente: %s (reanudar=%s)",
        current_id,
        total_saved,
        resume,
    )

    consecutive_failures = 0
    stop_requested = False

    try:
        for movie_id in range(current_id, end_id + 1):
            if is_stop_requested():
                stop_requested = True
                logger.info("Señal de parada detectada. Finalizando después del ID %s", movie_id - 1)
                break

            movie_url = f"{BASE_URL}{movie_id}/"
            logger.info(f"Extrayendo: {movie_url}")

            try:
                movie_data = get_movie_data(movie_url)
                if movie_data:
                    if save_to_db(movie_data):
                        logger.info(
                            f"Guardado: {movie_data['title']} ({movie_data['year']}) - Calidad: {movie_data['quality']}")
                        total_saved += 1
                        consecutive_failures = 0
                    else:
                        logger.info(
                            f"No guardado (posible duplicado): {movie_data['title']} - Calidad: {movie_data['quality']}")
                else:
                    consecutive_failures += 1
                    logger.warning(
                        f"Película no encontrada o datos incompletos para ID: {movie_id}. Fallos consecutivos: {consecutive_failures}")

                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(
                            f"Se alcanzó el límite de {max_consecutive_failures} fallos consecutivos. Finalizando el script.")
                        next_id = movie_id + 1
                        break
            except Exception as e:
                logger.error(f"Error al procesar película ID {movie_id}: {e}")
            finally:
                next_id = movie_id + 1
                save_progress(next_id, total_saved)

            if is_stop_requested():
                stop_requested = True
                logger.info("Solicitud de parada recibida. Se detendrá antes de continuar con el siguiente ID.")
                break

            sleep_time = 1 + (movie_id % 2)
            time.sleep(sleep_time)

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
        clear_stop_request()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper de películas torrent")
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
        "--end-page",
        type=int,
        default=35000,
        help="ID/página final a procesar",
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

        scrape_movies(
            start_id=args.start_page,
            end_id=args.end_page,
            max_consecutive_failures=args.max_failures,
            resume=resume,
        )

    except KeyboardInterrupt:
        logger.info("Script interrumpido por el usuario")
        shutdown_event.set()
    except Exception as e:
        logger.critical(f"Error crítico: {str(e)}")
    finally:
        try:
            progress = load_progress()
            save_progress(progress.get("current_id", 1), progress.get("total_saved", 0))
        except Exception:
            pass
        logger.info("Script finalizado")
