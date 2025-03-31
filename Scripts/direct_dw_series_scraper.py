import time
import re
import sqlite3
import json
import os
import logging
import sys
import concurrent.futures
import atexit
import signal
from queue import Queue
from threading import Lock
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from bs4 import BeautifulSoup

# Importar PROJECT_ROOT desde main.py
try:
    from main import PROJECT_ROOT
except ImportError:
    # Si no se puede importar, usar el directorio actual
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Configuración del logger para evitar duplicación
logger = logging.getLogger("series_scraper")
logger.setLevel(logging.DEBUG)
logger.propagate = False  # Importante para evitar duplicación

# Limpiar handlers previos
if logger.handlers:
    logger.handlers.clear()

# Asegurar que el directorio de logs existe
logs_dir = os.path.join(PROJECT_ROOT, "logs")
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Handler para consola con salida a stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Handler para archivo
file_handler = logging.FileHandler(os.path.join(logs_dir, "direct_scraper_series.log"))
file_handler.setLevel(logging.INFO)

# Formato único para ambos handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# Redirigir excepciones no capturadas
def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error("Excepción no capturada", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception

# Credenciales de inicio de sesión para diferentes cuentas
user_credentials = [
    {"username": "javierhesd", "password": "Larrykapija"},
    {"username": "grhsfvghjsawevfw", "password": "Larrykapija"},
    {"username": "sdrghsdrha", "password": "Larrykapija"},
    {"username": "rolankor", "password": "Rolankor_09"}
]

# URL de la página de inicio de sesión y de las series
login_url = "https://hdfull.blog/login"
base_url = "https://hdfull.blog"
series_url = "https://hdfull.blog/series/abc/"

# Directorio para guardar el progreso
progress_dir = os.path.join(PROJECT_ROOT, "progress")
if not os.path.exists(progress_dir):
    os.makedirs(progress_dir)

# Archivo para guardar el progreso
progress_file = os.path.join(progress_dir, "series_progress.json")

# Ruta de la base de datos
db_path = r'D:/Workplace/HdfullScrappers/Scripts/direct_dw_db.db'

# Contador de reinicios del script
restart_count = 0
MAX_RESTARTS = 3

# Número de workers para el scraping paralelo
NUM_WORKERS = 4

# Lock para sincronizar el acceso a la base de datos
db_lock = Lock()

# Cola para almacenar las URLs de las series a procesar
series_queue = Queue()

# Lista para mantener referencia a todos los drivers
all_drivers = []

# Lock para el archivo de progreso
progress_lock = Lock()


# Función para crear un nuevo driver de Chrome
def create_driver():
    service = Service(os.path.join(PROJECT_ROOT, "chromedriver.exe"))
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Ejecuta Chrome en modo headless
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=service, options=options)
    all_drivers.append(driver)  # Añadir a la lista global
    return driver


# Función para cerrar todos los drivers
def close_all_drivers():
    logger.info(f"Cerrando {len(all_drivers)} drivers...")
    for driver in all_drivers:
        try:
            driver.quit()
        except Exception as e:
            logger.error(f"Error al cerrar driver: {e}")
    logger.info("Todos los drivers han sido cerrados.")


# Registrar función para cerrar drivers al salir
atexit.register(close_all_drivers)


# Manejar señales de terminación
def signal_handler(sig, frame):
    logger.info(f"Señal recibida: {sig}. Cerrando drivers y finalizando...")
    close_all_drivers()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# Función para inicializar la base de datos
def initialize_db():
    with db_lock:
        try:
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()

            # Crear tablas si no existen
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS media_downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                year INTEGER,
                imdb_rating REAL,
                genre TEXT,
                type TEXT CHECK(type IN ('movie', 'serie')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS qualities (
                quality_id INTEGER PRIMARY KEY AUTOINCREMENT,
                quality TEXT
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS series_seasons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                season INTEGER,
                FOREIGN KEY(movie_id) REFERENCES media_downloads(id)
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS series_episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id INTEGER,
                episode INTEGER,
                title TEXT,
                FOREIGN KEY(season_id) REFERENCES series_seasons(id)
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS links_files_download (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                server_id INTEGER,
                language TEXT,
                link TEXT,
                quality_id INTEGER,
                episode_id INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(movie_id) REFERENCES media_downloads(id) ON DELETE CASCADE,
                FOREIGN KEY(server_id) REFERENCES servers(id),
                FOREIGN KEY(quality_id) REFERENCES qualities(quality_id),
                FOREIGN KEY(episode_id) REFERENCES series_episodes(id)
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes REAL,
                updated_movies INTEGER,
                new_links INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            connection.commit()
            logger.info("Base de datos inicializada correctamente")
            return True
        except Exception as e:
            logger.error(f"Error al inicializar la base de datos: {e}")
            return False
        finally:
            if connection:
                connection.close()


# Función para conectar a la base de datos
def connect_db():
    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        logger.debug("Conexión a la base de datos establecida correctamente")
        return connection
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise


# Función para iniciar sesión con credenciales específicas
def login(driver, username, password):
    try:
        logger.info(f"Iniciando sesión con usuario: {username}")
        driver.get(login_url)
        username_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
        password_field = driver.find_element(By.NAME, "password")
        login_button = driver.find_element(By.XPATH, "//a[text()='Ingresar']")

        username_field.send_keys(username)
        password_field.send_keys(password)
        login_button.click()

        WebDriverWait(driver, 10).until(EC.url_changes(login_url))
        logger.info(f"Sesión iniciada correctamente con usuario: {username}")
        return True
    except Exception as e:
        logger.error(f"Error al iniciar sesión con usuario {username}: {e}")
        return False


# Función para reiniciar el driver y la sesión
def restart_driver(driver, username, password):
    logger.info(f"Reiniciando el driver para usuario {username}...")
    try:
        driver.quit()
        if driver in all_drivers:
            all_drivers.remove(driver)
        time.sleep(2)  # Esperar a que se cierre correctamente

        new_driver = create_driver()
        login_success = login(new_driver, username, password)

        if login_success:
            logger.info(f"Driver reiniciado y sesión iniciada correctamente para usuario {username}")
            return new_driver
        else:
            logger.error(f"No se pudo iniciar sesión después de reiniciar el driver para usuario {username}")
            return None
    except Exception as e:
        logger.error(f"Error al reiniciar el driver para usuario {username}: {e}")
        return None


# Función para verificar si una serie ya existe en la base de datos
def series_exists(title, year, imdb_rating, genre):
    with db_lock:
        connection = connect_db()
        cursor = connection.cursor()
        try:
            cursor.execute('''
                SELECT id FROM media_downloads 
                WHERE title=? AND year=? AND imdb_rating=? AND genre=? AND type='serie'
            ''', (title, year, imdb_rating, genre))
            result = cursor.fetchone()
            exists = result is not None
            logger.debug(
                f"Verificación de existencia de serie: {title} ({year}) - {'Existe' if exists else 'No existe'}")
            return exists, result['id'] if exists else None
        except Exception as e:
            logger.error(f"Error al verificar si la serie existe: {e}")
            return False, None
        finally:
            cursor.close()
            connection.close()


# Función para verificar si un episodio ya existe en la base de datos
def episode_exists(season_id, episode_number, title):
    with db_lock:
        connection = connect_db()
        cursor = connection.cursor()
        try:
            cursor.execute('''
                SELECT id FROM series_episodes 
                WHERE season_id=? AND episode=? AND title=?
            ''', (season_id, episode_number, title))
            result = cursor.fetchone()
            exists = result is not None
            logger.debug(
                f"Verificación de existencia de episodio: temporada {season_id}, episodio {episode_number} - {'Existe' if exists else 'No existe'}")
            return exists, result['id'] if exists else None
        except Exception as e:
            logger.error(f"Error al verificar si el episodio existe: {e}")
            return False, None
        finally:
            cursor.close()
            connection.close()


# Función para extraer detalles del episodio
def extract_episode_details(driver, episode_url, episode_number, title, worker_id, username):
    logger.info(f"Worker {worker_id} (usuario {username}): Extrayendo detalles del episodio: {episode_url}")
    try:
        driver.get(episode_url)
        time.sleep(1)  # Esperar a que se cargue la página

        # Crear lista para almacenar los enlaces
        server_links = []

        # Encontrar todos los embed-selectors
        embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
        logger.debug(
            f"Worker {worker_id} (usuario {username}): Número de enlaces encontrados para episodio {episode_number}: {len(embed_selectors)}")

        for embed_selector in embed_selectors:
            language = None
            server = None
            embedded_link = None

            try:
                # Extraer idioma y servidor antes de hacer clic
                embed_html = embed_selector.get_attribute('outerHTML')
                embed_soup = BeautifulSoup(embed_html, "lxml")

                if "Audio Español" in embed_soup.text:
                    language = "Audio Español"
                elif "Subtítulo Español" in embed_soup.text:
                    language = "Subtítulo Español"
                elif "Audio Latino" in embed_soup.text:
                    language = "Audio Latino"

                server_tag = embed_soup.find("b", class_="provider")
                if server_tag:
                    server = server_tag.text.strip().lower()

                # Hacer clic en el selector
                embed_selector.click()
                time.sleep(1)  # Esperar 1 segundo para que el contenido se cargue

                # Obtener el enlace embebido
                try:
                    embed_movie = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'embed-movie'))
                    )
                    iframe = embed_movie.find_element(By.TAG_NAME, 'iframe')
                    embedded_link = iframe.get_attribute('src')
                    logger.debug(f"Worker {worker_id} (usuario {username}): Enlace embebido extraído: {embedded_link}")
                except (TimeoutException, Exception) as e:
                    logger.error(f"Worker {worker_id} (usuario {username}): Error al obtener el enlace embebido: {e}")
                    continue

                # Modificar el enlace si es powvideo o streamplay
                if embedded_link and server in ["powvideo", "streamplay"]:
                    embedded_link = re.sub(r"embed-([^-]+)-\d+x\d+\.html", r"\1", embedded_link)

                # Determinar la calidad en función del servidor
                quality = '1080p' if server in ['streamtape', 'vidmoly', 'mixdrop'] else 'hdrip'

                # Añadir enlace a la lista
                if server and language and embedded_link:
                    server_links.append({
                        "server": server,
                        "language": language,
                        "link": embedded_link,
                        "quality": quality
                    })
            except Exception as e:
                logger.error(
                    f"Worker {worker_id} (usuario {username}): Error al procesar el enlace para el episodio {episode_number}: {e}")
                continue

        logger.debug(
            f"Worker {worker_id} (usuario {username}): Enlaces obtenidos para el episodio {episode_number}: {len(server_links)}")

        # Solo devolver los detalles si se encontraron enlaces
        if server_links:
            return {
                "episode_number": episode_number,
                "title": title,
                "url": episode_url,
                "links": server_links
            }
        else:
            logger.warning(
                f"Worker {worker_id} (usuario {username}): No se encontraron enlaces para el episodio {episode_number}. Saltando...")
            return None
    except Exception as e:
        logger.error(
            f"Worker {worker_id} (usuario {username}): Error al extraer detalles del episodio {episode_url}: {e}")
        raise


# Función para extraer detalles de la temporada
def extract_season_details(driver, season_url, season_number, worker_id, username):
    logger.info(
        f"Worker {worker_id} (usuario {username}): Extrayendo detalles de la temporada {season_number}: {season_url}")
    try:
        driver.get(season_url)
        time.sleep(1)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Verificar si hay episodios en la temporada
        episode_elements = soup.find_all("div", class_="span-6 tt view show-view")
        if not episode_elements:
            logger.warning(
                f"Worker {worker_id} (usuario {username}): No se encontraron episodios en la temporada {season_number}: {season_url}")
            return []

        logger.info(
            f"Worker {worker_id} (usuario {username}): Episodios encontrados en la temporada {season_number}: {len(episode_elements)}")

        # Crear lista para almacenar los episodios
        episodes = []

        # Limitar el número de episodios a procesar para evitar bloqueos
        max_episodes = min(len(episode_elements), 5)  # Procesar máximo 5 episodios por temporada

        for i, episode_element in enumerate(episode_elements[:max_episodes]):
            try:
                episode_number_tag = episode_element.find("div", class_="rating")
                episode_number_text = episode_number_tag.text.strip() if episode_number_tag else None
                if episode_number_text:
                    episode_number = int(episode_number_text.split("x")[1])
                else:
                    logger.warning(
                        f"Worker {worker_id} (usuario {username}): No se pudo extraer el número de episodio. Saltando...")
                    continue

                title_tag = episode_element.find("a", class_="link title-ellipsis")
                title = title_tag['title'].split(' - ')[-1] if title_tag else "No encontrado"
                episode_url = base_url + title_tag['href']

                # Intentar extraer detalles del episodio
                episode_data = extract_episode_details(driver, episode_url, episode_number, title, worker_id, username)
                if episode_data:
                    episodes.append(episode_data)

            except Exception as e:
                logger.error(f"Worker {worker_id} (usuario {username}): Error al procesar episodio: {e}")
                continue

        return episodes
    except Exception as e:
        logger.error(
            f"Worker {worker_id} (usuario {username}): Error al extraer detalles de la temporada {season_url}: {e}")
        raise


# Función para verificar si hay una siguiente temporada
def has_next_season(driver, season_url, worker_id, username):
    try:
        logger.info(f"Worker {worker_id} (usuario {username}): Verificando si existe la temporada: {season_url}")
        driver.get(season_url)
        time.sleep(1)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")
        next_season_elements = soup.find_all("div", class_="span-6 tt view show-view")
        exists = len(next_season_elements) > 0
        logger.debug(
            f"Worker {worker_id} (usuario {username}): Temporada {season_url}: {'Existe' if exists else 'No existe'}")
        return exists
    except Exception as e:
        logger.error(
            f"Worker {worker_id} (usuario {username}): Error al verificar si existe la temporada {season_url}: {e}")
        return False


# Función para extraer detalles de la serie
def extract_series_details(driver, series_url, worker_id, username):
    logger.info(f"Worker {worker_id} (usuario {username}): Extrayendo datos de la serie: {series_url}")
    try:
        driver.get(series_url)
        time.sleep(1)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Extraer el título de la serie
        title_tag = soup.find("div", id="summary-title")
        title = title_tag.text.strip() if title_tag else "No encontrado"
        logger.info(f"Worker {worker_id} (usuario {username}): Título de la serie: {title}")

        # Extraer año, IMDB rating y género de la serie
        show_details = soup.find("div", class_="show-details")
        year = None
        imdb_rating = None
        genre = None

        if show_details:
            year_tag = show_details.find("a", href=re.compile(r"/buscar/year/"))
            if year_tag:
                year = int(year_tag.text.strip())
            logger.debug(f"Worker {worker_id} (usuario {username}): Año de la serie: {year}")

            imdb_rating_tag = show_details.find("p", itemprop="aggregateRating")
            if imdb_rating_tag and imdb_rating_tag.find("a"):
                imdb_rating = float(imdb_rating_tag.find("a").text.strip())
            logger.debug(f"Worker {worker_id} (usuario {username}): IMDB Rating de la serie: {imdb_rating}")

            genre_tags = show_details.find_all("a", href=re.compile(r"/tags-tv"))
            genre = ", ".join([tag.text.strip() for tag in genre_tags])
            logger.debug(f"Worker {worker_id} (usuario {username}): Género de la serie: {genre}")

        # Verificar si la serie ya existe
        series_exists_flag, existing_series_id = series_exists(title, year, imdb_rating, genre)
        if series_exists_flag:
            logger.info(
                f"Worker {worker_id} (usuario {username}): La serie '{title}' ({year}) ya existe en la base de datos. Saltando...")
            return None

        # Crear lista para almacenar las temporadas
        seasons_data = []

        # Limitar el número de temporadas a procesar para evitar bloqueos
        max_seasons = 2  # Procesar máximo 2 temporadas por serie

        for season_number in range(1, max_seasons + 1):
            season_url = f"{series_url}/temporada-{season_number}"
            logger.info(f"Worker {worker_id} (usuario {username}): Verificando temporada {season_number}: {season_url}")

            if not has_next_season(driver, season_url, worker_id, username):
                logger.info(
                    f"Worker {worker_id} (usuario {username}): No se encontró la temporada {season_number}. Finalizando.")
                break

            # Extraer detalles de la temporada
            try:
                episodes = extract_season_details(driver, season_url, season_number, worker_id, username)
                if episodes:
                    seasons_data.append({
                        "season_number": season_number,
                        "episodes": episodes
                    })
                    logger.info(
                        f"Worker {worker_id} (usuario {username}): Temporada {season_number} extraída con {len(episodes)} episodios")
                else:
                    logger.warning(
                        f"Worker {worker_id} (usuario {username}): No se encontraron episodios en la temporada {season_number}")
            except Exception as e:
                logger.error(
                    f"Worker {worker_id} (usuario {username}): Error al extraer la temporada {season_number}: {e}")
                continue

        # Solo devolver los detalles si se encontraron temporadas con episodios
        if seasons_data:
            logger.info(
                f"Worker {worker_id} (usuario {username}): Serie '{title}' extraída con {len(seasons_data)} temporadas")
            return {
                "title": title,
                "year": year,
                "imdb_rating": imdb_rating,
                "genre": genre,
                "seasons": seasons_data
            }
        else:
            logger.warning(
                f"Worker {worker_id} (usuario {username}): No se encontraron temporadas o episodios para la serie: {title}. Saltando...")
            return None
    except Exception as e:
        logger.error(
            f"Worker {worker_id} (usuario {username}): Error al extraer detalles de la serie {series_url}: {e}")
        raise


# Función para insertar la serie, temporadas y episodios en una sola transacción
def insert_series_with_seasons_and_episodes(series_data, worker_id, username):
    if not series_data:
        logger.debug(f"Worker {worker_id} (usuario {username}): No hay datos de serie para insertar")
        return False

    with db_lock:
        connection = connect_db()
        cursor = connection.cursor()
        series_id = None

        try:
            # Iniciar transacción
            connection.execute("BEGIN TRANSACTION")

            # Insertar la serie
            cursor.execute('''
                INSERT INTO media_downloads (title, year, imdb_rating, genre, type)
                VALUES (?, ?, ?, ?, 'serie')
            ''', (series_data["title"], series_data["year"], series_data["imdb_rating"], series_data["genre"]))
            series_id = cursor.lastrowid

            if not series_id:
                logger.error(
                    f"Worker {worker_id} (usuario {username}): Error al insertar la serie: {series_data['title']}")
                connection.rollback()
                return False

            logger.info(
                f"Worker {worker_id} (usuario {username}): Serie insertada: {series_data['title']} con ID: {series_id}")

            # Insertar temporadas y episodios
            for season in series_data["seasons"]:
                season_number = season["season_number"]

                # Insertar temporada
                cursor.execute('''
                    INSERT INTO series_seasons (movie_id, season)
                    VALUES (?, ?)
                ''', (series_id, season_number))
                season_id = cursor.lastrowid

                if not season_id:
                    logger.error(
                        f"Worker {worker_id} (usuario {username}): Error al insertar la temporada {season_number} para la serie ID: {series_id}")
                    connection.rollback()
                    return False

                logger.info(
                    f"Worker {worker_id} (usuario {username}): Temporada insertada: {season_number} para serie ID: {series_id} con temporada ID: {season_id}")

                # Insertar episodios y enlaces
                for episode in season["episodes"]:
                    episode_number = episode["episode_number"]
                    episode_title = episode["title"]

                    # Insertar episodio
                    cursor.execute('''
                        INSERT INTO series_episodes (season_id, episode, title)
                        VALUES (?, ?, ?)
                    ''', (season_id, episode_number, episode_title))
                    episode_id = cursor.lastrowid

                    if not episode_id:
                        logger.error(
                            f"Worker {worker_id} (usuario {username}): Error al insertar el episodio {episode_number} para la temporada ID: {season_id}")
                        connection.rollback()
                        return False

                    logger.info(
                        f"Worker {worker_id} (usuario {username}): Episodio insertado: {episode_number} - {episode_title} con ID: {episode_id}")

                    # Insertar enlaces del episodio
                    for link in episode["links"]:
                        # Insertar el servidor si no existe
                        cursor.execute('''
                            INSERT OR IGNORE INTO servers (name) VALUES (?)
                        ''', (link["server"],))
                        cursor.execute('''
                            SELECT id FROM servers WHERE name=?
                        ''', (link["server"],))
                        server_id = cursor.fetchone()["id"]

                        # Insertar la calidad si no existe
                        cursor.execute('''
                            SELECT quality_id FROM qualities WHERE quality=?
                        ''', (link["quality"],))
                        quality_result = cursor.fetchone()

                        if not quality_result:
                            cursor.execute('''
                                INSERT INTO qualities (quality) VALUES (?)
                            ''', (link["quality"],))
                            cursor.execute('''
                                SELECT quality_id FROM qualities WHERE quality=?
                            ''', (link["quality"],))
                            quality_result = cursor.fetchone()

                        quality_id = quality_result["quality_id"]

                        # Insertar el enlace
                        cursor.execute('''
                            INSERT INTO links_files_download (episode_id, server_id, language, link, quality_id)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (episode_id, server_id, link["language"], link["link"], quality_id))

                        logger.debug(
                            f"Worker {worker_id} (usuario {username}): Enlace insertado: episode_id={episode_id}, server={link['server']}, language={link['language']}")

            # Confirmar la transacción
            connection.commit()
            logger.info(
                f"Worker {worker_id} (usuario {username}): Serie completa insertada con éxito: {series_data['title']} con ID: {series_id}")
            return True
        except Exception as e:
            logger.error(
                f"Worker {worker_id} (usuario {username}): Error al insertar la serie con sus temporadas y episodios: {e}")
            connection.rollback()
            return False
        finally:
            cursor.close()
            connection.close()


# Función para guardar el progreso
def save_progress(letter_index, pending_urls=None, completed_urls=None):
    with progress_lock:
        try:
            # Cargar datos existentes si el archivo existe
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    progress_data = json.load(f)
            else:
                progress_data = {
                    'letter_index': 0,
                    'pending_urls': [],
                    'completed_urls': [],
                    'last_update': '',
                    'restart_count': 0
                }

            # Actualizar datos
            progress_data['letter_index'] = letter_index

            if pending_urls is not None:
                progress_data['pending_urls'] = pending_urls

            if completed_urls is not None:
                progress_data['completed_urls'] = completed_urls

            progress_data['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')

            # Guardar los datos actualizados
            with open(progress_file, 'w') as f:
                json.dump(progress_data, f)

            logger.debug(
                f"Progreso guardado: índice letra {letter_index}, URLs pendientes: {len(pending_urls) if pending_urls else 0}, URLs completadas: {len(completed_urls) if completed_urls else 0}")
        except Exception as e:
            logger.error(f"Error al guardar el progreso: {e}")

# Función para cargar el progreso
def load_progress():
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                logger.info(f"Progreso cargado: índice letra {progress['letter_index']}")

                # Cargar URLs pendientes y completadas
                pending_urls = progress.get('pending_urls', [])
                completed_urls = progress.get('completed_urls', [])

                logger.info(f"URLs pendientes cargadas: {len(pending_urls)}")
                logger.info(f"URLs completadas cargadas: {len(completed_urls)}")

                if 'restart_count' in progress:
                    global restart_count
                    restart_count = progress['restart_count']
                logger.info(f"Contador de reinicios cargado: {restart_count}/{MAX_RESTARTS}")

                return progress['letter_index'], pending_urls, completed_urls
    except Exception as e:
        logger.error(f"Error al cargar el progreso: {e}")

    logger.info("No se encontró archivo de progreso o hubo un error. Comenzando desde el principio.")
    return 0, [], []


# Función para extraer URLs de series de una página
def extract_series_urls_from_page(driver, page_url, letter):
    logger.info(f"Extrayendo URLs de series de la página: {page_url}")
    try:
        driver.get(page_url)
        time.sleep(1)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Buscar todos los divs de las series
        series_divs = soup.find_all("div", class_="span-6 inner-6 tt view")
        logger.info(f"Encontradas {len(series_divs)} series en la página {page_url}")

        # Guardar las URLs de las series
        series_urls = []
        for i, div in enumerate(series_divs):
            try:
                link_tag = div.find("a")
                if link_tag and link_tag.has_attr('href'):
                    series_url = base_url + link_tag['href']
                    series_urls.append((i, series_url))
            except Exception as e:
                logger.error(f"Error al obtener la URL de la serie en el índice {i}: {e}")

        return series_urls
    except Exception as e:
        logger.error(f"Error al extraer URLs de series de la página {page_url}: {e}")
        return []


# Función worker para procesar series con una cuenta específica
def series_worker(worker_id):
    # Asignar credenciales específicas a este worker
    worker_index = (worker_id - 1) % len(user_credentials)
    credentials = user_credentials[worker_index]
    username = credentials["username"]
    password = credentials["password"]

    # Crear un driver para este worker
    driver = create_driver()

    # Iniciar sesión con este driver usando las credenciales asignadas
    if not login(driver, username, password):
        logger.error(f"Worker {worker_id}: No se pudo iniciar sesión con usuario {username}. Abortando...")
        driver.quit()
        return

    logger.info(f"Worker {worker_id}: Iniciado con usuario {username} y listo para procesar series")

    try:
        while True:
            try:
                # Obtener una URL de serie de la cola
                index, series_url = series_queue.get(block=False)

                logger.info(f"Worker {worker_id} (usuario {username}): Procesando serie {index}: {series_url}")

                # Intentar extraer los detalles de la serie con reintentos
                success = False
                series_details = None

                # Primer conjunto de 3 intentos
                for attempt in range(3):
                    try:
                        series_details = extract_series_details(driver, series_url, worker_id, username)
                        success = True
                        break  # Salir del bucle de reintentos si tiene éxito
                    except Exception as e:
                        logger.error(
                            f"Worker {worker_id} (usuario {username}): Error al extraer datos de la serie {series_url}: {e}")
                        if attempt < 2:
                            logger.info(
                                f"Worker {worker_id} (usuario {username}): Reintentando en 5 segundos... (Intento {attempt + 1}/3)")
                            time.sleep(5)

                # Si después de 3 intentos no hay éxito, reiniciar el driver y probar 3 veces más
                if not success:
                    logger.info(
                        f"Worker {worker_id} (usuario {username}): Reiniciando el driver después de 3 intentos fallidos...")
                    driver.quit()
                    all_drivers.remove(driver)  # Eliminar de la lista global
                    driver = create_driver()
                    if login(driver, username, password):
                        # Segundo conjunto de 3 intentos después de reiniciar el driver
                        for attempt in range(3):
                            try:
                                series_details = extract_series_details(driver, series_url, worker_id, username)
                                success = True
                                break  # Salir del bucle de reintentos si tiene éxito
                            except Exception as e:
                                logger.error(
                                    f"Worker {worker_id} (usuario {username}): Error al extraer datos de la serie {series_url} después de reiniciar: {e}")
                                if attempt < 2:
                                    logger.info(
                                        f"Worker {worker_id} (usuario {username}): Reintentando en 5 segundos... (Intento {attempt + 1}/3 después de reiniciar)")
                                    time.sleep(5)
                    else:
                        logger.error(
                            f"Worker {worker_id} (usuario {username}): No se pudo iniciar sesión después de reiniciar el driver.")

                # Si se obtuvieron los detalles de la serie, insertarlos en la base de datos
                if success and series_details:
                    insert_success = insert_series_with_seasons_and_episodes(series_details, worker_id, username)

                    # Actualizar listas de URLs procesadas
                    with progress_lock:
                        # Cargar progreso actual
                        if os.path.exists(progress_file):
                            with open(progress_file, 'r') as f:
                                progress_data = json.load(f)
                                pending_urls = progress_data.get('pending_urls', [])
                                completed_urls = progress_data.get('completed_urls', [])
                        else:
                            pending_urls = []
                            completed_urls = []

                        # Actualizar listas
                        if series_url in pending_urls:
                            pending_urls.remove(series_url)

                        if insert_success and series_url not in completed_urls:
                            completed_urls.append(series_url)

                        # Guardar progreso actualizado
                        with open(progress_file, 'w') as f:
                            progress_data = {
                                'letter_index': progress_data.get('letter_index', 0),
                                'pending_urls': pending_urls,
                                'completed_urls': completed_urls,
                                'last_update': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'restart_count': restart_count
                            }
                            json.dump(progress_data, f)

                # Marcar la tarea como completada
                series_queue.task_done()

            except Exception as e:
                if "queue.Empty" in str(e.__class__):
                    # La cola está vacía, esperar un poco y volver a intentar
                    time.sleep(1)
                    # Si la cola sigue vacía después de esperar, salir del bucle
                    if series_queue.empty():
                        logger.info(
                            f"Worker {worker_id} (usuario {username}): No hay más series para procesar. Finalizando.")
                        break
                else:
                    logger.error(f"Worker {worker_id} (usuario {username}): Error inesperado: {e}")
                    time.sleep(1)
    finally:
        # Cerrar el driver al finalizar
        try:
            driver.quit()
            if driver in all_drivers:
                all_drivers.remove(driver)
        except:
            pass
        logger.info(f"Worker {worker_id} (usuario {username}): Finalizado y driver cerrado.")


# Función principal para extraer todas las series
def extract_all_series():
    # Inicializar la base de datos si es necesario
    initialize_db()

    # Crear un driver principal para la navegación por páginas
    main_driver = create_driver()

    # Usar la primera cuenta para el driver principal
    main_credentials = user_credentials[0]
    if not login(main_driver, main_credentials["username"], main_credentials["password"]):
        logger.error(
            f"No se pudo iniciar sesión con el driver principal usando {main_credentials['username']}. Abortando...")
        main_driver.quit()
        all_drivers.remove(main_driver)
        return

    # Lista de letras del alfabeto para recorrer
    alphabet = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ#")

    # Cargar el progreso guardado
    letter_index, pending_urls, completed_urls = load_progress()

    try:
        # Crear un pool de workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            # Iniciar los workers
            workers = [executor.submit(series_worker, i + 1) for i in range(NUM_WORKERS)]

            # Si hay URLs pendientes de procesamiento, añadirlas primero a la cola
            if pending_urls:
                logger.info(f"Añadiendo {len(pending_urls)} URLs pendientes a la cola...")
                for i, url in enumerate(pending_urls):
                    series_queue.put((i, url))
                    logger.debug(f"Añadida serie pendiente {i} a la cola: {url}")

                # Esperar a que se procesen las URLs pendientes
                logger.info("Esperando a que se procesen las URLs pendientes...")
                start_time = time.time()
                max_wait_time = 600  # 10 minutos máximo de espera

                while not series_queue.empty():
                    if time.time() - start_time > max_wait_time:
                        logger.warning(
                            "Tiempo de espera excedido para las URLs pendientes. Continuando con el proceso normal.")
                        break
                    time.sleep(5)  # Esperar 5 segundos y verificar de nuevo

            # Procesar letras una por una
            for i in range(letter_index, len(alphabet)):
                letter = alphabet[i]
                page_url = f"{series_url}{letter}"
                logger.info(f"Procesando letra {letter}: {page_url}")

                # Extraer URLs de series de la página actual
                series_urls = extract_series_urls_from_page(main_driver, page_url, letter)

                # Si no hay series en esta página, pasar a la siguiente letra
                if not series_urls:
                    logger.info(f"No se encontraron series para la letra {letter}. Pasando a la siguiente.")
                    # Actualizar el índice de letra en el archivo de progreso
                    save_progress(i + 1, pending_urls, completed_urls)
                    continue

                # Filtrar URLs ya completadas
                new_urls = []
                for index, url in series_urls:
                    if url not in completed_urls and url not in pending_urls:
                        new_urls.append((index, url))
                        pending_urls.append(url)  # Añadir a la lista de pendientes

                logger.info(f"Encontradas {len(new_urls)} nuevas series para procesar con la letra {letter}")

                # Añadir las URLs a la cola para que los workers las procesen
                for index, url in new_urls:
                    series_queue.put((index, url))
                    logger.debug(f"Añadida serie {index} a la cola: {url}")

                # Guardar el progreso después de añadir todas las URLs a la cola
                save_progress(i, pending_urls, completed_urls)

                # Esperar a que la cola esté vacía antes de pasar a la siguiente letra
                # pero con un timeout para evitar bloqueos
                start_time = time.time()
                max_wait_time = 300  # 5 minutos máximo de espera

                while not series_queue.empty():
                    if time.time() - start_time > max_wait_time:
                        logger.warning(
                            f"Tiempo de espera excedido para la letra {letter}. Continuando con la siguiente letra.")
                        break
                    time.sleep(5)  # Esperar 5 segundos y verificar de nuevo

                # Actualizar las listas de URLs pendientes y completadas
                with progress_lock:
                    if os.path.exists(progress_file):
                        with open(progress_file, 'r') as f:
                            progress_data = json.load(f)
                            pending_urls = progress_data.get('pending_urls', [])
                            completed_urls = progress_data.get('completed_urls', [])

                # Actualizar el índice de letra en el archivo de progreso
                save_progress(i + 1, pending_urls, completed_urls)

                logger.info(f"Procesamiento de la letra {letter} completado o tiempo de espera excedido.")

            # Esperar a que se completen todas las tareas restantes
            logger.info("Esperando a que se completen todas las tareas restantes...")
            start_time = time.time()
            max_wait_time = 300  # 5 minutos máximo de espera

            while not series_queue.empty():
                if time.time() - start_time > max_wait_time:
                    logger.warning("Tiempo de espera excedido para las tareas restantes. Finalizando.")
                    break
                time.sleep(5)  # Esperar 5 segundos y verificar de nuevo

            # Cancelar los workers
            for worker in workers:
                worker.cancel()

            logger.info("Todos los workers han finalizado o han sido cancelados.")

    except Exception as e:
        logger.critical(f"Error crítico en el proceso principal: {e}")
        # Guardar el progreso antes de salir
        save_progress(letter_index, pending_urls, completed_urls)
    finally:
        # Cerrar el driver principal
        try:
            main_driver.quit()
            if main_driver in all_drivers:
                all_drivers.remove(main_driver)
        except:
            pass
        logger.info("Driver principal cerrado.")


# Punto de entrada principal
if __name__ == "__main__":
    try:
        logger.info("Iniciando el scraper de series con procesamiento paralelo y múltiples cuentas...")
        # Verificar si estamos en un reinicio
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                if 'restart_count' in progress:
                    restart_count = progress['restart_count']
                    logger.info(f"Reinicio detectado. Contador de reinicios: {restart_count}/{MAX_RESTARTS}")

        # Ejecutar la extracción de todas las series
        extract_all_series()
        logger.info("Proceso de scraping de series completado.")
    except Exception as e:
        logger.critical(f"Error crítico en el scraper: {e}")
        # Intentar guardar el progreso antes de salir
        try:
            letter_index, pending_urls, completed_urls = load_progress()
            save_progress(letter_index, pending_urls, completed_urls)
        except:
            pass
    finally:
        # Asegurarse de que todos los drivers se cierren
        close_all_drivers()
        logger.info("Scraper finalizado.")