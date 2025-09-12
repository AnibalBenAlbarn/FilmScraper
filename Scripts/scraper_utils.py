import time
import re
import sqlite3
import json
import os
import logging
import smtplib
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# Configuración global
BASE_URL = "https://hdfull.blog"
LOGIN_URL = f"{BASE_URL}/login"
USERNAME = 'rolankor'
PASSWORD = 'Rolankor_09'

# Configuración de paralelización
MAX_WORKERS = 4  # Número máximo de workers para procesamiento paralelo
MAX_RETRIES = 3  # Número máximo de reintentos para cada elemento

# Ruta del proyecto
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configuración de la base de datos
DB_PATH = os.path.join(PROJECT_ROOT, "Scripts", "direct_dw_db.db")

# Archivo de configuración para persistir la ruta de la base de datos
CONFIG_FILE = os.path.join(PROJECT_ROOT, "db_config.json")

# Cargar la ruta almacenada si existe
if os.path.exists(CONFIG_FILE):
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        DB_PATH = data.get('db_path', DB_PATH)
    except Exception:
        pass

# Configuración de caché para reducir consultas repetidas
CACHE_ENABLED = True
CACHE = {
    'servers': {},
    'qualities': {},
    'movies': {},
    'seasons': {},
    'episodes': {}
}


def set_db_path(path):
    """Actualiza y persiste la ruta de la base de datos."""
    global DB_PATH
    DB_PATH = os.path.abspath(path)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump({'db_path': DB_PATH}, f, indent=2)
    except Exception as e:
        logging.getLogger(__name__).error(f"Error guardando la ruta de la base de datos: {e}")
    logging.getLogger(__name__).info(f"Ruta de base de datos establecida en: {DB_PATH}")


def set_max_workers(value):
    """Actualiza el número máximo de workers."""
    global MAX_WORKERS
    MAX_WORKERS = value
    logging.getLogger(__name__).debug(f"MAX_WORKERS establecido en {value}")


def set_max_retries(value):
    """Actualiza el número máximo de reintentos."""
    global MAX_RETRIES
    MAX_RETRIES = value
    logging.getLogger(__name__).debug(f"MAX_RETRIES establecido en {value}")


def toggle_cache():
    """Activa o desactiva el uso de caché."""
    global CACHE_ENABLED
    CACHE_ENABLED = not CACHE_ENABLED
    logging.getLogger(__name__).debug(f"CACHE_ENABLED ahora es {CACHE_ENABLED}")


# Configuración del logger
def setup_logger(name, log_file, level=logging.INFO):
    """Configura y devuelve un logger con el nombre y archivo especificados."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Nivel base para el logger

    # Crear directorio de logs si no existe
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)

    # Crear handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler.setLevel(level)

    # Crear formato y agregarlo a los handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Limpiar handlers previos y añadir los nuevos
    logger.handlers = []
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# Función para crear un driver de Selenium con configuración optimizada
def create_driver(headless=True):
    """Crea y devuelve un driver de Selenium con configuración optimizada."""
    options = webdriver.ChromeOptions()

    if headless:
        options.add_argument("--headless=new")

    # Reducir el uso de memoria
    options.add_argument("--js-flags=--max-old-space-size=512")

    # Configurar el tamaño de la ventana para consistencia
    options.add_argument("--window-size=1366,768")

    # Otras opciones útiles
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Usar webdriver-manager para gestionar el chromedriver automáticamente
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        # Fallback a la ruta local si webdriver-manager falla
        service = Service(os.path.join(PROJECT_ROOT, "chromedriver.exe"))
        driver = webdriver.Chrome(service=service, options=options)

    # Configurar timeouts
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)

    return driver


# Función para conectar a la base de datos con optimizaciones
def connect_db(db_path=None):
    """Conecta a la base de datos SQLite con configuración optimizada."""
    if db_path is None:
        db_path = DB_PATH

    try:
        logging.getLogger(__name__).debug(f"Conectando a la base de datos en: {db_path}")
        connection = sqlite3.connect(db_path, timeout=30)
        connection.row_factory = sqlite3.Row

        # Optimizaciones para SQLite
        connection.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging para mejor concurrencia
        connection.execute("PRAGMA synchronous = NORMAL")  # Menos sincronización para mejor rendimiento
        connection.execute("PRAGMA cache_size = 10000")  # Aumentar caché
        connection.execute("PRAGMA temp_store = MEMORY")  # Almacenar tablas temporales en memoria

        return connection
    except Exception as e:
        raise Exception(f"Error al conectar a la base de datos: {e}")


# Función para iniciar sesión con manejo de errores mejorado
def login(driver, logger):
    """Inicia sesión en el sitio web con manejo de errores mejorado."""
    try:
        logger.info("Iniciando sesión...")
        driver.get(LOGIN_URL)

        # Esperar a que la página cargue completamente
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )

        # Llenar formulario de login
        username_field = driver.find_element(By.NAME, "username")
        password_field = driver.find_element(By.NAME, "password")

        # Limpiar campos antes de enviar texto
        username_field.clear()
        password_field.clear()

        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)

        # Buscar el botón de login y hacer clic
        try:
            login_button = driver.find_element(By.XPATH, "//a[text()='Ingresar']")
            login_button.click()
        except Exception:
            # Intentar con otro selector si el primero falla
            login_button = driver.find_element(By.CSS_SELECTOR, ".btn.btn-primary")
            login_button.click()

        # Verificar que el login fue exitoso
        WebDriverWait(driver, 10).until(EC.url_changes(LOGIN_URL))

        # Verificación adicional: buscar elementos que indiquen login exitoso
        try:
            WebDriverWait(driver, 10).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CLASS_NAME, "user-menu")),
                    EC.presence_of_element_located((By.CLASS_NAME, "username")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".nav-profile-name"))
                )
            )
        except TimeoutException:
            logger.warning("No se encontraron indicadores claros de login exitoso, pero la URL cambió")

        logger.info("Sesión iniciada correctamente")
        return True
    except Exception as e:
        logger.error(f"Error al iniciar sesión: {e}")
        logger.debug(traceback.format_exc())
        return False


# Función para verificar y crear tablas necesarias
def setup_database(logger, db_path=None):
    """Configura la base de datos, creando tablas si no existen y añadiendo columnas necesarias."""
    if db_path is None:
        db_path = DB_PATH
    logger.debug(f"Iniciando configuración de la base de datos en: {db_path}")
    connection = connect_db(db_path)
    cursor = connection.cursor()

    try:
        # Verificar si existe la tabla de estadísticas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes REAL,
                updated_movies INTEGER,
                new_links INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Verificar si existe la tabla de estadísticas de episodios
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS episode_update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes REAL,
                new_series INTEGER,
                new_seasons INTEGER,
                new_episodes INTEGER,
                new_links INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Verificar si las columnas created_at y updated_at existen en media_downloads
        cursor.execute("PRAGMA table_info(media_downloads)")
        columns = {row['name'] for row in cursor.fetchall()}

        if 'created_at' not in columns:
            cursor.execute("ALTER TABLE media_downloads ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        if 'updated_at' not in columns:
            cursor.execute("ALTER TABLE media_downloads ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        # Verificar si la columna created_at existe en links_files_download
        cursor.execute("PRAGMA table_info(links_files_download)")
        columns = {row['name'] for row in cursor.fetchall()}

        if 'created_at' not in columns:
            cursor.execute("ALTER TABLE links_files_download ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        # Crear índices para mejorar el rendimiento de las consultas
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_downloads_title ON media_downloads(title, type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_series_seasons_movie_id ON series_seasons(movie_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_series_episodes_season_id ON series_episodes(season_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_movie_id ON links_files_download(movie_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_episode_id ON links_files_download(episode_id)")

        connection.commit()
        logger.info("Base de datos configurada correctamente")
        return True
    except Exception as e:
        logger.error(f"Error al configurar la base de datos: {e}")
        logger.debug(traceback.format_exc())
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()


# Función para guardar el progreso
def save_progress(progress_file, data):
    """Guarda el progreso de la ejecución en un archivo JSON."""
    try:
        # Asegurarse de que el directorio existe
        os.makedirs(os.path.dirname(progress_file), exist_ok=True)

        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"Error al guardar progreso: {e}")
        return False


# Función para cargar el progreso
def load_progress(progress_file, default=None):
    """Carga el progreso de la ejecución desde un archivo JSON."""
    if default is None:
        default = {}

    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return default
    except Exception as e:
        print(f"Error al cargar progreso: {e}")
        return default


# Función para obtener o crear un servidor en la base de datos
def get_or_create_server(server_name, connection=None, db_path=None):
    """Obtiene o crea un servidor en la base de datos."""
    # Usar caché si está habilitada
    if CACHE_ENABLED and server_name in CACHE['servers']:
        return CACHE['servers'][server_name]

    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        # Intentar obtener el servidor
        cursor.execute('SELECT id FROM servers WHERE name=?', (server_name,))
        result = cursor.fetchone()

        if result:
            server_id = result['id']
        else:
            # Crear el servidor si no existe
            cursor.execute('INSERT INTO servers (name) VALUES (?)', (server_name,))
            connection.commit()
            server_id = cursor.lastrowid

        # Guardar en caché
        if CACHE_ENABLED:
            CACHE['servers'][server_name] = server_id

        return server_id
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para obtener o crear una calidad en la base de datos
def get_or_create_quality(quality_name, connection=None, db_path=None):
    """Obtiene o crea una calidad en la base de datos."""
    # Usar caché si está habilitada
    if CACHE_ENABLED and quality_name in CACHE['qualities']:
        return CACHE['qualities'][quality_name]

    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        # Intentar obtener la calidad
        cursor.execute('SELECT quality_id FROM qualities WHERE quality=?', (quality_name,))
        result = cursor.fetchone()

        if result:
            quality_id = result['quality_id']
        else:
            # Crear la calidad si no existe
            cursor.execute('INSERT INTO qualities (quality) VALUES (?)', (quality_name,))
            connection.commit()
            quality_id = cursor.lastrowid

        # Guardar en caché
        if CACHE_ENABLED:
            CACHE['qualities'][quality_name] = quality_id

        return quality_id
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para verificar si un enlace ya existe
def link_exists(movie_id=None, episode_id=None, server_id=None, language=None, link=None, connection=None,
                db_path=None):
    """Verifica si un enlace ya existe en la base de datos."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        if movie_id:
            cursor.execute('''
                SELECT id FROM links_files_download 
                WHERE movie_id=? AND server_id=? AND language=? AND link=?
            ''', (movie_id, server_id, language, link))
        elif episode_id:
            cursor.execute('''
                SELECT id FROM links_files_download 
                WHERE episode_id=? AND server_id=? AND language=? AND link=?
            ''', (episode_id, server_id, language, link))
        else:
            return False

        result = cursor.fetchone()
        return result is not None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para insertar enlaces en lote
def insert_links_batch(links, logger, connection=None, db_path=None):
    """Inserta múltiples enlaces en la base de datos de forma eficiente."""
    if not links:
        return 0

    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()
    inserted_count = 0

    try:
        # Preparar todos los servidores y calidades de una vez
        servers = {}
        qualities = {}

        for link in links:
            if link["server"] not in servers:
                servers[link["server"]] = get_or_create_server(link["server"], connection, db_path)

            if link["quality"] not in qualities:
                qualities[link["quality"]] = get_or_create_quality(link["quality"], connection, db_path)

        # Insertar enlaces en lote
        for link in links:
            server_id = servers[link["server"]]
            quality_id = qualities[link["quality"]]

            # Verificar si el enlace ya existe
            exists = False
            if "movie_id" in link and link["movie_id"]:
                exists = link_exists(movie_id=link["movie_id"], server_id=server_id,
                                     language=link["language"], link=link["link"], connection=connection,
                                     db_path=db_path)
            elif "episode_id" in link and link["episode_id"]:
                exists = link_exists(episode_id=link["episode_id"], server_id=server_id,
                                     language=link["language"], link=link["link"], connection=connection,
                                     db_path=db_path)

            if not exists:
                if "movie_id" in link and link["movie_id"]:
                    cursor.execute('''
                        INSERT INTO links_files_download 
                        (movie_id, server_id, language, link, quality_id, created_at)
                        VALUES (?, ?, ?, ?, ?, datetime('now'))
                    ''', (link["movie_id"], server_id, link["language"], link["link"], quality_id))
                elif "episode_id" in link and link["episode_id"]:
                    cursor.execute('''
                        INSERT INTO links_files_download 
                        (episode_id, server_id, language, link, quality_id, created_at)
                        VALUES (?, ?, ?, ?, ?, datetime('now'))
                    ''', (link["episode_id"], server_id, link["language"], link["link"], quality_id))

                inserted_count += 1

        connection.commit()
        return inserted_count
    except Exception as e:
        logger.error(f"Error al insertar enlaces en lote: {e}")
        logger.debug(traceback.format_exc())
        connection.rollback()
        return 0
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para extraer enlaces de una página
def extract_links(driver, movie_id=None, episode_id=None, logger=None):
    """Extrae enlaces de una página de película o episodio."""
    server_links = []

    try:
        # Encontrar todos los embed-selectors
        embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
        if logger:
            logger.debug(f"Número de enlaces encontrados: {len(embed_selectors)}")

        for i, embed_selector in enumerate(embed_selectors):
            language = None
            server = None
            embedded_link = None

            try:
                # Extraer idioma y servidor antes de hacer clic
                embed_html = embed_selector.get_attribute('outerHTML')
                embed_soup = BeautifulSoup(embed_html, "html.parser")

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
                try:
                    embed_selector.click()
                    time.sleep(1)  # Esperar a que se cargue el contenido
                except StaleElementReferenceException:
                    # Si el elemento está obsoleto, refrescar y volver a intentar
                    if logger:
                        logger.warning(
                            f"Elemento obsoleto al hacer clic en el enlace {i + 1}. Refrescando elementos...")
                    embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
                    if i < len(embed_selectors):
                        embed_selector = embed_selectors[i]
                        embed_selector.click()
                        time.sleep(1)
                    else:
                        continue
            except Exception as e:
                if logger:
                    logger.error(f"Error al hacer clic en el embed-selector {i + 1}: {e}")
                continue

            try:
                # Esperar a que aparezca el iframe
                embed_movie = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'embed-movie'))
                )
                iframe = embed_movie.find_element(By.TAG_NAME, 'iframe')
                embedded_link = iframe.get_attribute('src')
            except Exception as e:
                if logger:
                    logger.error(f"Error al obtener el enlace embebido {i + 1}: {e}")
                continue

            # Modificar el enlace si es powvideo o streamplay
            if embedded_link and server in ["powvideo", "streamplay"]:
                embedded_link = re.sub(r"embed-([^-]+)-\d+x\d+\.html", r"\1", embedded_link)

            # Determinar la calidad en función del servidor
            quality = '1080p' if server in ['streamtape', 'vidmoly', 'mixdrop'] else 'hdrip'

            # Añadir enlace a la lista
            if server and language and embedded_link:
                link_data = {
                    "server": server,
                    "language": language,
                    "link": embedded_link,
                    "quality": quality
                }

                if movie_id:
                    link_data["movie_id"] = movie_id
                elif episode_id:
                    link_data["episode_id"] = episode_id

                server_links.append(link_data)

        return server_links
    except Exception as e:
        if logger:
            logger.error(f"Error al extraer enlaces: {e}")
            logger.debug(traceback.format_exc())
        return []


# Función para limpiar la caché
def clear_cache():
    """Limpia la caché de datos."""
    if CACHE_ENABLED:
        CACHE['servers'] = {}
        CACHE['qualities'] = {}
        CACHE['movies'] = {}
        CACHE['seasons'] = {}
        CACHE['episodes'] = {}


# Función para verificar si hay una siguiente página
def has_next_page(page_url, driver):
    """Verifica si hay una siguiente página de resultados."""
    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Buscar el botón de siguiente página
        next_button = soup.find("a", class_="current")
        if next_button and next_button.find_next_sibling("a"):
            return True

        # Verificar si hay elementos en la página actual
        items = soup.find_all(["div", "span"], class_=lambda c: c and ("tt view" in c))
        return len(items) > 0
    except Exception:
        return False


# Función para buscar una serie por título y año
def find_series_by_title_year(title, year=None, connection=None, db_path=None):
    """Busca una serie por título y año en la base de datos."""
    # Usar caché si está habilitada
    cache_key = f"{title}_{year}"
    if CACHE_ENABLED and cache_key in CACHE['movies']:
        return CACHE['movies'][cache_key]

    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        if year:
            cursor.execute('''
                SELECT id, title, year, imdb_rating, genre FROM media_downloads 
                WHERE title=? AND year=? AND type='serie'
            ''', (title, year))
        else:
            cursor.execute('''
                SELECT id, title, year, imdb_rating, genre FROM media_downloads 
                WHERE title=? AND type='serie'
            ''', (title,))

        results = cursor.fetchall()

        if not results:
            return None

        # Convertir a diccionario
        result_dict = dict(results[0])

        # Guardar en caché
        if CACHE_ENABLED:
            CACHE['movies'][cache_key] = result_dict

        return result_dict
    except Exception as e:
        return None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para verificar si una temporada existe
def season_exists(series_id, season_number, connection=None, db_path=None):
    """Verifica si una temporada existe en la base de datos."""
    # Usar caché si está habilitada
    cache_key = f"{series_id}_{season_number}"
    if CACHE_ENABLED and cache_key in CACHE['seasons']:
        return True, CACHE['seasons'][cache_key]

    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        cursor.execute('''
            SELECT id FROM series_seasons 
            WHERE movie_id=? AND season=?
        ''', (series_id, season_number))
        result = cursor.fetchone()
        exists = result is not None

        if exists:
            season_id = result['id']
            # Guardar en caché
            if CACHE_ENABLED:
                CACHE['seasons'][cache_key] = season_id
            return True, season_id
        else:
            return False, None
    except Exception as e:
        return False, None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para verificar si un episodio existe
def episode_exists(season_id, episode_number, title=None, connection=None, db_path=None):
    """Verifica si un episodio existe en la base de datos."""
    # Usar caché si está habilitada
    cache_key = f"{season_id}_{episode_number}"
    if CACHE_ENABLED and cache_key in CACHE['episodes']:
        return True, CACHE['episodes'][cache_key]

    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        if title:
            cursor.execute('''
                SELECT id FROM series_episodes 
                WHERE season_id=? AND episode=? AND title=?
            ''', (season_id, episode_number, title))
        else:
            cursor.execute('''
                SELECT id FROM series_episodes 
                WHERE season_id=? AND episode=?
            ''', (season_id, episode_number))

        result = cursor.fetchone()
        exists = result is not None

        if exists:
            episode_id = result['id']
            # Guardar en caché
            if CACHE_ENABLED:
                CACHE['episodes'][cache_key] = episode_id
            return True, episode_id
        else:
            return False, None
    except Exception as e:
        return False, None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para insertar una nueva serie
def insert_series(title, year, imdb_rating=None, genre=None, connection=None, db_path=None):
    """Inserta una nueva serie en la base de datos."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True
    cursor = connection.cursor()
    series_id = None

    try:
        cursor.execute('''
            INSERT INTO media_downloads (title, year, imdb_rating, genre, type, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'serie', datetime('now'), datetime('now'))
        ''', (title, year, imdb_rating, genre))
        series_id = cursor.lastrowid
        connection.commit()

        # Actualizar caché
        if CACHE_ENABLED:
            cache_key = f"{title}_{year}"
            CACHE['movies'][cache_key] = {
                'id': series_id,
                'title': title,
                'year': year,
                'imdb_rating': imdb_rating,
                'genre': genre
            }

        return series_id
    except Exception as e:
        connection.rollback()
        return None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para insertar una nueva temporada
def insert_season(series_id, season_number, connection=None, db_path=None):
    """Inserta una nueva temporada en la base de datos."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()
    season_id = None

    try:
        cursor.execute('''
            INSERT INTO series_seasons (movie_id, season)
            VALUES (?, ?)
        ''', (series_id, season_number))
        season_id = cursor.lastrowid
        connection.commit()

        # Actualizar caché
        if CACHE_ENABLED:
            cache_key = f"{series_id}_{season_number}"
            CACHE['seasons'][cache_key] = season_id

        return season_id
    except Exception as e:
        connection.rollback()
        return None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para insertar un nuevo episodio
def insert_episode(season_id, episode_number, title, connection=None, db_path=None):
    """Inserta un nuevo episodio en la base de datos evitando duplicados."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    # Comprobar si el episodio ya existe
    exists, existing_id = episode_exists(
        season_id,
        episode_number,
        title,
        connection=connection,
        db_path=db_path,
    )
    if exists:
        return existing_id

    cursor = connection.cursor()
    episode_id = None

    try:
        cursor.execute(
            '''
            INSERT INTO series_episodes (season_id, episode, title)
            VALUES (?, ?, ?)
            ''',
            (season_id, episode_number, title),
        )
        episode_id = cursor.lastrowid
        connection.commit()

        # Actualizar caché
        if CACHE_ENABLED:
            cache_key = f"{season_id}_{episode_number}"
            CACHE['episodes'][cache_key] = episode_id

        return episode_id
    except Exception as e:
        connection.rollback()
        return None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para verificar si una película ya existe en la base de datos
def movie_exists(title, year=None, imdb_rating=None, genre=None, connection=None, db_path=None):
    """Verifica si una película ya existe en la base de datos."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()

    try:
        # Buscar primero por título y año (si está disponible)
        if year:
            cursor.execute('''
                SELECT id, title, year, imdb_rating, genre FROM media_downloads 
                WHERE title=? AND year=? AND type='movie'
            ''', (title, year))
        else:
            cursor.execute('''
                SELECT id, title, year, imdb_rating, genre FROM media_downloads 
                WHERE title=? AND type='movie'
            ''', (title,))

        results = cursor.fetchall()

        if not results:
            return False, None

        # Si hay resultados, verificar si alguno coincide completamente
        for result in results:
            result_dict = dict(result)
            # Comparar todos los campos relevantes que no sean None
            if (year is None or result_dict['year'] == year) and \
                    (imdb_rating is None or result_dict['imdb_rating'] == imdb_rating) and \
                    (genre is None or result_dict['genre'] == genre):
                return True, result_dict['id']

        # Si llegamos aquí, el título existe pero otros datos no coinciden
        return False, results[0]['id']  # Devolver el ID del primer resultado para posible actualización
    except Exception as e:
        return False, None
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para insertar o actualizar una película en la base de datos
def insert_or_update_movie(movie_data, connection=None, db_path=None):
    """Inserta o actualiza una película en la base de datos."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()
    movie_id = None

    try:
        if movie_data.get("existing_id"):
            # Actualizar película existente
            cursor.execute('''
                UPDATE media_downloads 
                SET year=?, imdb_rating=?, genre=?, updated_at=datetime('now')
                WHERE id=?
            ''', (movie_data["year"], movie_data["imdb_rating"], movie_data["genre"], movie_data["existing_id"]))
            movie_id = movie_data["existing_id"]
            is_new = False
        else:
            # Insertar nueva película
            cursor.execute('''
                INSERT INTO media_downloads (title, year, imdb_rating, genre, type, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ''', (movie_data["title"], movie_data["year"], movie_data["imdb_rating"], movie_data["genre"],
                  movie_data["type"]))
            movie_id = cursor.lastrowid
            is_new = True

        connection.commit()
        return movie_id, is_new
    except Exception as e:
        connection.rollback()
        return None, False
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para obtener episodios con scroll infinito
def get_all_episodes_with_infinite_scroll(driver, logger, max_scroll_attempts=10):
    """Obtiene todos los episodios de una página con scroll infinito."""
    logger.info("Obteniendo episodios con scroll infinito...")

    # Lista para almacenar los URLs de episodios
    episode_urls = []

    # Conjunto para evitar duplicados
    seen_urls = set()

    # Contador para el número de intentos de scroll sin nuevos resultados
    no_new_results_count = 0

    try:
        # Esperar a que el contenedor de episodios esté presente
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "episodes-content"))
        )

        # Bucle para hacer scroll y obtener más episodios
        for scroll_attempt in range(max_scroll_attempts):
            # Obtener los episodios actuales
            episode_divs = driver.find_elements(By.CSS_SELECTOR, "#episodes-content .span-6.tt.view.show-view")
            current_count = len(episode_urls)

            # Procesar los episodios visibles actualmente
            for div in episode_divs:
                try:
                    link_tag = div.find_element(By.TAG_NAME, "a")
                    episode_href = link_tag.get_attribute("href")

                    # Añadir solo si no lo hemos visto antes
                    if episode_href and episode_href not in seen_urls:
                        episode_urls.append(episode_href)
                        seen_urls.add(episode_href)
                except Exception as e:
                    logger.warning(f"Error al procesar un div de episodio: {e}")

            # Verificar si se encontraron nuevos episodios
            if len(episode_urls) == current_count:
                no_new_results_count += 1
                if no_new_results_count >= 3:  # Si no hay nuevos resultados después de 3 intentos, terminar
                    logger.info(
                        f"No se encontraron nuevos episodios después de {no_new_results_count} intentos. Terminando scroll.")
                    break
            else:
                no_new_results_count = 0  # Reiniciar contador si se encontraron nuevos episodios

            # Hacer scroll hacia abajo
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            logger.debug(
                f"Scroll {scroll_attempt + 1}/{max_scroll_attempts}: {len(episode_urls)} episodios encontrados")

            # Esperar a que se carguen más contenidos
            time.sleep(2)

        logger.info(f"Total de episodios encontrados: {len(episode_urls)}")
        return episode_urls

    except Exception as e:
        logger.error(f"Error al obtener episodios con scroll infinito: {e}")
        logger.debug(traceback.format_exc())
        return episode_urls  # Devolver los episodios que se hayan podido obtener


# Función para enviar correo con informe
def send_email_report(subject, body, to_email, from_email, smtp_server, smtp_port, smtp_user, smtp_password):
    """Envía un correo electrónico con el informe de actualización."""
    try:
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()

        return True
    except Exception as e:
        print(f"Error al enviar correo: {e}")
        return False