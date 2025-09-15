import time
import re
import sqlite3
import json
import os
import logging
import sys
import argparse
import concurrent.futures
from datetime import datetime
from queue import Queue
from threading import Lock
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from bs4 import BeautifulSoup

try:  # pragma: no cover - compatible al ejecutarse como script o módulo
    from .scraper_utils import PROJECT_ROOT, BASE_URL, LOGIN_URL, DB_PATH, setup_logger
except ImportError:  # pragma: no cover
    from scraper_utils import PROJECT_ROOT, BASE_URL, LOGIN_URL, DB_PATH, setup_logger
from webdriver_manager.chrome import ChromeDriverManager


# Configuración del logger homogéneo
SCRIPT_NAME = "direct_dw_films_scraper"
LOG_FILE = f"{SCRIPT_NAME}.log"
logger = setup_logger(SCRIPT_NAME, LOG_FILE)


# Redirigir excepciones no capturadas
def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error("Excepción no capturada", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception

# Credenciales de inicio de sesión
username = 'rolankor'
password = 'Rolankor_09'

# URL de la página de inicio de sesión y de las películas
login_url = LOGIN_URL
base_url = BASE_URL
movies_url = f"{BASE_URL}/peliculas/imdb_rating"

# Directorio para guardar el progreso
progress_dir = os.path.join(PROJECT_ROOT, "progress")
if not os.path.exists(progress_dir):
    os.makedirs(progress_dir)

# Archivo para guardar el progreso
progress_file = os.path.join(progress_dir, "movie_progress.json")

# Ruta de la base de datos (usando configuración compartida)
db_path = DB_PATH


def get_total_saved_links(content_type):
    """Return number of links_files_download for a given media type."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(lfd.id)
            FROM links_files_download lfd
            JOIN media_downloads md ON lfd.movie_id = md.id
            WHERE md.type = ?
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

# Contador de reinicios del script
restart_count = 0
MAX_RESTARTS = 3

# Número de workers para el scraping paralelo
NUM_WORKERS = 4

# Lock para sincronizar el acceso a la base de datos
db_lock = Lock()

# Lock para sincronizar el guardado de progreso
progress_lock = Lock()

# Contador total de enlaces guardados
total_saved = 0
total_saved_lock = Lock()

# Cola para almacenar las URLs de las películas a procesar
movie_queue = Queue()


# Función para crear un nuevo driver de Chrome utilizando webdriver-manager
def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    try:
        service = Service(ChromeDriverManager().install())
    except Exception:
        service = Service(os.path.join(PROJECT_ROOT, "chromedriver.exe"))
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(5)
    return driver


# Función para inicializar la base de datos
def initialize_db(path=None):
    global db_path
    with db_lock:
        try:
            if path:
                db_path = path
            logger.debug(f"Iniciando configuración de la base de datos en: {db_path}")
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
            CREATE TABLE IF NOT EXISTS update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes REAL,
                updated_movies INTEGER,
                new_links INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            connection.commit()
            logger.info("Base de datos configurada correctamente")
            return True
        except Exception as e:
            logger.error(f"Error al configurar la base de datos: {e}")
            return False
        finally:
            if connection:
                connection.close()


# Función para conectar a la base de datos
def connect_db():
    try:
        logger.debug(f"Conectando a la base de datos en: {db_path}")
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        logger.debug("Conexión a la base de datos establecida correctamente")
        return connection
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise


# Función para iniciar sesión
def login(driver):
    try:
        logger.info("Iniciando sesión...")
        driver.get(login_url)
        username_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
        password_field = driver.find_element(By.NAME, "password")
        login_button = driver.find_element(By.XPATH, "//a[text()='Ingresar']")

        username_field.send_keys(username)
        password_field.send_keys(password)
        login_button.click()

        WebDriverWait(driver, 10).until(EC.url_changes(login_url))
        logger.info("Sesión iniciada correctamente")
        return True
    except Exception as e:
        logger.error(f"Error al iniciar sesión: {e}")
        return False


# Función para verificar si una película ya existe en la base de datos
def movie_exists(title, year, imdb_rating, genre):
    with db_lock:
        connection = connect_db()
        cursor = connection.cursor()
        try:
            cursor.execute('''
                SELECT id FROM media_downloads 
                WHERE title=? AND year=? AND imdb_rating=? AND genre=? AND type='movie'
            ''', (title, year, imdb_rating, genre))
            result = cursor.fetchone()
            exists = result is not None
            logger.debug(
                f"Verificación de existencia de película: {title} ({year}) - {'Existe' if exists else 'No existe'}")
            return exists
        except Exception as e:
            logger.error(f"Error al verificar si la película existe: {e}")
            return False
        finally:
            cursor.close()
            connection.close()


# Función para extraer detalles de la película
def extract_movie_details(driver, movie_url):
    logger.info(f"Extrayendo detalles de la película: {movie_url}")
    try:
        driver.get(movie_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Extraer el título de la película
        title_tag = soup.find("div", id="summary-title")
        title = title_tag.text.strip() if title_tag else "No encontrado"
        logger.debug(f"Título extraído: {title}")

        # Extraer datos básicos de la película
        show_details = soup.find("div", class_="show-details")
        year = None
        imdb_rating = None
        genre = None

        if show_details:
            year_tag = show_details.find("a", href=re.compile(r"/buscar/year/"))
            if year_tag:
                year = int(year_tag.text.strip())
                logger.debug(f"Año extraído: {year}")

            imdb_rating_tag = show_details.find("p", itemprop="aggregateRating")
            if imdb_rating_tag and imdb_rating_tag.find("a"):
                imdb_rating = float(imdb_rating_tag.find("a").text.strip())
                logger.debug(f"IMDB Rating extraído: {imdb_rating}")

            genre_tag = show_details.find("a", href=re.compile(r"/tags-peliculas"))
            if genre_tag:
                genre = genre_tag.text.strip()
                logger.debug(f"Género extraído: {genre}")

        # Verificar si la película ya existe en la base de datos
        if movie_exists(title, year, imdb_rating, genre):
            logger.info(f"La película '{title}' ({year}) ya existe en la base de datos. Saltando...")
            return None

        # Crear lista para almacenar los enlaces
        server_links = []

        # Encontrar todos los embed-selectors
        embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
        logger.debug(f"Número de enlaces encontrados: {len(embed_selectors)}")

        for i, embed_selector in enumerate(embed_selectors):
            language = None
            server = None
            embedded_link = None

            try:
                # Refrescar la lista de embed-selectors para evitar StaleElementReferenceException
                if i > 0:  # No es necesario para el primer elemento
                    embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
                    if i >= len(embed_selectors):
                        logger.warning(f"Índice {i} fuera de rango después de refrescar los embed-selectors")
                        continue
                    embed_selector = embed_selectors[i]

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
                time.sleep(2)  # Esperar 2 segundos para que el contenido se cargue

                # Obtener el enlace embebido
                try:
                    embed_movie = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'embed-movie'))
                    )
                    iframe = embed_movie.find_element(By.TAG_NAME, 'iframe')
                    embedded_link = iframe.get_attribute('src')
                    logger.debug(f"Enlace embebido extraído: {embedded_link}")
                except (TimeoutException, Exception) as e:
                    logger.error(f"Error al obtener el enlace embebido: {e}")
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
            except StaleElementReferenceException:
                logger.warning(f"Elemento obsoleto encontrado para el enlace {i + 1}. Refrescando elementos...")
                time.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Error al procesar el enlace {i + 1}: {e}")
                continue

        # Solo devolver los detalles si se encontraron enlaces
        if server_links:
            logger.debug(
                f"Detalles de la película extraídos: {title}, {year}, {imdb_rating}, {genre}, {len(server_links)} enlaces")
            return {
                "Nombre": title,
                "Año": year,
                "IMDB Rating": imdb_rating,
                "Género": genre,
                "Enlaces": server_links
            }
        else:
            logger.warning(f"No se encontraron enlaces para la película: {title}. Saltando...")
            return None
    except Exception as e:
        logger.error(f"Error al extraer detalles de la película {movie_url}: {e}")
        raise


# Función para insertar datos en la base de datos
def insert_data_into_db(movie):
    if not movie:
        logger.debug("No hay datos de película para insertar (posiblemente ya existe)")
        return 0

    with db_lock:
        connection = connect_db()
        cursor = connection.cursor()

        try:
            # Iniciar transacción
            connection.execute("BEGIN TRANSACTION")
            links_inserted = 0

            # Insertar la película en la base de datos con type='movie'
            cursor.execute('''
                INSERT INTO media_downloads (title, year, imdb_rating, genre, type)
                VALUES (?, ?, ?, ?, 'movie')
            ''', (movie["Nombre"], movie["Año"], movie["IMDB Rating"], movie["Género"]))
            movie_id = cursor.lastrowid

            # Verificar que se haya insertado correctamente
            if not movie_id:
                logger.error(f"Error al insertar la película: {movie['Nombre']}")
                connection.rollback()
                return False

            # Insertar los enlaces de la película
            for link in movie["Enlaces"]:
                # Insertar el servidor si no existe
                cursor.execute('''
                    INSERT OR IGNORE INTO servers (name) VALUES (?)
                ''', (link["server"],))
                cursor.execute('''
                    SELECT id FROM servers WHERE name=?
                ''', (link["server"],))
                server_id = cursor.fetchone()['id']

                # Obtener el ID de la calidad
                cursor.execute('''
                    SELECT quality_id FROM qualities WHERE quality=?
                ''', (link["quality"],))
                quality_result = cursor.fetchone()

                # Si la calidad no existe, insertarla
                if not quality_result:
                    cursor.execute('''
                        INSERT INTO qualities (quality) VALUES (?)
                    ''', (link["quality"],))
                    cursor.execute('''
                        SELECT quality_id FROM qualities WHERE quality=?
                    ''', (link["quality"],))
                    quality_result = cursor.fetchone()

                quality_id = quality_result['quality_id']

                # Insertar el enlace en la base de datos
                cursor.execute('''
                    INSERT INTO links_files_download (movie_id, server_id, language, link, quality_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (movie_id, server_id, link["language"], link["link"], quality_id))
                log_link_insertion(
                    logger,
                    movie_id=movie_id,
                    server=link["server"],
                    language=link["language"],
                )
                links_inserted += 1

            # Confirmar la transacción solo si todo fue exitoso
            connection.commit()
            logger.info(
                f"Datos insertados en la base de datos para la película: {movie['Nombre']} ({links_inserted} enlaces)"
            )
            return links_inserted
        except Exception as e:
            logger.error(f"Error al insertar datos en la base de datos: {e}")
            connection.rollback()
            return 0
        finally:
            cursor.close()
            connection.close()


# Función para contar el número total de páginas de películas
def count_total_pages(driver):
    try:
        logger.info("Contando el número total de páginas de películas...")
        driver.get(movies_url)
        time.sleep(2)  # Esperar a que se cargue la página

        # Buscar el elemento de paginación
        pagination = driver.find_elements(By.XPATH, "//ul[@class='pagination']/li")

        if pagination and len(pagination) > 2:
            # El último elemento de la paginación suele ser el botón "Siguiente"
            # El penúltimo elemento suele ser el número de la última página
            last_page_element = pagination[-2]
            last_page_text = last_page_element.text.strip()

            try:
                total_pages = int(last_page_text)
                logger.info(f"Número total de páginas encontrado: {total_pages}")
                return total_pages
            except ValueError:
                logger.warning(f"No se pudo convertir '{last_page_text}' a un número entero")
                return None
        else:
            logger.warning("No se encontró el elemento de paginación o no tiene suficientes elementos")
            return None
    except Exception as e:
        logger.error(f"Error al contar el número total de páginas: {e}")
        return None


# Función auxiliar para localizar los divs de películas
def _find_movie_divs(driver):
    """Devuelve una lista de divs que representan películas en la página actual."""
    for container_selector in ["div.center", "div.container"]:
        try:
            container = driver.find_element(By.CSS_SELECTOR, container_selector)
            return container.find_elements(By.CSS_SELECTOR, "div.span-6.inner-6.tt.view")
        except Exception:
            continue
    # Fallback: buscar directamente todos los divs de película
    try:
        return driver.find_elements(By.CSS_SELECTOR, "div.span-6.inner-6.tt.view")
    except Exception:
        return []


# Función para extraer URLs de películas de una página
def extract_movie_urls_from_page(driver, page_url, page_number):
    logger.info(f"Extrayendo URLs de películas de la página: {page_url}")
    try:
        driver.get(page_url)
        time.sleep(2)

        movie_divs = _find_movie_divs(driver)
        total_movies = len(movie_divs)
        logger.info(f"Encontradas {total_movies} películas en la página {page_number}")

        if total_movies == 0:
            logger.info(f"No se encontraron películas en la página {page_number}. Puede ser la última página.")
            return []

        movie_urls = []
        for idx, movie_div in enumerate(movie_divs):
            if not movie_div:
                continue
            try:
                link_tag = movie_div.find_element(By.TAG_NAME, "a")
                movie_url = link_tag.get_attribute("href")
                title = link_tag.get_attribute("title") or link_tag.text.strip()
                movie_urls.append((page_number, idx, movie_url, title))
            except StaleElementReferenceException:
                logger.warning(f"Elemento obsoleto encontrado en el índice {idx}. Refrescando página...")
                driver.refresh()
                time.sleep(2)
                movie_divs = _find_movie_divs(driver)
                if idx < len(movie_divs):
                    try:
                        link_tag = movie_divs[idx].find_element(By.TAG_NAME, "a")
                        movie_url = link_tag.get_attribute("href")
                        title = link_tag.get_attribute("title") or link_tag.text.strip()
                        movie_urls.append((page_number, idx, movie_url, title))
                    except Exception as e:
                        logger.error(f"Error al obtener la URL después del refresco: {e}")
                else:
                    logger.error(f"Índice {idx} fuera de rango después de refrescar")
            except Exception as e:
                logger.error(f"Error al obtener la URL de la película en el índice {idx}: {e}")

        return movie_urls
    except Exception as e:
        logger.error(f"Error al extraer URLs de películas de la página {page_url}: {e}")
        return []


# Función para guardar el progreso
def save_progress(page_number, last_movie_title, last_movie_index, total_saved):
    try:
        with progress_lock:
            with open(progress_file, 'w') as f:
                json.dump({
                    'page_number': page_number,
                    'last_movie_title': last_movie_title,
                    'last_movie_index': last_movie_index,
                    'total_saved': total_saved
                }, f)
        logger.debug(
            f"Progreso guardado: página {page_number}, título {last_movie_title}, índice {last_movie_index}, total {total_saved}")
    except Exception as e:
        logger.error(f"Error al guardar el progreso: {e}")


# Función para cargar el progreso
def load_progress():
    db_total = get_total_saved_links('movie')
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                logger.info(
                    f"Progreso cargado: página {progress['page_number']}, título {progress.get('last_movie_title')}, índice {progress.get('last_movie_index')}, Total guardado = {db_total}")
                return (progress['page_number'],
                        progress.get('last_movie_title'),
                        progress.get('last_movie_index'),
                        db_total)
        logger.info("No se encontró archivo de progreso. Comenzando desde el principio.")
        return 1, None, None, db_total
    except Exception as e:
        logger.error(f"Error al cargar el progreso: {e}")
        return 1, None, None, db_total


# Función worker para procesar películas
def movie_worker(worker_id):
    global total_saved
    # Crear un driver para este worker
    driver = create_driver()

    # Iniciar sesión con este driver
    if not login(driver):
        logger.error(f"Worker {worker_id}: No se pudo iniciar sesión. Abortando...")
        driver.quit()
        return

    logger.info(f"Worker {worker_id}: Iniciado y listo para procesar películas")

    try:
        while True:
            try:
                # Obtener datos de película de la cola
                page_num, index, movie_url, title = movie_queue.get(block=False)

                logger.info(f"Worker {worker_id}: Procesando película {index} (página {page_num}): {movie_url}")

                # Intentar extraer los detalles de la película con reintentos
                success = False
                movie_details = None

                # Primer conjunto de 3 intentos
                for attempt in range(3):
                    try:
                        movie_details = extract_movie_details(driver, movie_url)
                        success = True
                        break  # Salir del bucle de reintentos si tiene éxito
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Error al extraer datos de la película {movie_url}: {e}")
                        if attempt < 2:
                            logger.info(f"Worker {worker_id}: Reintentando en 5 segundos... (Intento {attempt + 1}/3)")
                            time.sleep(5)

                # Si después de 3 intentos no hay éxito, reiniciar el driver y probar 3 veces más
                if not success:
                    logger.info(f"Worker {worker_id}: Reiniciando el driver después de 3 intentos fallidos...")
                    driver.quit()
                    driver = create_driver()
                    if login(driver):
                        # Segundo conjunto de 3 intentos después de reiniciar el driver
                        for attempt in range(3):
                            try:
                                movie_details = extract_movie_details(driver, movie_url)
                                success = True
                                break  # Salir del bucle de reintentos si tiene éxito
                            except Exception as e:
                                logger.error(
                                    f"Worker {worker_id}: Error al extraer datos de la película {movie_url} después de reiniciar: {e}")
                                if attempt < 2:
                                    logger.info(
                                        f"Worker {worker_id}: Reintentando en 5 segundos... (Intento {attempt + 1}/3 después de reiniciar)")
                                    time.sleep(5)
                    else:
                        logger.error(f"Worker {worker_id}: No se pudo iniciar sesión después de reiniciar el driver.")

                # Si se obtuvieron los detalles de la película, insertarlos en la base de datos
                if success and movie_details:
                    links = insert_data_into_db(movie_details)
                    if links:
                        with total_saved_lock:
                            total_saved += links
                        save_progress(page_num, title, index, total_saved)

                # Marcar la tarea como completada
                movie_queue.task_done()

            except Exception as e:
                if "queue.Empty" in str(e.__class__):
                    # La cola está vacía, esperar un poco y volver a intentar
                    time.sleep(1)
                    # Si la cola sigue vacía después de esperar, salir del bucle
                    if movie_queue.empty():
                        logger.info(f"Worker {worker_id}: No hay más películas para procesar. Finalizando.")
                        break
                else:
                    logger.error(f"Worker {worker_id}: Error inesperado: {e}")
                    time.sleep(1)
    finally:
        # Cerrar el driver al finalizar
        driver.quit()
        logger.info(f"Worker {worker_id}: Finalizado y driver cerrado.")


# Función principal para extraer todas las páginas de películas
def extract_all_movies(start_page=None, db_path=None):
    start_time = datetime.now()
    logger.info(f"Iniciando procesamiento de películas: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    # Inicializar la base de datos
    initialize_db(db_path)

    # Crear un driver principal para la navegación por páginas
    main_driver = create_driver()

    if not login(main_driver):
        logger.error("No se pudo iniciar sesión con el driver principal. Abortando...")
        main_driver.quit()
        return

    # Contar el número total de páginas
    total_pages = count_total_pages(main_driver)
    if total_pages:
        logger.info(f"Se procesarán {total_pages} páginas en total")
    else:
        logger.warning(
            "No se pudo determinar el número total de páginas. Se procesarán hasta encontrar una página vacía.")

    # Cargar el progreso guardado
    global total_saved
    page_number, last_title, last_index, total_saved_local = load_progress()
    if start_page is not None:
        page_number = start_page
        last_title = None
        last_index = -1
    with total_saved_lock:
        total_saved = total_saved_local
    logger.info(f"Enlaces guardados previamente: {total_saved}")

    try:
        # Crear un pool de workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            # Iniciar los workers
            workers = [executor.submit(movie_worker, i + 1) for i in range(NUM_WORKERS)]

            # Procesar páginas una por una
            while True:
                page_url = f"{movies_url}/{page_number}"
                logger.info(f"Extrayendo URLs de películas de la página: {page_url}")

                movie_urls = extract_movie_urls_from_page(main_driver, page_url, page_number)

                if not movie_urls:
                    logger.info(f"No se encontraron películas en la página {page_number}. Finalizando.")
                    break

                # Si estamos reanudando y hay un título previo, recortar la lista
                if last_title:
                    for idx, data in enumerate(movie_urls):
                        if data[3] == last_title:
                            movie_urls = movie_urls[idx:]
                            break
                    last_title = None

                # Guardar progreso al inicio de la página
                with total_saved_lock:
                    current_total = total_saved
                save_progress(page_number, None, -1, current_total)

                for data in movie_urls:
                    movie_queue.put(data)
                    logger.debug(f"Añadida película {data[1]} a la cola: {data[2]}")

                if total_pages and page_number >= total_pages:
                    logger.info(f"Se ha alcanzado la última página ({page_number} de {total_pages}). Finalizando.")
                    break

                page_number += 1

            # Esperar a que se complete el procesamiento de todas las películas en la cola
            logger.info("Esperando a que se completen todas las tareas en la cola...")
            movie_queue.join()

            # Cancelar los workers
            for worker in workers:
                worker.cancel()

            logger.info("Todos los workers han finalizado.")

    except Exception as e:
        logger.critical(f"Error crítico en el proceso principal: {e}")
    finally:
        # Cerrar el driver principal
        main_driver.quit()
        logger.info("Driver principal cerrado.")
        with total_saved_lock:
            current_total = total_saved
        save_progress(page_number, None, -1, current_total)
        logger.info(f"Proceso finalizado. Total enlaces guardados: {current_total}")


# Punto de entrada principal
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Procesamiento de películas por rating IMDB')
    parser.add_argument('--start-page', type=int, help='Página inicial para comenzar el procesamiento')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')
    args = parser.parse_args()

    try:
        logger.info("Iniciando el scraper de películas con procesamiento paralelo...")
        # Verificar si estamos en un reinicio
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                if 'restart_count' in progress:
                    restart_count = progress['restart_count']
                    logger.info(f"Reinicio detectado. Contador de reinicios: {restart_count}/{MAX_RESTARTS}")

        # Ejecutar la extracción de todas las películas
        extract_all_movies(args.start_page, args.db_path)
        logger.info("Proceso de scraping de películas completado.")
    except Exception as e:
        logger.critical(f"Error crítico en el scraper: {e}")
        # Intentar guardar el progreso antes de salir
        try:
            page_number, _, _, total_saved_local = load_progress()
            save_progress(page_number, None, -1, total_saved_local)
        except Exception:
            pass
    finally:
        logger.info("Scraper finalizado.")
