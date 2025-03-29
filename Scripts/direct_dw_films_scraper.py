import time
import re
import sqlite3
import json
import os
import logging
import sys
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from bs4 import BeautifulSoup

from main import PROJECT_ROOT

# Configuración del logger para evitar duplicación
logger = logging.getLogger("films_scraper")
logger.setLevel(logging.DEBUG)
logger.propagate = False  # Importante para evitar duplicación

# Limpiar handlers previos
if logger.handlers:
    logger.handlers.clear()

# Handler para consola con salida a stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Handler para archivo
file_handler = logging.FileHandler(os.path.join(PROJECT_ROOT, "logs", "direct_scraper_films.log"))
file_handler.setLevel(logging.INFO)

# Formato único para ambos handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# Redirigir excepciones no capturadas
def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error("Excepción no capturada", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception

# Credenciales de inicio de sesión
username = 'rolankor'
password = 'Rolankor_09'

# URL de la página de inicio de sesión y de las películas
login_url = "https://hdfull.blog/login"
base_url = "https://hdfull.blog"
movies_url = "https://hdfull.blog/peliculas/imdb_rating"

# Configuración de Selenium
service = Service((os.path.join(PROJECT_ROOT, "chromedriver.exe")))
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Ejecuta Chrome en modo headless
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(service=service, options=options)

# Archivo para guardar el progreso
progress_file = (os.path.join(PROJECT_ROOT, "progress", "movie_progress.json"))

# Ruta de la base de datos
db_path = r'D:/Workplace/HdfullScrappers/Scripts/direct_dw_db.db'

# Contador de reinicios del script
restart_count = 0
MAX_RESTARTS = 3


# Función para inicializar la base de datos
def initialize_db():
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
        # Verificar si el archivo de la base de datos existe
        db_exists = os.path.exists(db_path)

        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row

        # Si la base de datos no existía, inicializarla
        if not db_exists:
            logger.info("La base de datos no existe. Creando tablas...")
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
            logger.info("Tablas creadas correctamente")

        logger.debug("Conexión a la base de datos establecida correctamente")
        return connection
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise


# Función para iniciar sesión
def login():
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
def extract_movie_details(movie_url, movie_id):
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
                        "movie_id": movie_id,
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
                "id": movie_id,
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


# Función para reiniciar el navegador y la sesión
def restart_browser():
    global driver
    logger.info("Reiniciando el navegador...")
    try:
        driver.quit()
        time.sleep(2)  # Esperar a que se cierre correctamente
        driver = webdriver.Chrome(service=service, options=options)
        login_success = login()
        if login_success:
            logger.info("Navegador reiniciado y sesión iniciada correctamente")
            return True
        else:
            logger.error("No se pudo iniciar sesión después de reiniciar el navegador")
            return False
    except Exception as e:
        logger.error(f"Error al reiniciar el navegador: {e}")
        return False


# Función para reiniciar el script completo
def restart_script():
    global restart_count
    restart_count += 1

    if restart_count <= MAX_RESTARTS:
        logger.info(f"Reiniciando el script completo (intento {restart_count}/{MAX_RESTARTS})...")
        try:
            # Cerrar el navegador antes de reiniciar
            driver.quit()

            # Reiniciar el script usando el mismo intérprete de Python
            python = sys.executable
            script = os.path.abspath(__file__)
            os.execl(python, python, script)
        except Exception as e:
            logger.error(f"Error al reiniciar el script: {e}")
            return False
    else:
        logger.warning(f"Se alcanzó el máximo de reinicios ({MAX_RESTARTS}). Continuando con la siguiente película.")
        return False


# Función para contar el número total de páginas de películas
def count_total_pages():
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


# Función para insertar datos en la base de datos
def insert_data_into_db(movie):
    if not movie:
        logger.debug("No hay datos de película para insertar (posiblemente ya existe)")
        return False

    connection = connect_db()
    cursor = connection.cursor()

    try:
        # Iniciar transacción
        connection.execute("BEGIN TRANSACTION")

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
            logger.debug(
                f"Enlace insertado: movie_id={movie_id}, server={link['server']}, language={link['language']}")

        # Confirmar la transacción solo si todo fue exitoso
        connection.commit()
        logger.info(f"Datos insertados en la base de datos para la película: {movie['Nombre']}")
        return True
    except Exception as e:
        logger.error(f"Error al insertar datos en la base de datos: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()
        connection.close()


# Función para extraer todas las películas de una página
def extract_movies_from_page(page_url, page_number, start_id, last_movie_index=None):
    logger.info(f"Extrayendo películas de la página: {page_url}")
    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página

        # Obtener el contenedor principal de películas (div con clase "center")
        try:
            center_div = driver.find_element(By.CSS_SELECTOR, "div.center")
            # Obtener todas las películas dentro del contenedor
            movie_divs = center_div.find_elements(By.CSS_SELECTOR, "div.span-6.inner-6.tt.view")
        except Exception as e:
            logger.error(f"Error al obtener el contenedor principal (div.center): {e}")
            # Intento alternativo si falla el selector específico
            try:
                movie_divs = driver.find_elements(By.CSS_SELECTOR, "div.span-6.inner-6.tt.view")
            except Exception as e2:
                logger.error(f"Error al obtener películas con selector alternativo: {e2}")
                movie_divs = []

        total_movies = len(movie_divs)
        logger.info(f"Encontradas {total_movies} películas en la página {page_number}")

        if total_movies == 0:
            logger.info(f"No se encontraron películas en la página {page_number}. Puede ser la última página.")
            return [], -1

        movies = []
        movie_id = start_id

        # Determinar el índice inicial para el scraping
        start_index = last_movie_index + 1 if last_movie_index is not None else 0
        logger.debug(f"Comenzando desde el índice {start_index}")

        # Guardar las URLs de las películas primero para evitar StaleElementReferenceException
        movie_urls = []
        for i in range(start_index, total_movies):
            try:
                movie_div = movie_divs[i]
                link_tag = movie_div.find_element(By.TAG_NAME, "a")
                movie_url = link_tag.get_attribute("href")
                movie_urls.append((i, movie_url))
            except StaleElementReferenceException:
                # Si el elemento está obsoleto, refrescamos la página y volvemos a intentarlo
                logger.warning(f"Elemento obsoleto encontrado en el índice {i}. Refrescando página...")
                driver.refresh()
                time.sleep(2)

                # Volver a obtener el contenedor y las películas
                try:
                    center_div = driver.find_element(By.CSS_SELECTOR, "div.center")
                    movie_divs = center_div.find_elements(By.CSS_SELECTOR, "div.span-6.inner-6.tt.view")

                    if i < len(movie_divs):
                        movie_div = movie_divs[i]
                        link_tag = movie_div.find_element(By.TAG_NAME, "a")
                        movie_url = link_tag.get_attribute("href")
                        movie_urls.append((i, movie_url))
                    else:
                        logger.error(f"Índice {i} fuera de rango después de refrescar")
                except Exception as e:
                    logger.error(f"Error al refrescar elementos: {e}")
            except Exception as e:
                logger.error(f"Error al obtener la URL de la película en el índice {i}: {e}")

        # Procesar cada película usando las URLs guardadas
        for index, movie_url in movie_urls:
            logger.info(f"Extrayendo datos de la película {index + 1}/{total_movies}: {movie_url}")

            success = False

            # Primer conjunto de 3 intentos
            for attempt in range(3):
                try:
                    movie_details = extract_movie_details(movie_url, movie_id)
                    if movie_details:  # Solo procesar si la película no existe ya y tiene enlaces
                        # Insertar inmediatamente los datos de la película
                        if insert_data_into_db(movie_details):
                            movies.append(movie_details)
                            movie_id += 1
                    success = True
                    break  # Salir del bucle de reintentos si tiene éxito
                except Exception as e:
                    logger.error(f"Error al extraer datos de la película {movie_url}: {e}")
                    if attempt < 2:
                        logger.info(f"Reintentando en 1 minuto... (Intento {attempt + 1}/3)")
                        time.sleep(60)  # Esperar 1 minuto antes de reintentar

            # Si después de 3 intentos no hay éxito, reiniciar el navegador y probar 3 veces más
            if not success:
                logger.info("Reiniciando el navegador después de 3 intentos fallidos...")
                if restart_browser():
                    # Segundo conjunto de 3 intentos después de reiniciar el navegador
                    for attempt in range(3):
                        try:
                            movie_details = extract_movie_details(movie_url, movie_id)
                            if movie_details:  # Solo procesar si la película no existe ya y tiene enlaces
                                # Insertar inmediatamente los datos de la película
                                if insert_data_into_db(movie_details):
                                    movies.append(movie_details)
                                    movie_id += 1
                            success = True
                            break  # Salir del bucle de reintentos si tiene éxito
                        except Exception as e:
                            logger.error(f"Error al extraer datos de la película {movie_url} después de reiniciar: {e}")
                            if attempt < 2:
                                logger.info(
                                    f"Reintentando en 1 minuto... (Intento {attempt + 1}/3 después de reiniciar)")
                                time.sleep(60)  # Esperar 1 minuto antes de reintentar
                else:
                    logger.error("No se pudo reiniciar el navegador.")

            # Si después de todos los intentos sigue fallando, reiniciar el script completo
            if not success:
                logger.warning("Todos los intentos fallidos. Reiniciando el script completo...")
                # Guardar el progreso antes de reiniciar
                save_progress(page_number, index)
                # Reiniciar el script
                if restart_script():
                    # Si el reinicio fue exitoso, el script se habrá reiniciado y no llegaremos aquí
                    pass
                else:
                    # Si se alcanzó el máximo de reinicios, continuar con la siguiente película
                    logger.warning(f"Pasando a la siguiente película después de {MAX_RESTARTS} reinicios fallidos.")
                    continue

            # Guardar el progreso después de cada película
            save_progress(page_number, index)

        return movies, index if movie_urls else -1
    except Exception as e:
        logger.error(f"Error al extraer películas de la página {page_url}: {e}")
        raise


# Función para guardar el progreso
def save_progress(page_number, last_movie_index):
    try:
        # Asegurarse de que el directorio de progreso existe
        progress_dir = os.path.dirname(progress_file)
        if not os.path.exists(progress_dir):
            os.makedirs(progress_dir)

        with open(progress_file, 'w') as f:
            json.dump({'page_number': page_number, 'last_movie_index': last_movie_index}, f)
        logger.debug(f"Progreso guardado: página {page_number}, índice {last_movie_index}")
    except Exception as e:
        logger.error(f"Error al guardar el progreso: {e}")


# Función para cargar el progreso
def load_progress():
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                logger.info(
                    f"Progreso cargado: página {progress['page_number']}, índice {progress.get('last_movie_index')}")
                return progress['page_number'], progress.get('last_movie_index')
        logger.info("No se encontró archivo de progreso. Comenzando desde el principio.")
        return 1, None
    except Exception as e:
        logger.error(f"Error al cargar el progreso: {e}")
        return 1, None


# Función principal para extraer todas las páginas de películas
def extract_all_movies():
    # Inicializar la base de datos si es necesario
    initialize_db()

    if not login():
        logger.error("No se pudo iniciar sesión. Abortando...")
        return

    # Contar el número total de páginas
    total_pages = count_total_pages()
    if total_pages:
        logger.info(f"Se procesarán {total_pages} páginas en total")
    else:
        logger.warning(
            "No se pudo determinar el número total de páginas. Se procesarán hasta encontrar una página vacía.")

    page_number, last_movie_index = load_progress()

    while True:  # Continuar hasta que se encuentre una página vacía o se alcance el número total de páginas
        try:
            page_url = f"{movies_url}/{page_number}" if page_number > 1 else movies_url
            logger.info(f"Extrayendo datos de la página: {page_url}")
            movies, last_movie_index = extract_movies_from_page(page_url, page_number, start_id=1,
                                                                last_movie_index=last_movie_index)

            # Si no hay películas en esta página, hemos llegado al final
            if not movies and last_movie_index == -1:
                logger.info(f"No se encontraron películas en la página {page_number}. Finalizando.")
                break

            # Si conocemos el total de páginas y hemos llegado a la última, terminar
            if total_pages and page_number >= total_pages:
                logger.info(f"Se ha alcanzado la última página ({page_number} de {total_pages}). Finalizando.")
                break

            # Avanzar a la siguiente página
            last_movie_index = -1  # Resetear el índice para la siguiente página
            page_number += 1
            save_progress(page_number, last_movie_index)

        except Exception as e:
            logger.error(f"Error: {e}. Intentando nuevamente en 1 minuto...")
            time.sleep(60)  # Esperar 1 minuto antes de intentar nuevamente

            # Reiniciar el driver y la sesión
            if not restart_browser():
                logger.error("No se pudo reiniciar la sesión después de un error.")
                # Intentar reiniciar el script completo
                if restart_script():
                    # Si el reinicio fue exitoso, el script se habrá reiniciado y no llegaremos aquí
                    pass
                else:
                    # Si se alcanzó el máximo de reinicios, abortar
                    logger.error("No se pudo reiniciar el script después de múltiples intentos. Abortando...")
                    break


# Punto de entrada principal
if __name__ == "__main__":
    try:
        logger.info("Iniciando el scraper de películas...")
        # Verificar si estamos en un reinicio
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                if 'restart_count' in progress:
                    restart_count = progress['restart_count']
                    logger.info(f"Reinicio detectado. Contador de reinicios: {restart_count}/{MAX_RESTARTS}")

        # Ejecutar la extracción de todas las películas
        extract_all_movies()
        logger.info("Proceso de scraping de películas completado.")
    except Exception as e:
        logger.critical(f"Error crítico en el scraper: {e}")
        # Intentar reiniciar el script en caso de error crítico
        if restart_script():
            pass  # El script se reiniciará si es posible
        else:
            logger.critical("No se pudo recuperar del error crítico después de múltiples intentos.")
    finally:
        driver.quit()
        logger.info("Driver de Selenium cerrado.")