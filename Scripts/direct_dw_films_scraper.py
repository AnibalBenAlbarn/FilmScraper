import time
import re
import sqlite3
import json
import os
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
# Configuración mejorada del logging
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Nivel más bajo para el logger

# Handler para consola con salida a stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Handler para archivo
file_handler = logging.FileHandler('../logs/direct_scraper.log')
file_handler.setLevel(logging.INFO)

# Formato único para ambos handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Limpiar handlers previos y añadir los nuevos
logger.handlers = []
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
service = Service('../chromedriver.exe')  # Reemplaza 'path/to/chromedriver' con la ruta a tu chromedriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Ejecuta Chrome en modo headless
driver = webdriver.Chrome(service=service, options=options)

# Archivo para guardar el progreso
progress_file = "../progress/movie_progress.json"


# Función para conectar a la base de datos
def connect_db():
    try:
        connection = sqlite3.connect(r'D:/Workplace/HdfullScrappers/Scripts/direct_dw_db.db')
        connection.row_factory = sqlite3.Row
        logger.debug("Conexión a la base de datos establecida correctamente")
        return connection
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise
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
            if imdb_rating_tag:
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

        for embed_selector in embed_selectors:
            language = None
            server = None
            embedded_link = None

            try:
                embed_selector.click()
                time.sleep(2)  # Esperar 2 segundos para que el contenido se cargue
            except Exception as e:
                logger.error(f"Error al hacer clic en el embed-selector: {e}")
                continue

            try:
                embed_movie = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'embed-movie'))
                )
                iframe = embed_movie.find_element(By.TAG_NAME, 'iframe')
                embedded_link = iframe.get_attribute('src')
                logger.debug(f"Enlace embebido extraído: {embedded_link}")
            except Exception as e:
                logger.error(f"Error al obtener el enlace embebido: {e}")
                continue

            # Extraer idioma y servidor
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

            # Modificar el enlace si es powvideo o streamplay
            if embedded_link and server in ["powvideo", "streamplay"]:
                embedded_link = re.sub(r"embed-([^-]+)-\d+x\d+\.html", r"\1", embedded_link)

            # Determinar la calidad en función del servidor
            quality = '1080p' if server in ['streamtape', 'vidmoly', 'mixdrop'] else 'hdrip'

            # Añadir enlace a la lista
            if server and language:
                server_links.append({
                    "movie_id": movie_id,
                    "server": server,
                    "language": language,
                    "link": embedded_link,
                    "quality": quality
                })

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
    except Exception as e:
        logger.error(f"Error al extraer detalles de la película {movie_url}: {e}")
        raise


# Función para extraer todas las películas de una página
def extract_movies_from_page(page_url, page_number, start_id, last_movie_index=None):
    logger.info(f"Extrayendo películas de la página: {page_url}")
    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")
        movies = []
        movie_id = start_id

        # Buscar todos los divs de las películas
        movie_divs = soup.find_all("div", class_="span-6 inner-6 tt view")
        logger.info(f"Encontradas {len(movie_divs)} películas en la página {page_number}")

        # Determinar el índice inicial para el scraping
        start_index = last_movie_index + 1 if last_movie_index is not None else 0
        logger.debug(f"Comenzando desde el índice {start_index}")

        for index, movie_div in enumerate(movie_divs[start_index:], start_index):
            link_tag = movie_div.find("a", href=re.compile(r"/pelicula/"))
            if link_tag:
                movie_href = link_tag['href']
                movie_url = base_url + movie_href
                logger.info(f"Extrayendo datos de la película {index + 1}/{len(movie_divs)}: {movie_url}")
                success = False

                for package_attempt in range(3):
                    for attempt in range(3):
                        try:
                            movie_details = extract_movie_details(movie_url, movie_id)
                            if movie_details:  # Solo procesar si la película no existe ya
                                movies.append(movie_details)
                                insert_data_into_db(movie_details)  # Insertar la película a medida que se extrae
                                movie_id += 1
                            success = True
                            break  # Salir del bucle de reintentos si tiene éxito
                        except Exception as e:
                            logger.error(f"Error al extraer datos de la película {movie_url}: {e}")
                            if attempt < 2:
                                logger.info(f"Reintentando en 5 minutos... (Intento {attempt + 1}/3)")
                                time.sleep(300)  # Esperar 5 minutos antes de reintentar
                            else:
                                logger.info(
                                    f"Paquete de 3 intentos fallidos. Esperando 5 minutos antes del siguiente paquete.")
                                time.sleep(300)  # Esperar 5 minutos antes del siguiente paquete de intentos

                    if success:
                        break  # Salir del bucle de paquetes de intentos si tiene éxito
                else:
                    logger.warning(f"Pasando a la siguiente película después de 3 paquetes de intentos fallidos.")
                    continue  # Pasar a la siguiente película después de 3 paquetes de intentos fallidos

                # Guardar el progreso después de cada película
                save_progress(page_number, index)

        return movies, index
    except Exception as e:
        logger.error(f"Error al extraer películas de la página {page_url}: {e}")
        raise


# Función para guardar el progreso
def save_progress(page_number, last_movie_index):
    try:
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


# Función para insertar datos en la base de datos
def insert_data_into_db(movie):
    if not movie:
        logger.debug("No hay datos de película para insertar (posiblemente ya existe)")
        return

    connection = connect_db()
    cursor = connection.cursor()

    try:
        # Insertar la película en la base de datos con type='movie'
        cursor.execute('''
            INSERT INTO media_downloads (title, year, imdb_rating, genre, type)
            VALUES (?, ?, ?, ?, 'movie')
        ''', (movie["Nombre"], movie["Año"], movie["IMDB Rating"], movie["Género"]))
        movie_id = cursor.lastrowid

        # Si es una actualización, obtener el ID existente
        if movie_id == 0:
            cursor.execute('''
                SELECT id FROM media_downloads 
                WHERE title=? AND year=? AND imdb_rating=? AND genre=?
            ''', (movie["Nombre"], movie["Año"], movie["IMDB Rating"], movie["Género"]))
            movie_id = cursor.fetchone()['id']

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

            # Verificar si el enlace ya existe
            cursor.execute('''
                SELECT id FROM links_files_download 
                WHERE movie_id=? AND server_id=? AND language=? AND link=?
            ''', (movie_id, server_id, link["language"], link["link"]))
            link_exists = cursor.fetchone()

            if not link_exists:
                # Insertar el enlace en la base de datos
                cursor.execute('''
                    INSERT INTO links_files_download (movie_id, server_id, language, link, quality_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (movie_id, server_id, link["language"], link["link"], quality_id))
                logger.debug(
                    f"Enlace insertado: movie_id={movie_id}, server={link['server']}, language={link['language']}")
            else:
                logger.debug(
                    f"Enlace ya existe: movie_id={movie_id}, server={link['server']}, language={link['language']}")

        # Confirmar la transacción
        connection.commit()
        logger.info(f"Datos insertados en la base de datos para la película: {movie['Nombre']}")
    except Exception as e:
        logger.error(f"Error al insertar datos en la base de datos: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# Función principal para extraer todas las páginas de películas
def extract_all_movies():
    if not login():
        logger.error("No se pudo iniciar sesión. Abortando...")
        return

    page_number, last_movie_index = load_progress()

    while True:  # Continuar indefinidamente hasta que se scrapeen todas las páginas
        try:
            page_url = f"{movies_url}/{page_number}" if page_number > 1 else movies_url
            logger.info(f"Extrayendo datos de la página: {page_url}")
            movies, last_movie_index = extract_movies_from_page(page_url, page_number, start_id=1,
                                                                last_movie_index=last_movie_index)

            if not movies and last_movie_index >= 0:  # Si no hay películas nuevas pero se procesaron algunas
                logger.info(
                    f"No se encontraron películas nuevas en la página {page_number}. Pasando a la siguiente página.")
                # Reset last_movie_index when moving to the next page
                last_movie_index = -1
                page_number += 1
                save_progress(page_number, last_movie_index)
            elif not movies:  # Si no hay películas en absoluto
                logger.info(f"No se encontraron películas en la página {page_number}. Finalizando.")
                break  # No hay más películas en esta página, salir del bucle
            else:
                # Reset last_movie_index when moving to the next page
                last_movie_index = -1
                page_number += 1
                save_progress(page_number, last_movie_index)
        except Exception as e:
            logger.error(f"Error: {e}. Intentando nuevamente en 5 minutos...")
            time.sleep(300)  # Esperar 5 minutos antes de intentar nuevamente

            # Reiniciar el driver y la sesión
            global driver
            driver.quit()
            driver = webdriver.Chrome(service=service, options=options)
            if not login():
                logger.error("No se pudo reiniciar la sesión después de un error. Abortando...")
                break


# Punto de entrada principal
if __name__ == "__main__":
    try:
        logger.info("Iniciando el scraper de películas...")
        # Ejecutar la extracción de todas las películas
        extract_all_movies()
        logger.info("Proceso de scraping de películas completado.")
    except Exception as e:
        logger.critical(f"Error crítico en el scraper: {e}")
    finally:
        driver.quit()
        logger.info("Driver de Selenium cerrado.")