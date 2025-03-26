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

# URL de la página de inicio de sesión y de las series
login_url = "https://hdfull.blog/login"
base_url = "https://hdfull.blog"
series_url = "https://hdfull.blog/series/abc/"

# Configuración de Selenium
service = Service('../chromedriver.exe')  # Reemplaza 'path/to/chromedriver' con la ruta a tu chromedriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Ejecuta Chrome en modo headless
driver = webdriver.Chrome(service=service, options=options)

# Archivo para guardar el progreso /progres/"series_progress.json"
progress_file = "../progress/series_progress.json"


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


# Función para verificar si una serie ya existe en la base de datos
def series_exists(title, year, imdb_rating, genre):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        cursor.execute('''
            SELECT id FROM media_downloads 
            WHERE title=? AND year=? AND imdb_rating=? AND genre=? AND type='serie'
        ''', (title, year, imdb_rating, genre))
        result = cursor.fetchone()
        exists = result is not None
        logger.debug(f"Verificación de existencia de serie: {title} ({year}) - {'Existe' if exists else 'No existe'}")
        return exists
    except Exception as e:
        logger.error(f"Error al verificar si la serie existe: {e}")
        return False
    finally:
        cursor.close()
        connection.close()


# Función para verificar si un episodio ya existe en la base de datos
def episode_exists(season_id, episode_number, title):
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
        return exists
    except Exception as e:
        logger.error(f"Error al verificar si el episodio existe: {e}")
        return False
    finally:
        cursor.close()
        connection.close()


# Función para extraer detalles del episodio
def extract_episode_details(episode_url, season_id, episode_id, episode_number, title):
    logger.info(f"Extrayendo detalles del episodio: {episode_url}")
    try:
        driver.get(episode_url)
        time.sleep(2)  # Esperar a que se cargue la página

        # Crear lista para almacenar los enlaces
        server_links = []

        # Encontrar todos los embed-selectors
        embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
        logger.debug(f"Número de enlaces encontrados para episodio {episode_number}: {len(embed_selectors)}")

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
                    "episode_id": episode_id,
                    "server": server,
                    "language": language,
                    "link": embedded_link,
                    "quality": quality
                })

        logger.debug(f"Enlaces obtenidos para el episodio {episode_number}: {len(server_links)}")
        return server_links
    except Exception as e:
        logger.error(f"Error al extraer detalles del episodio {episode_url}: {e}")
        raise


# Función para extraer detalles de la temporada
def extract_season_details(season_url, season_id):
    logger.info(f"Extrayendo detalles de la temporada: {season_url}")
    try:
        driver.get(season_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Verificar si hay episodios en la temporada
        episode_elements = soup.find_all("div", class_="span-6 tt view show-view")
        if not episode_elements:
            logger.warning(f"No se encontraron episodios en la temporada: {season_url}")
            return []

        logger.info(f"Episodios encontrados en esta temporada: {len(episode_elements)}")

        # Crear lista para almacenar los episodios
        episodes = []

        # Conectar a la base de datos
        connection = connect_db()
        cursor = connection.cursor()

        for episode_element in episode_elements:
            try:
                episode_number_tag = episode_element.find("div", class_="rating")
                episode_number_text = episode_number_tag.text.strip() if episode_number_tag else None
                if episode_number_text:
                    episode_number = int(episode_number_text.split("x")[1])
                else:
                    logger.warning("No se pudo extraer el número de episodio. Saltando...")
                    continue

                title_tag = episode_element.find("a", class_="link title-ellipsis")
                title = title_tag['title'].split(' - ')[-1] if title_tag else "No encontrado"

                # Verificar si el episodio ya existe
                if episode_exists(season_id, episode_number, title):
                    logger.info(f"El episodio {episode_number} ya existe. Obteniendo su ID...")
                    cursor.execute('''
                        SELECT id FROM series_episodes WHERE season_id=? AND episode=?
                    ''', (season_id, episode_number))
                    episode_id = cursor.fetchone()['id']
                else:
                    # Insertar el episodio en la base de datos
                    cursor.execute('''
                        INSERT INTO series_episodes (season_id, episode, title)
                        VALUES (?, ?, ?)
                    ''', (season_id, episode_number, title))
                    connection.commit()
                    episode_id = cursor.lastrowid

                    if episode_id == 0:
                        cursor.execute('''
                            SELECT id FROM series_episodes WHERE season_id=? AND episode=?
                        ''', (season_id, episode_number))
                        episode_id = cursor.fetchone()['id']

                logger.info(
                    f"Episodio procesado: season_id={season_id}, episode_number={episode_number}, title={title}, id={episode_id}")

                episode_url = base_url + title_tag['href']
                episode_links = extract_episode_details(episode_url, season_id, episode_id, episode_number, title)

                if episode_links:
                    episodes.append({
                        "episode_id": episode_id,
                        "episode_number": episode_number,
                        "title": title,
                        "url": episode_url,
                        "links": episode_links
                    })

                    # Insertar los enlaces del episodio
                    insert_episode_links(episode_id, episode_links)
            except Exception as e:
                logger.error(f"Error al procesar episodio: {e}")
                continue

        # Cerrar la conexión a la base de datos
        cursor.close()
        connection.close()

        return episodes
    except Exception as e:
        logger.error(f"Error al extraer detalles de la temporada {season_url}: {e}")
        raise


# Función para insertar enlaces de episodios
def insert_episode_links(episode_id, links):
    connection = connect_db()
    cursor = connection.cursor()

    try:
        for link in links:
            # Insertar el servidor si no existe
            cursor.execute('''
                INSERT OR IGNORE INTO servers (name) VALUES (?)
            ''', (link["server"],))
            cursor.execute('''
                SELECT id FROM servers WHERE name=?
            ''', (link["server"],))
            server_row = cursor.fetchone()
            if not server_row:
                logger.error(f"Error: No se encontró el servidor {link['server']}")
                continue
            server_id = server_row["id"]

            # Obtener el ID de la calidad
            cursor.execute('''
                SELECT quality_id FROM qualities WHERE quality=?
            ''', (link["quality"],))
            quality_row = cursor.fetchone()
            if not quality_row:
                # Si la calidad no existe, insertarla
                cursor.execute('''
                    INSERT INTO qualities (quality) VALUES (?)
                ''', (link["quality"],))
                cursor.execute('''
                    SELECT quality_id FROM qualities WHERE quality=?
                ''', (link["quality"],))
                quality_row = cursor.fetchone()
            quality_id = quality_row["quality_id"]

            # Verificar si el enlace ya existe
            cursor.execute('''
                SELECT id FROM links_files_download 
                WHERE episode_id=? AND server_id=? AND language=? AND link=?
            ''', (episode_id, server_id, link["language"], link["link"]))
            link_exists = cursor.fetchone()

            if not link_exists:
                # Insertar el enlace en la base de datos
                cursor.execute('''
                    INSERT INTO links_files_download (episode_id, server_id, language, link, quality_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (episode_id, server_id, link["language"], link["link"], quality_id))
                connection.commit()
                logger.debug(
                    f"Enlace insertado: episode_id={episode_id}, server={link['server']}, language={link['language']}")
            else:
                logger.debug(
                    f"Enlace ya existe: episode_id={episode_id}, server={link['server']}, language={link['language']}")
    except Exception as e:
        logger.error(f"Error al insertar enlaces del episodio: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# Función para verificar si hay una siguiente temporada
def has_next_season(season_url):
    try:
        logger.info(f"Verificando si existe la temporada: {season_url}")
        driver.get(season_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")
        next_season_elements = soup.find_all("div", class_="span-6 tt view show-view")
        exists = len(next_season_elements) > 0
        logger.debug(f"Temporada {season_url}: {'Existe' if exists else 'No existe'}")
        return exists
    except Exception as e:
        logger.error(f"Error al verificar si existe la temporada {season_url}: {e}")
        return False


# Función para extraer detalles de la serie
def extract_series_details(series_url):
    logger.info(f"Extrayendo datos de la serie: {series_url}")
    try:
        driver.get(series_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Extraer el título de la serie
        title_tag = soup.find("div", id="summary-title")
        title = title_tag.text.strip() if title_tag else "No encontrado"
        logger.info(f"Título de la serie: {title}")

        # Extraer año, IMDB rating y género de la serie
        show_details = soup.find("div", class_="show-details")
        year = None
        imdb_rating = None
        genre = None

        if show_details:
            year_tag = show_details.find("a", href=re.compile(r"/buscar/year/"))
            if year_tag:
                year = int(year_tag.text.strip())
            logger.debug(f"Año de la serie: {year}")

            imdb_rating_tag = show_details.find("p", itemprop="aggregateRating")
            if imdb_rating_tag:
                imdb_rating = float(imdb_rating_tag.find("a").text.strip())
            logger.debug(f"IMDB Rating de la serie: {imdb_rating}")

            genre_tags = show_details.find_all("a", href=re.compile(r"/tags-tv"))
            genre = ", ".join([tag.text.strip() for tag in genre_tags])
            logger.debug(f"Género de la serie: {genre}")

        # Verificar si la serie ya existe
        if series_exists(title, year, imdb_rating, genre):
            logger.info(f"La serie '{title}' ({year}) ya existe en la base de datos.")

        # Insertar la serie en la base de datos y obtener su ID
        series_id = insert_series_into_db({
            "title": title,
            "year": year,
            "imdb_rating": imdb_rating,
            "genre": genre,
            "type": "serie"  # Indicar que es de tipo 'serie'
        })

        if not series_id:
            logger.error(f"Error al insertar la serie: {title}")
            return None

        # Crear lista para almacenar las temporadas
        seasons = []
        season_number = 1

        while True:
            season_url = f"{series_url}/temporada-{season_number}"
            logger.info(f"Verificando temporada {season_number}: {season_url}")

            if not has_next_season(season_url):
                logger.info(f"No se encontró la temporada {season_number}. Finalizando.")
                break

            season_id = insert_season_into_db(series_id, season_number)

            if not season_id:
                logger.error(f"Error al insertar la temporada {season_number} para la serie ID: {series_id}")
                season_number += 1
                continue

            season_episodes = extract_season_details(season_url, season_id)

            seasons.append({
                "season_id": season_id,
                "season_number": season_number,
                "episodes": season_episodes
            })

            # Guardar progreso después de cada temporada
            save_progress("series", series_url, season_number)

            season_number += 1

        return {
            "series_id": series_id,
            "title": title,
            "year": year,
            "imdb_rating": imdb_rating,
            "genre": genre,
            "seasons": seasons
        }
    except Exception as e:
        logger.error(f"Error al extraer detalles de la serie {series_url}: {e}")
        raise


# Función para insertar la serie en la base de datos
def insert_series_into_db(series):
    connection = connect_db()
    cursor = connection.cursor()
    series_id = None

    try:
        cursor.execute('''
            INSERT INTO media_downloads (title, year, imdb_rating, genre, type)
            VALUES (?, ?, ?, ?, ?)
        ''', (series["title"], series["year"], series["imdb_rating"], series["genre"], series["type"]))

        # Obtener el ID de la serie insertada
        series_id = cursor.lastrowid

        # Si es una actualización, obtener el ID existente
        if series_id == 0:
            cursor.execute('''
                SELECT id FROM media_downloads 
                WHERE title=? AND year=?
                AND imdb_rating=? AND genre=? AND type=?
            ''', (series["title"], series["year"], series["imdb_rating"], series["genre"], series["type"]))
            result = cursor.fetchone()
            series_id = result['id'] if result else None

        connection.commit()  # Confirmar la transacción
        logger.info(f"Serie insertada/actualizada: {series['title']} con ID: {series_id} de tipo: {series['type']}")
    except Exception as e:
        logger.error(f"Error al insertar la serie: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    return series_id


# Función para insertar la temporada en la base de datos
def insert_season_into_db(series_id, season_number):
    connection = connect_db()
    cursor = connection.cursor()
    season_id = None

    try:
        cursor.execute('''
            INSERT INTO series_seasons (movie_id, season)
            VALUES (?, ?)
        ''', (series_id, season_number))

        # Obtener el ID de la temporada
        season_id = cursor.lastrowid

        # Si es una actualización, obtener el ID existente
        if season_id == 0:
            cursor.execute('''
                SELECT id FROM series_seasons 
                WHERE movie_id=? AND season=?
            ''', (series_id, season_number))
            result = cursor.fetchone()
            season_id = result['id'] if result else None

        connection.commit()  # Confirmar la transacción
        logger.info(
            f"Temporada insertada/actualizada: {season_number} para serie ID: {series_id} con temporada ID: {season_id}")
    except Exception as e:
        logger.error(f"Error al insertar la temporada: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    return season_id


# Función para guardar el progreso
def save_progress(type_content, current_url, current_index):
    try:
        progress = {}
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)

        progress[type_content] = {
            'current_url': current_url,
            'current_index': current_index,
            'last_update': time.strftime('%Y-%m-%d %H:%M:%S')
        }

        with open(progress_file, 'w') as f:
            json.dump(progress, f)

        logger.debug(f"Progreso guardado: {type_content}, índice {current_index}, URL {current_url}")
    except Exception as e:
        logger.error(f"Error al guardar el progreso: {e}")


# Función para cargar el progreso
def load_progress(type_content):
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                if type_content in progress:
                    logger.info(
                        f"Progreso cargado para {type_content}: índice {progress[type_content]['current_index']}")
                    return progress[type_content]['current_url'], progress[type_content]['current_index']

        logger.info(f"No se encontró progreso para {type_content}. Comenzando desde el principio.")
        return None, None
    except Exception as e:
        logger.error(f"Error al cargar el progreso: {e}")
        return None, None


# Función para extraer todas las series de una página
def extract_series_from_page(page_url, letter):
    logger.info(f"Extrayendo series de la página: {page_url}")
    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")
        series = []

        # Buscar todos los divs de las series
        series_divs = soup.find_all("div", class_="span-6 inner-6 tt view")
        logger.info(f"Encontradas {len(series_divs)} series en la página {page_url}")

        # Cargar el progreso para esta letra
        last_url, last_index = load_progress(f"letter_{letter}")
        start_index = 0

        if last_url and last_index:
            # Encontrar el índice donde dejamos la última vez
            for i, div in enumerate(series_divs):
                series_url = base_url + div.find("a")['href']
                if series_url == last_url:
                    start_index = i + 1  # Comenzar desde la siguiente serie
                    break

        logger.info(f"Comenzando desde el índice {start_index} para la letra {letter}")

        # Iterar sobre las series desde el punto donde dejamos
        for index, series_div in enumerate(series_divs[start_index:], start_index):
            series_url = base_url + series_div.find("a")['href']
            logger.info(f"Extrayendo datos de la serie {index + 1}/{len(series_divs)}: {series_url}")

            success = False
            for attempt in range(3):
                try:
                    series_details = extract_series_details(series_url)
                    if series_details:
                        series.append(series_details)
                    success = True
                    break
                except Exception as e:
                    logger.error(f"Error al extraer datos de la serie {series_url}: {e}")
                    if attempt < 2:
                        logger.info(f"Reintentando en 2 minutos... (Intento {attempt + 1}/3)")
                        time.sleep(120)  # Esperar 2 minutos antes de reintentar

            if not success:
                logger.warning(
                    f"No se pudo extraer la serie {series_url} después de 3 intentos. Continuando con la siguiente.")

            # Guardar progreso después de cada serie
            save_progress(f"letter_{letter}", series_url, index)

        return series
    except Exception as e:
        logger.error(f"Error al extraer series de la página {page_url}: {e}")
        raise


# Función principal para extraer todas las páginas de series
def extract_all_series():
    if not login():
        logger.error("No se pudo iniciar sesión. Abortando...")
        return []

    all_series = []
    alphabet = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ#")

    # Cargar el progreso general
    last_letter, _ = load_progress("general")
    start_index = 0

    if last_letter:
        try:
            start_index = alphabet.index(last_letter) + 1  # Comenzar desde la siguiente letra
            if start_index >= len(alphabet):  # Si ya terminamos todas las letras, empezar de nuevo
                start_index = 0
        except ValueError:
            start_index = 0

    logger.info(f"Comenzando desde la letra {alphabet[start_index]}")

    # Procesar todas las letras desde el punto donde dejamos
    for letter in alphabet[start_index:] + alphabet[:start_index]:
        page_url = f"{series_url}{letter}"
        logger.info(f"Procesando letra {letter}: {page_url}")

        try:
            series = extract_series_from_page(page_url, letter)
            all_series.extend(series)

            # Guardar progreso después de cada letra
            save_progress("general", letter, 0)
        except Exception as e:
            logger.error(f"Error al procesar la letra {letter}: {e}")
            # Reiniciar el driver y la sesión
            global driver
            driver.quit()
            driver = webdriver.Chrome(service=service, options=options)
            if not login():
                logger.error("No se pudo reiniciar la sesión después de un error. Abortando...")
                break

    return all_series


# Punto de entrada principal
if __name__ == "__main__":
    try:
        logger.info("Iniciando el scraper de series...")
        # Ejecutar la extracción de todas las series sin límite
        all_series = extract_all_series()
        logger.info(f"Proceso de scraping de series completado. Series extraídas: {len(all_series)}")
    except Exception as e:
        logger.critical(f"Error crítico en el scraper: {e}")
    finally:
        driver.quit()
        logger.info("Driver de Selenium cerrado.")