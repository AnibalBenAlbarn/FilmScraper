import time
import re
import pymysql
import json
import os
import logging
import smtplib
import concurrent.futures
import argparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Configuración del logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Crear handlers
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('../logs/logs/update_episodes_scraper.log')
file_handler.setLevel(logging.INFO)

# Crear formato y agregarlo a los handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Agregar los handlers al logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Credenciales de inicio de sesión
username = 'rolankor'
password = 'Rolankor_09'

# URL de la página de inicio de sesión y de los episodios
login_url = "https://hdfull.blog/login"
base_url = "https://hdfull.blog"
new_episodes_url = "https://hdfull.blog/episodios#premiere"

# Archivo para guardar el progreso
progress_file = "../progress/update_episodes_progress.json"

# Configuración de correo electrónico para notificaciones
email_config = {
    "enabled": True,
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": "tu_correo@gmail.com",  # Cambia esto por tu correo
    "sender_password": "tu_contraseña",  # Cambia esto por tu contraseña o clave de aplicación
    "recipient_email": "destinatario@gmail.com"  # Cambia esto por el correo del destinatario
}

# Configuración de paralelización
max_workers = 4  # Número máximo de workers para procesamiento paralelo
max_retries = 3  # Número máximo de reintentos para cada episodio


# Función para crear un driver de Selenium
def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Ejecuta Chrome en modo headless
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service('chromedriver.exe')  # Reemplaza con la ruta a tu chromedriver
    driver = webdriver.Chrome(service=service, options=options)
    return driver


# Función para conectar a la base de datos
def connect_db():
    try:
        connection = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password="Rolankor_09",
            database="direct_dw_bd",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor
        )
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


# Función para buscar una serie por título y año
def find_series_by_title_year(title, year=None):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        if year:
            cursor.execute('''
                SELECT id, title, year, imdb_rating, genre FROM media_downloads 
                WHERE title=%s AND year=%s AND type='serie'
            ''', (title, year))
        else:
            cursor.execute('''
                SELECT id, title, year, imdb_rating, genre FROM media_downloads 
                WHERE title=%s AND type='serie'
            ''', (title,))

        results = cursor.fetchall()

        if not results:
            logger.debug(f"Serie no encontrada: {title} ({year if year else 'año desconocido'})")
            return None

        # Si hay múltiples resultados, devolver el primero
        logger.debug(f"Serie encontrada: {title} ({results[0]['year']}) con ID {results[0]['id']}")
        return results[0]
    except Exception as e:
        logger.error(f"Error al buscar serie por título y año: {e}")
        return None
    finally:
        cursor.close()
        connection.close()


# Función para verificar si una temporada existe
def season_exists(series_id, season_number):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        cursor.execute('''
            SELECT id FROM series_seasons 
            WHERE movie_id=%s AND season=%s
        ''', (series_id, season_number))
        result = cursor.fetchone()
        exists = result is not None
        logger.debug(
            f"Verificación de existencia de temporada: serie_id={series_id}, temporada={season_number} - {'Existe' if exists else 'No existe'}")
        return exists, result['id'] if exists else None
    except Exception as e:
        logger.error(f"Error al verificar si la temporada existe: {e}")
        return False, None
    finally:
        cursor.close()
        connection.close()


# Función para verificar si un episodio existe
def episode_exists(season_id, episode_number, title=None):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        if title:
            cursor.execute('''
                SELECT id FROM series_episodes 
                WHERE season_id=%s AND episode=%s AND title=%s
            ''', (season_id, episode_number, title))
        else:
            cursor.execute('''
                SELECT id FROM series_episodes 
                WHERE season_id=%s AND episode=%s
            ''', (season_id, episode_number))

        result = cursor.fetchone()
        exists = result is not None
        logger.debug(
            f"Verificación de existencia de episodio: season_id={season_id}, episodio={episode_number} - {'Existe' if exists else 'No existe'}")
        return exists, result['id'] if exists else None
    except Exception as e:
        logger.error(f"Error al verificar si el episodio existe: {e}")
        return False, None
    finally:
        cursor.close()
        connection.close()


# Función para verificar si un enlace ya existe en la base de datos
def link_exists(episode_id, server_id, language, link):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        cursor.execute('''
            SELECT id FROM links_files_download 
            WHERE episode_id=%s AND server_id=%s AND language=%s AND link=%s
        ''', (episode_id, server_id, language, link))
        result = cursor.fetchone()
        exists = result is not None
        return exists
    except Exception as e:
        logger.error(f"Error al verificar si el enlace existe: {e}")
        return False
    finally:
        cursor.close()
        connection.close()


# Función para insertar una nueva serie
def insert_series(title, year, imdb_rating=None, genre=None):
    connection = connect_db()
    cursor = connection.cursor()
    series_id = None

    try:
        cursor.execute('''
            INSERT INTO media_downloads (title, year, imdb_rating, genre, type, created_at, updated_at)
            VALUES (%s, %s, %s, %s, 'serie', NOW(), NOW())
        ''', (title, year, imdb_rating, genre))
        series_id = cursor.lastrowid
        connection.commit()
        logger.info(f"Nueva serie insertada: {title} ({year}) con ID {series_id}")
    except Exception as e:
        logger.error(f"Error al insertar nueva serie: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    return series_id


# Función para insertar una nueva temporada
def insert_season(series_id, season_number):
    connection = connect_db()
    cursor = connection.cursor()
    season_id = None

    try:
        cursor.execute('''
            INSERT INTO series_seasons (movie_id, season)
            VALUES (%s, %s)
        ''', (series_id, season_number))
        season_id = cursor.lastrowid
        connection.commit()
        logger.info(
            f"Nueva temporada insertada: serie_id={series_id}, temporada={season_number}, season_id={season_id}")
    except Exception as e:
        logger.error(f"Error al insertar nueva temporada: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    return season_id


# Función para insertar un nuevo episodio
def insert_episode(season_id, episode_number, title):
    connection = connect_db()
    cursor = connection.cursor()
    episode_id = None

    try:
        cursor.execute('''
            INSERT INTO series_episodes (season_id, episode, title)
            VALUES (%s, %s, %s)
        ''', (season_id, episode_number, title))
        episode_id = cursor.lastrowid
        connection.commit()
        logger.info(
            f"Nuevo episodio insertado: season_id={season_id}, episodio={episode_number}, título={title}, episode_id={episode_id}")
    except Exception as e:
        logger.error(f"Error al insertar nuevo episodio: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    return episode_id


# Función para insertar enlaces de episodios
def insert_links(links):
    connection = connect_db()
    cursor = connection.cursor()

    try:
        for link in links:
            # Insertar el servidor si no existe
            cursor.execute('''
                INSERT IGNORE INTO servers (name) VALUES (%s)
            ''', (link["server"],))
            cursor.execute('''
                SELECT id FROM servers WHERE name=%s
            ''', (link["server"],))
            server_row = cursor.fetchone()
            if not server_row:
                logger.error(f"Error: No se encontró el servidor {link['server']}")
                continue
            server_id = server_row["id"]

            # Obtener el ID de la calidad
            cursor.execute('''
                SELECT quality_id FROM qualities WHERE quality=%s
            ''', (link["quality"],))
            quality_row = cursor.fetchone()
            if not quality_row:
                logger.error(f"Error: No se encontró la calidad {link['quality']}")
                continue
            quality_id = quality_row["quality_id"]

            # Verificar si el enlace ya existe
            if not link_exists(link["episode_id"], server_id, link["language"], link["link"]):
                # Insertar el enlace en la base de datos
                cursor.execute('''
                    INSERT INTO links_files_download (episode_id, server_id, language, link, quality_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                ''', (link["episode_id"], server_id, link["language"], link["link"], quality_id))
                connection.commit()
                logger.debug(
                    f"Enlace insertado: episode_id={link['episode_id']}, server={link['server']}, language={link['language']}")
            else:
                logger.debug(
                    f"Enlace ya existe: episode_id={link['episode_id']}, server={link['server']}, language={link['language']}")
    except Exception as e:
        logger.error(f"Error al insertar enlaces: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# Función para extraer detalles del episodio
def extract_episode_details(episode_url, worker_id=0):
    logger.info(f"[Worker {worker_id}] Extrayendo detalles del episodio: {episode_url}")

    # Crear un nuevo driver para este worker
    driver = create_driver()

    try:
        # Iniciar sesión con este driver
        if not login(driver):
            logger.error(f"[Worker {worker_id}] No se pudo iniciar sesión. Abortando extracción de {episode_url}")
            driver.quit()
            return None

        driver.get(episode_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Extraer información de la serie
        series_info = soup.find("div", class_="show-data")
        if not series_info:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar información de la serie en {episode_url}")
            driver.quit()
            return None

        # Extraer título de la serie
        series_title_tag = series_info.find("a", href=re.compile(r"/serie/"))
        if not series_title_tag:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar el título de la serie en {episode_url}")
            driver.quit()
            return None

        series_title = series_title_tag.text.strip()
        series_url = base_url + series_title_tag['href']

        # Extraer información del episodio
        episode_info = soup.find("div", id="summary-title")
        if not episode_info:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar información del episodio en {episode_url}")
            driver.quit()
            return None

        # El formato suele ser "1x01 - Título del episodio"
        episode_info_text = episode_info.text.strip()
        episode_match = re.match(r'(\d+)x(\d+)\s*-\s*(.*)', episode_info_text)

        if not episode_match:
            logger.error(f"[Worker {worker_id}] No se pudo extraer la información del episodio: {episode_info_text}")
            driver.quit()
            return None

        season_number = int(episode_match.group(1))
        episode_number = int(episode_match.group(2))
        episode_title = episode_match.group(3).strip()

        logger.info(
            f"[Worker {worker_id}] Información extraída: Serie={series_title}, Temporada={season_number}, Episodio={episode_number}, Título={episode_title}")

        # Extraer año de la serie (necesitamos visitar la página de la serie)
        series_year = None
        try:
            driver.get(series_url)
            time.sleep(2)
            series_page_source = driver.page_source
            series_soup = BeautifulSoup(series_page_source, "lxml")

            show_details = series_soup.find("div", class_="show-details")
            if show_details:
                year_tag = show_details.find("a", href=re.compile(r"/buscar/year/"))
                if year_tag:
                    series_year = int(year_tag.text.strip())
                    logger.debug(f"[Worker {worker_id}] Año de la serie extraído: {series_year}")

                # También podemos extraer rating y género mientras estamos aquí
                imdb_rating_tag = show_details.find("p", itemprop="aggregateRating")
                imdb_rating = None
                if imdb_rating_tag:
                    rating_text = imdb_rating_tag.find("a").text.strip()
                    try:
                        imdb_rating = float(rating_text)
                        logger.debug(f"[Worker {worker_id}] IMDB Rating extraído: {imdb_rating}")
                    except ValueError:
                        logger.warning(f"[Worker {worker_id}] No se pudo convertir el rating a float: {rating_text}")

                genre_tags = show_details.find_all("a", href=re.compile(r"/tags-tv"))
                genre = ", ".join([tag.text.strip() for tag in genre_tags]) if genre_tags else None
                logger.debug(f"[Worker {worker_id}] Género extraído: {genre}")
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error al extraer información adicional de la serie: {e}")
            # Continuamos aunque no podamos obtener el año

        # Buscar la serie en la base de datos
        series_data = find_series_by_title_year(series_title, series_year)
        series_id = None

        # Si la serie no existe, la insertamos
        if not series_data:
            logger.info(
                f"[Worker {worker_id}] Serie no encontrada en la base de datos. Insertando nueva serie: {series_title} ({series_year})")
            series_id = insert_series(series_title, series_year, imdb_rating, genre)
            if not series_id:
                logger.error(f"[Worker {worker_id}] Error al insertar la serie. Abortando.")
                driver.quit()
                return None
        else:
            series_id = series_data['id']
            logger.info(f"[Worker {worker_id}] Serie encontrada en la base de datos con ID {series_id}")

        # Verificar si la temporada existe
        season_exists_flag, season_id = season_exists(series_id, season_number)

        # Si la temporada no existe, la insertamos
        if not season_exists_flag:
            logger.info(f"[Worker {worker_id}] Temporada {season_number} no encontrada. Insertando nueva temporada.")
            season_id = insert_season(series_id, season_number)
            if not season_id:
                logger.error(f"[Worker {worker_id}] Error al insertar la temporada. Abortando.")
                driver.quit()
                return None
        else:
            logger.info(f"[Worker {worker_id}] Temporada {season_number} encontrada con ID {season_id}")

        # Verificar si el episodio existe
        episode_exists_flag, episode_id = episode_exists(season_id, episode_number, episode_title)

        # Si el episodio no existe, lo insertamos
        if not episode_exists_flag:
            logger.info(f"[Worker {worker_id}] Episodio {episode_number} no encontrado. Insertando nuevo episodio.")
            episode_id = insert_episode(season_id, episode_number, episode_title)
            if not episode_id:
                logger.error(f"[Worker {worker_id}] Error al insertar el episodio. Abortando.")
                driver.quit()
                return None
        else:
            logger.info(f"[Worker {worker_id}] Episodio {episode_number} encontrado con ID {episode_id}")

        # Volver a la página del episodio para extraer los enlaces
        driver.get(episode_url)
        time.sleep(2)

        # Crear lista para almacenar los enlaces
        server_links = []

        # Encontrar todos los embed-selectors
        embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
        logger.debug(f"[Worker {worker_id}] Número de enlaces encontrados: {len(embed_selectors)}")

        for embed_selector in embed_selectors:
            language = None
            server = None
            embedded_link = None

            try:
                embed_selector.click()
                time.sleep(2)  # Esperar 2 segundos para que el contenido se cargue
            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error al hacer clic en el embed-selector: {e}")
                continue

            try:
                embed_movie = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'embed-movie'))
                )
                iframe = embed_movie.find_element(By.TAG_NAME, 'iframe')
                embedded_link = iframe.get_attribute('src')
                logger.debug(f"[Worker {worker_id}] Enlace embebido extraído: {embedded_link}")
            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error al obtener el enlace embebido: {e}")
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
            if server and language and embedded_link:
                server_links.append({
                    "episode_id": episode_id,
                    "server": server,
                    "language": language,
                    "link": embedded_link,
                    "quality": quality
                })

        # Insertar los enlaces en la base de datos
        if server_links:
            insert_links(server_links)
            logger.info(f"[Worker {worker_id}] Se insertaron {len(server_links)} enlaces para el episodio")
        else:
            logger.warning(f"[Worker {worker_id}] No se encontraron enlaces para el episodio")

        # Cerrar el driver
        driver.quit()

        return {
            "series_id": series_id,
            "series_title": series_title,
            "series_year": series_year,
            "season_id": season_id,
            "season_number": season_number,
            "episode_id": episode_id,
            "episode_number": episode_number,
            "episode_title": episode_title,
            "links": server_links,
            "is_new_series": not series_data,
            "is_new_season": not season_exists_flag,
            "is_new_episode": not episode_exists_flag
        }
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles del episodio {episode_url}: {e}")
        if driver:
            driver.quit()
        raise


# Función para obtener URLs de episodios de la página de estrenos
def get_episode_urls_from_premiere_page(driver):
    logger.info("Obteniendo URLs de episodios de estreno...")
    try:
        driver.get(new_episodes_url)
        time.sleep(3)  # Esperar a que se cargue la página y el contenido dinámico

        # Hacer clic en la pestaña "Estrenos" si es necesario
        try:
            premiere_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Estrenos')]"))
            )
            premiere_tab.click()
            time.sleep(2)  # Esperar a que se cargue el contenido
        except Exception as e:
            logger.warning(f"No se pudo hacer clic en la pestaña 'Estrenos': {e}")
            # Continuamos de todos modos, ya que podríamos estar ya en la pestaña correcta

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Buscar todos los divs de los episodios
        episode_divs = soup.find_all("div", class_="span-6 tt view show-view")
        logger.info(f"Encontrados {len(episode_divs)} episodios de estreno")

        episode_urls = []
        for episode_div in episode_divs:
            link_tag = episode_div.find("a", href=re.compile(r"/episodio/"))
            if link_tag:
                episode_href = link_tag['href']
                episode_url = base_url + episode_href
                episode_urls.append(episode_url)

        return episode_urls
    except Exception as e:
        logger.error(f"Error al obtener URLs de episodios de estreno: {e}")
        return []


# Función para procesar un episodio con reintentos
def process_episode_with_retries(episode_url, worker_id):
    for attempt in range(max_retries):
        try:
            return extract_episode_details(episode_url, worker_id)
        except Exception as e:
            logger.error(
                f"[Worker {worker_id}] Error al procesar el episodio {episode_url} (intento {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(30)  # Esperar 30 segundos antes de reintentar

    logger.error(
        f"[Worker {worker_id}] No se pudo procesar el episodio {episode_url} después de {max_retries} intentos")
    return None


# Función para procesar episodios en paralelo
def process_episodes_in_parallel(episode_urls):
    logger.info(f"Procesando {len(episode_urls)} episodios en paralelo con {max_workers} workers")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Crear un diccionario de futuros/tareas
        future_to_url = {
            executor.submit(process_episode_with_retries, url, i % max_workers): url
            for i, url in enumerate(episode_urls)
        }

        # Procesar los resultados a medida que se completan
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                episode_data = future.result()
                if episode_data:
                    results.append(episode_data)
                    logger.info(f"Episodio procesado correctamente: {url}")
            except Exception as e:
                logger.error(f"Error al procesar el episodio {url}: {e}")

    return results


# Función para guardar el progreso
def save_progress(processed_urls):
    try:
        progress = {
            'processed_urls': processed_urls,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        with open(progress_file, 'w') as f:
            json.dump(progress, f)

        logger.debug(f"Progreso guardado: {len(processed_urls)} URLs procesadas")
    except Exception as e:
        logger.error(f"Error al guardar el progreso: {e}")


# Función para cargar el progreso
def load_progress():
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                logger.info(
                    f"Progreso cargado: {len(progress.get('processed_urls', []))} URLs procesadas anteriormente")
                return progress.get('processed_urls', [])

        logger.info("No se encontró archivo de progreso. Comenzando desde el principio.")
        return []
    except Exception as e:
        logger.error(f"Error al cargar el progreso: {e}")
        return []


# Función para enviar notificación por correo electrónico
def send_email_notification(subject, message):
    if not email_config["enabled"]:
        logger.info("Notificaciones por correo electrónico desactivadas")
        return False

    try:
        msg = MIMEMultipart()
        msg['From'] = email_config["sender_email"]
        msg['To'] = email_config["recipient_email"]
        msg['Subject'] = subject

        msg.attach(MIMEText(message, 'plain'))

        server = smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"])
        server.starttls()
        server.login(email_config["sender_email"], email_config["sender_password"])
        server.send_message(msg)
        server.quit()

        logger.info(f"Notificación enviada: {subject}")
        return True
    except Exception as e:
        logger.error(f"Error al enviar notificación por correo electrónico: {e}")
        return False


# Función para generar informe de actualización
def generate_update_report(start_time, processed_episodes):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos

    # Contar episodios nuevos, temporadas nuevas y series nuevas
    new_episodes_count = sum(1 for ep in processed_episodes if ep.get("is_new_episode", False))
    new_seasons_count = sum(1 for ep in processed_episodes if ep.get("is_new_season", False))
    new_series_count = sum(1 for ep in processed_episodes if ep.get("is_new_series", False))

    # Contar enlaces nuevos
    new_links_count = sum(len(ep.get("links", [])) for ep in processed_episodes)

    # Generar informe
    report = f"""
INFORME DE ACTUALIZACIÓN DE EPISODIOS - {end_time.strftime('%Y-%m-%d %H:%M:%S')}
===========================================================================

Duración: {duration:.2f} minutos

RESUMEN:
- Episodios procesados: {len(processed_episodes)}
- Nuevas series añadidas: {new_series_count}
- Nuevas temporadas añadidas: {new_seasons_count}
- Nuevos episodios añadidos: {new_episodes_count}
- Total de nuevos enlaces: {new_links_count}

DETALLES:
"""

    # Agrupar episodios por serie
    episodes_by_series = {}
    for ep in processed_episodes:
        series_title = ep.get("series_title", "Desconocida")
        if series_title not in episodes_by_series:
            episodes_by_series[series_title] = []
        episodes_by_series[series_title].append(ep)

    # Añadir detalles de cada serie
    for series_title, episodes in episodes_by_series.items():
        report += f"\\n{series_title} ({episodes[0].get('series_year', 'Año desconocido')}):\\n"

        # Agrupar episodios por temporada
        episodes_by_season = {}
        for ep in episodes:
            season_number = ep.get("season_number", 0)
            if season_number not in episodes_by_season:
                episodes_by_season[season_number] = []
            episodes_by_season[season_number].append(ep)

        # Añadir detalles de cada temporada
        for season_number, season_episodes in sorted(episodes_by_season.items()):
            report += f"  Temporada {season_number}:\\n"

            # Añadir detalles de cada episodio
            for ep in sorted(season_episodes, key=lambda x: x.get("episode_number", 0)):
                status = []
                if ep.get("is_new_series", False):
                    status.append("NUEVA SERIE")
                if ep.get("is_new_season", False):
                    status.append("NUEVA TEMPORADA")
                if ep.get("is_new_episode", False):
                    status.append("NUEVO EPISODIO")

                status_str = f" ({', '.join(status)})" if status else ""
                report += f"    {ep.get('episode_number', '?')}. {ep.get('episode_title', 'Sin título')} - {len(ep.get('links', []))} enlaces{status_str}\\n"

    report += """
===========================================================================
Este es un mensaje automático generado por el sistema de actualización de episodios.
"""

    return report


# Función para registrar estadísticas de actualización
def log_update_stats(start_time, processed_episodes):
    try:
        connection = connect_db()
        cursor = connection.cursor()

        # Obtener estadísticas de la actualización actual
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # en minutos

        # Contar episodios nuevos, temporadas nuevas y series nuevas
        new_episodes_count = sum(1 for ep in processed_episodes if ep.get("is_new_episode", False))
        new_seasons_count = sum(1 for ep in processed_episodes if ep.get("is_new_season", False))
        new_series_count = sum(1 for ep in processed_episodes if ep.get("is_new_series", False))

        # Contar enlaces nuevos
        new_links_count = sum(len(ep.get("links", [])) for ep in processed_episodes)

        # Registrar estadísticas en la base de datos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS episode_update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes FLOAT,
                new_series INT,
                new_seasons INT,
                new_episodes INT,
                new_links INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            INSERT INTO episode_update_stats (update_date, duration_minutes, new_series, new_seasons, new_episodes, new_links)
            VALUES (CURDATE(), %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                duration_minutes = duration_minutes + %s,
                new_series = new_series + %s,
                new_seasons = new_seasons + %s,
                new_episodes = new_episodes + %s,
                new_links = new_links + %s
        ''', (
            duration, new_series_count, new_seasons_count, new_episodes_count, new_links_count,
            duration, new_series_count, new_seasons_count, new_episodes_count, new_links_count
        ))

        connection.commit()

        logger.info(
            f"Estadísticas de actualización: Duración={duration:.2f} minutos, Nuevas series={new_series_count}, Nuevas temporadas={new_seasons_count}, Nuevos episodios={new_episodes_count}, Nuevos enlaces={new_links_count}")
        return {
            "duration": duration,
            "new_series": new_series_count,
            "new_seasons": new_seasons_count,
            "new_episodes": new_episodes_count,
            "new_links": new_links_count
        }
    except Exception as e:
        logger.error(f"Error al registrar estadísticas de actualización: {e}")
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()


# Función para verificar y crear tablas necesarias
def setup_database():
    connection = connect_db()
    cursor = connection.cursor()

    try:
        # Verificar si existe la tabla de estadísticas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS episode_update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes FLOAT,
                new_series INT,
                new_seasons INT,
                new_episodes INT,
                new_links INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Verificar si las columnas created_at y updated_at existen en media_downloads
        cursor.execute("SHOW COLUMNS FROM media_downloads LIKE 'created_at'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE media_downloads ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        cursor.execute("SHOW COLUMNS FROM media_downloads LIKE 'updated_at'")
        if not cursor.fetchone():
            cursor.execute(
                "ALTER TABLE media_downloads ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

        # Verificar si la columna created_at existe en links_files_download
        cursor.execute("SHOW COLUMNS FROM links_files_download LIKE 'created_at'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE links_files_download ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        connection.commit()
        logger.info("Base de datos configurada correctamente")
    except Exception as e:
        logger.error(f"Error al configurar la base de datos: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# Función principal para procesar episodios de estreno
def process_premiere_episodes():
    start_time = datetime.now()
    logger.info(f"Iniciando actualización de episodios de estreno: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Configurar la base de datos
        setup_database()

        # Crear un driver principal para obtener las URLs de los episodios
        main_driver = create_driver()
        if not login(main_driver):
            logger.error("No se pudo iniciar sesión. Abortando procesamiento de episodios de estreno.")
            main_driver.quit()
            return []

        # Obtener URLs de episodios de estreno
        episode_urls = get_episode_urls_from_premiere_page(main_driver)
        main_driver.quit()

        if not episode_urls:
            logger.warning("No se encontraron episodios de estreno. Finalizando.")
            return []

        # Cargar progreso anterior
        processed_urls = load_progress()

        # Filtrar URLs ya procesadas
        new_urls = [url for url in episode_urls if url not in processed_urls]
        logger.info(f"Encontrados {len(new_urls)} episodios nuevos para procesar")

        if not new_urls:
            logger.info("No hay episodios nuevos para procesar. Finalizando.")
            return []

        # Procesar episodios en paralelo
        processed_episodes = process_episodes_in_parallel(new_urls)

        # Actualizar la lista de URLs procesadas
        processed_urls.extend(new_urls)

        # Guardar progreso
        save_progress(processed_urls)

        # Registrar estadísticas
        stats = log_update_stats(start_time, processed_episodes)

        # Generar y enviar informe
        if email_config["enabled"] and processed_episodes:
            report = generate_update_report(start_time, processed_episodes)
            send_email_notification(
                f"Informe de actualización de episodios - {datetime.now().strftime('%Y-%m-%d')}",
                report
            )

        logger.info(f"Actualización de episodios completada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return processed_episodes
    except Exception as e:
        logger.critical(f"Error crítico en la actualización de episodios: {e}")

        # Enviar notificación de error
        if email_config["enabled"]:
            send_email_notification(
                f"ERROR CRÍTICO - Actualización de episodios - {datetime.now().strftime('%Y-%m-%d')}",
                f"Se ha producido un error crítico durante la actualización de episodios:\\n\\n{str(e)}\\n\\nPor favor, revise los logs para más detalles."
            )

        return []


# Crear archivo de programación automática para Windows
def create_scheduler_script():
    import sys
    script_path = os.path.abspath(__file__)
    bat_file_path = os.path.join(os.path.dirname(script_path), "schedule_episode_updates.bat")

    with open(bat_file_path, "w") as f:
        f.write(f"""@echo off
echo Iniciando actualizacion de episodios...
python "{script_path}"
echo Actualizacion completada.
pause
""")

    # Crear archivo XML para Task Scheduler
    xml_file_path = os.path.join(os.path.dirname(script_path), "episode_update_task.xml")

    # Obtener la ruta completa al intérprete de Python
    python_path = sys.executable

    with open(xml_file_path, "w") as f:
        f.write(f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>2023-06-15T12:00:00</Date>
    <Author>Sistema de Actualización de Episodios</Author>
    <Description>Tarea semanal para actualizar la base de datos de episodios</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2023-06-15T04:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByWeek>
        <DaysOfWeek>
          <Sunday />
        </DaysOfWeek>
        <WeeksInterval>1</WeeksInterval>
      </ScheduleByWeek>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>true</WakeToRun>
    <ExecutionTimeLimit>PT4H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>"{python_path}"</Command>
      <Arguments>"{script_path}"</Arguments>
      <WorkingDirectory>{os.path.dirname(script_path)}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
""")

    print(f"""
Se han creado los archivos para programar la tarea:
1. {bat_file_path} - Archivo batch para ejecución manual
2. {xml_file_path} - Archivo XML para importar en el Programador de tareas

Para programar la tarea automáticamente:
1. Abra el Programador de tareas de Windows (taskschd.msc)
2. Haga clic en "Importar tarea..." en el menú de la derecha
3. Seleccione el archivo XML: {xml_file_path}
4. Ajuste la configuración según sea necesario (hora de inicio, frecuencia, etc.)
5. Haga clic en "Aceptar" para guardar la tarea

La tarea está configurada para ejecutarse cada domingo a las 4:00 AM.
""")


# Punto de entrada principal
if __name__ == "__main__":
    import sys

    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Actualización de episodios de estreno')
    parser.add_argument('--create-scheduler', action='store_true',
                        help='Crear archivos para programar la tarea en Windows')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')

    args = parser.parse_args()

    # Actualizar configuración de paralelización si se especifica
    if args.max_workers:
        max_workers = args.max_workers

    # Crear archivos para programar la tarea
    if args.create_scheduler:
        create_scheduler_script()
    else:
        # Ejecutar la actualización de episodios
        process_premiere_episodes()