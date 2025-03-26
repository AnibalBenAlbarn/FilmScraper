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

file_handler = logging.FileHandler('../logs/update_movies_scraper.log')
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

# URL de la página de inicio de sesión y de las películas
login_url = "https://hdfull.blog/login"
base_url = "https://hdfull.blog"
new_movies_url = "https://hdfull.blog/peliculas-estreno"
updated_movies_url = "https://hdfull.blog/peliculas-actualizadas"

#Archivo para guardar
progress_file = "../progress/update_movies_progress.json"

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
max_retries = 3  # Número máximo de reintentos para cada película


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


# Función para verificar si una película ya existe en la base de datos
def movie_exists(title, year, imdb_rating, genre):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        cursor.execute('''
            SELECT id, title, year, imdb_rating, genre FROM media_downloads 
            WHERE title=%s AND type='movie'
        ''', (title,))
        results = cursor.fetchall()

        if not results:
            logger.debug(f"Película no encontrada: {title}")
            return False, None

        # Verificar si alguno de los resultados coincide completamente
        for result in results:
            # Comparar todos los campos relevantes
            if (result['year'] == year and
                    (result['imdb_rating'] == imdb_rating or (
                            result['imdb_rating'] is None and imdb_rating is None)) and
                    (result['genre'] == genre or (result['genre'] is None and genre is None))):
                logger.debug(f"Película encontrada con coincidencia exacta: {title} ({year})")
                return True, result['id']

        # Si llegamos aquí, el título existe pero otros datos no coinciden
        logger.debug(f"Película encontrada pero con datos diferentes: {title} ({year})")
        return False, results[0]['id']  # Devolver el ID del primer resultado para posible actualización
    except Exception as e:
        logger.error(f"Error al verificar si la película existe: {e}")
        return False, None
    finally:
        cursor.close()
        connection.close()


# Función para verificar si un enlace ya existe en la base de datos
def link_exists(movie_id, server_id, language, link):
    connection = connect_db()
    cursor = connection.cursor()
    try:
        cursor.execute('''
            SELECT id FROM links_files_download 
            WHERE movie_id=%s AND server_id=%s AND language=%s AND link=%s
        ''', (movie_id, server_id, language, link))
        result = cursor.fetchone()
        exists = result is not None
        return exists
    except Exception as e:
        logger.error(f"Error al verificar si el enlace existe: {e}")
        return False
    finally:
        cursor.close()
        connection.close()


# Función para extraer detalles de la película
def extract_movie_details(movie_url, movie_type="new", worker_id=0):
    logger.info(f"[Worker {worker_id}] Extrayendo detalles de la película ({movie_type}): {movie_url}")

    # Crear un nuevo driver para este worker
    driver = create_driver()

    try:
        # Iniciar sesión con este driver
        if not login(driver):
            logger.error(f"[Worker {worker_id}] No se pudo iniciar sesión. Abortando extracción de {movie_url}")
            driver.quit()
            return None

        driver.get(movie_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Extraer el título de la película
        title_tag = soup.find("div", id="summary-title")
        title = title_tag.text.strip() if title_tag else "No encontrado"
        logger.debug(f"[Worker {worker_id}] Título extraído: {title}")

        # Extraer datos básicos de la película
        show_details = soup.find("div", class_="show-details")
        year = None
        imdb_rating = None
        genre = None

        if show_details:
            year_tag = show_details.find("a", href=re.compile(r"/buscar/year/"))
            if year_tag:
                year = int(year_tag.text.strip())
                logger.debug(f"[Worker {worker_id}] Año extraído: {year}")

            imdb_rating_tag = show_details.find("p", itemprop="aggregateRating")
            if imdb_rating_tag:
                rating_text = imdb_rating_tag.find("a").text.strip()
                try:
                    imdb_rating = float(rating_text)
                    logger.debug(f"[Worker {worker_id}] IMDB Rating extraído: {imdb_rating}")
                except ValueError:
                    logger.warning(f"[Worker {worker_id}] No se pudo convertir el rating a float: {rating_text}")

            genre_tag = show_details.find("a", href=re.compile(r"/tags-peliculas"))
            if genre_tag:
                genre = genre_tag.text.strip()
                logger.debug(f"[Worker {worker_id}] Género extraído: {genre}")

        # Verificar si la película ya existe en la base de datos
        exists, movie_id = movie_exists(title, year, imdb_rating, genre)

        # Si la película existe exactamente igual, solo extraemos los enlaces
        if exists:
            logger.info(
                f"[Worker {worker_id}] La película '{title}' ({year}) ya existe en la base de datos con ID {movie_id}. Actualizando enlaces...")
        else:
            # Si no existe o hay diferencias, insertamos/actualizamos la película
            movie_id = insert_or_update_movie({
                "title": title,
                "year": year,
                "imdb_rating": imdb_rating,
                "genre": genre,
                "type": "movie",
                "existing_id": movie_id  # Puede ser None si no existe
            })

            if not movie_id:
                logger.error(f"[Worker {worker_id}] Error al insertar/actualizar la película: {title}")
                driver.quit()
                return None

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
                    "movie_id": movie_id,
                    "server": server,
                    "language": language,
                    "link": embedded_link,
                    "quality": quality
                })

        # Insertar los enlaces en la base de datos
        if server_links:
            insert_links(server_links)

        logger.debug(
            f"[Worker {worker_id}] Detalles de la película extraídos: {title}, {year}, {imdb_rating}, {genre}, {len(server_links)} enlaces")

        # Cerrar el driver
        driver.quit()

        return {
            "id": movie_id,
            "title": title,
            "year": year,
            "imdb_rating": imdb_rating,
            "genre": genre,
            "links": server_links,
            "type": movie_type
        }
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles de la película {movie_url}: {e}")
        driver.quit()
        raise


# Función para insertar o actualizar una película en la base de datos
def insert_or_update_movie(movie_data):
    connection = connect_db()
    cursor = connection.cursor()
    movie_id = None

    try:
        if movie_data.get("existing_id"):
            # Actualizar película existente
            cursor.execute('''
                UPDATE media_downloads 
                SET year=%s, imdb_rating=%s, genre=%s, updated_at=NOW()
                WHERE id=%s
            ''', (movie_data["year"], movie_data["imdb_rating"], movie_data["genre"], movie_data["existing_id"]))
            movie_id = movie_data["existing_id"]
            logger.info(f"Película actualizada: {movie_data['title']} (ID: {movie_id})")
        else:
            # Insertar nueva película
            cursor.execute('''
                INSERT INTO media_downloads (title, year, imdb_rating, genre, type, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ''', (movie_data["title"], movie_data["year"], movie_data["imdb_rating"], movie_data["genre"],
                  movie_data["type"]))
            movie_id = cursor.lastrowid
            logger.info(f"Nueva película insertada: {movie_data['title']} (ID: {movie_id})")

        connection.commit()
    except Exception as e:
        logger.error(f"Error al insertar/actualizar la película: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    return movie_id


# Función para insertar enlaces en la base de datos
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
            server_id = cursor.fetchone()['id']

            # Obtener el ID de la calidad
            cursor.execute('''
                SELECT quality_id FROM qualities WHERE quality=%s
            ''', (link["quality"],))
            quality_id = cursor.fetchone()['quality_id']

            # Verificar si el enlace ya existe
            if not link_exists(link["movie_id"], server_id, link["language"], link["link"]):
                # Insertar el enlace en la base de datos
                cursor.execute('''
                    INSERT INTO links_files_download (movie_id, server_id, language, link, quality_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                ''', (link["movie_id"], server_id, link["language"], link["link"], quality_id))
                logger.debug(
                    f"Enlace insertado: movie_id={link['movie_id']}, server={link['server']}, language={link['language']}")
            else:
                logger.debug(
                    f"Enlace ya existe: movie_id={link['movie_id']}, server={link['server']}, language={link['language']}")

        connection.commit()
    except Exception as e:
        logger.error(f"Error al insertar enlaces: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# Función para obtener URLs de películas de una página
def get_movie_urls_from_page(page_url, driver):
    logger.info(f"Obteniendo URLs de películas de la página: {page_url}")
    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Buscar todos los divs de las películas
        movie_divs = soup.find_all("div", class_="span-6 inner-6 tt view")
        logger.info(f"Encontradas {len(movie_divs)} películas en la página")

        movie_urls = []
        for movie_div in movie_divs:
            link_tag = movie_div.find("a", href=re.compile(r"/pelicula/"))
            if link_tag:
                movie_href = link_tag['href']
                movie_url = base_url + movie_href
                movie_urls.append(movie_url)

        return movie_urls
    except Exception as e:
        logger.error(f"Error al obtener URLs de películas de la página {page_url}: {e}")
        return []


# Función para procesar una película con reintentos
def process_movie_with_retries(movie_url, movie_type, worker_id):
    for attempt in range(max_retries):
        try:
            return extract_movie_details(movie_url, movie_type, worker_id)
        except Exception as e:
            logger.error(
                f"[Worker {worker_id}] Error al procesar la película {movie_url} (intento {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(30)  # Esperar 30 segundos antes de reintentar

    logger.error(f"[Worker {worker_id}] No se pudo procesar la película {movie_url} después de {max_retries} intentos")
    return None


# Función para procesar películas en paralelo
def process_movies_in_parallel(movie_urls, movie_type):
    logger.info(f"Procesando {len(movie_urls)} películas en paralelo con {max_workers} workers")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Crear un diccionario de futuros/tareas
        future_to_url = {
            executor.submit(process_movie_with_retries, url, movie_type, i % max_workers): url
            for i, url in enumerate(movie_urls)
        }

        # Procesar los resultados a medida que se completan
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                movie_data = future.result()
                if movie_data:
                    results.append(movie_data)
                    logger.info(f"Película procesada correctamente: {url}")
            except Exception as e:
                logger.error(f"Error al procesar la película {url}: {e}")

    return results


# Función para verificar si hay una siguiente página
def has_next_page(page_url, driver):
    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Buscar el botón de siguiente página
        next_button = soup.find("a", class_="current")
        if next_button and next_button.find_next_sibling("a"):
            return True

        # Verificar si hay películas en la página actual
        movie_divs = soup.find_all("div", class_="span-6 inner-6 tt view")
        return len(movie_divs) > 0
    except Exception as e:
        logger.error(f"Error al verificar si hay una siguiente página: {e}")
        return False


# Función para guardar el progreso
def save_progress(movie_type, page_number, processed_urls):
    try:
        progress = {}
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)

        if movie_type not in progress:
            progress[movie_type] = {}

        progress[movie_type]['page_number'] = page_number
        progress[movie_type]['processed_urls'] = processed_urls
        progress[movie_type]['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(progress_file, 'w') as f:
            json.dump(progress, f)

        logger.debug(
            f"Progreso guardado para {movie_type}: página {page_number}, {len(processed_urls)} URLs procesadas")
    except Exception as e:
        logger.error(f"Error al guardar el progreso: {e}")


# Función para cargar el progreso
def load_progress(movie_type):
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                if movie_type in progress:
                    logger.info(f"Progreso cargado para {movie_type}: página {progress[movie_type]['page_number']}")
                    return progress[movie_type]['page_number'], progress[movie_type].get('processed_urls', [])

        logger.info(f"No se encontró progreso para {movie_type}. Comenzando desde el principio.")
        return 1, []
    except Exception as e:
        logger.error(f"Error al cargar el progreso: {e}")
        return 1, []


# Función para procesar películas de estreno
def process_new_movies(max_pages=None):
    logger.info("Iniciando procesamiento de películas de estreno...")

    # Crear un driver principal para navegar por las páginas
    main_driver = create_driver()
    if not login(main_driver):
        logger.error("No se pudo iniciar sesión. Abortando procesamiento de películas de estreno.")
        main_driver.quit()
        return []

    page_number, processed_urls = load_progress("new")
    all_processed_movies = []

    processed_pages = 0
    while True:
        if max_pages and processed_pages >= max_pages:
            logger.info(f"Se ha alcanzado el límite de {max_pages} páginas para películas de estreno.")
            break

        try:
            page_url = f"{new_movies_url}/{page_number}" if page_number > 1 else new_movies_url

            if not has_next_page(page_url, main_driver):
                logger.info(f"No hay más páginas de películas de estreno después de la página {page_number}.")
                break

            logger.info(f"Procesando página {page_number} de películas de estreno: {page_url}")

            # Obtener URLs de películas de la página actual
            movie_urls = get_movie_urls_from_page(page_url, main_driver)

            # Filtrar URLs ya procesadas
            new_urls = [url for url in movie_urls if url not in processed_urls]
            logger.info(f"Encontradas {len(new_urls)} películas nuevas para procesar en la página {page_number}")

            if new_urls:
                # Procesar películas en paralelo
                processed_movies = process_movies_in_parallel(new_urls, "new")
                all_processed_movies.extend(processed_movies)

                # Actualizar la lista de URLs procesadas
                processed_urls.extend(new_urls)

                # Guardar progreso
                save_progress("new", page_number, processed_urls)

            # Avanzar a la siguiente página
            page_number += 1
            processed_pages += 1

            # Pequeña pausa entre páginas para no sobrecargar el servidor
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error al procesar la página {page_number} de películas de estreno: {e}")
            time.sleep(60)  # Esperar 1 minuto antes de intentar nuevamente

    main_driver.quit()
    return all_processed_movies


# Función para procesar películas actualizadas
def process_updated_movies(max_pages=None):
    logger.info("Iniciando procesamiento de películas actualizadas...")

    # Crear un driver principal para navegar por las páginas
    main_driver = create_driver()
    if not login(main_driver):
        logger.error("No se pudo iniciar sesión. Abortando procesamiento de películas actualizadas.")
        main_driver.quit()
        return []

    page_number, processed_urls = load_progress("updated")
    all_processed_movies = []

    processed_pages = 0
    while True:
        if max_pages and processed_pages >= max_pages:
            logger.info(f"Se ha alcanzado el límite de {max_pages} páginas para películas actualizadas.")
            break

        try:
            page_url = f"{updated_movies_url}/{page_number}" if page_number > 1 else updated_movies_url

            if not has_next_page(page_url, main_driver):
                logger.info(f"No hay más páginas de películas actualizadas después de la página {page_number}.")
                break

            logger.info(f"Procesando página {page_number} de películas actualizadas: {page_url}")

            # Obtener URLs de películas de la página actual
            movie_urls = get_movie_urls_from_page(page_url, main_driver)

            # Filtrar URLs ya procesadas
            new_urls = [url for url in movie_urls if url not in processed_urls]
            logger.info(f"Encontradas {len(new_urls)} películas nuevas para procesar en la página {page_number}")

            if new_urls:
                # Procesar películas en paralelo
                processed_movies = process_movies_in_parallel(new_urls, "updated")
                all_processed_movies.extend(processed_movies)

                # Actualizar la lista de URLs procesadas
                processed_urls.extend(new_urls)

                # Guardar progreso
                save_progress("updated", page_number, processed_urls)

            # Avanzar a la siguiente página
            page_number += 1
            processed_pages += 1

            # Pequeña pausa entre páginas para no sobrecargar el servidor
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error al procesar la página {page_number} de películas actualizadas: {e}")
            time.sleep(60)  # Esperar 1 minuto antes de intentar nuevamente

    main_driver.quit()
    return all_processed_movies


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
def generate_update_report(start_time, new_movies, updated_movies):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos

    # Contar películas nuevas y actualizadas
    new_count = len(new_movies)
    updated_count = len(updated_movies)

    # Contar enlaces nuevos
    new_links_count = sum(len(movie.get("links", [])) for movie in new_movies + updated_movies)

    # Generar informe
    report = f"""
INFORME DE ACTUALIZACIÓN DE PELÍCULAS - {end_time.strftime('%Y-%m-%d %H:%M:%S')}
===========================================================================

Duración: {duration:.2f} minutos

RESUMEN:
- Películas de estreno procesadas: {new_count}
- Películas actualizadas procesadas: {updated_count}
- Total de nuevos enlaces: {new_links_count}

PELÍCULAS DE ESTRENO:
"""

    for movie in new_movies[:10]:  # Mostrar solo las primeras 10 para no hacer el correo demasiado largo
        report += f"- {movie.get('title', 'Sin título')} ({movie.get('year', 'N/A')}) - {len(movie.get('links', []))} enlaces\\n"

    if len(new_movies) > 10:
        report += f"... y {len(new_movies) - 10} películas más\\n"

    report += """
PELÍCULAS ACTUALIZADAS:
"""
    
    for movie in updated_movies[:10]:  # Mostrar solo las primeras 10
        report += f"- {movie.get('title', 'Sin título')} ({movie.get('year', 'N/A')}) - {len(movie.get('links', []))} enlaces\\n"

    if len(updated_movies) > 10:
        report += f"... y {len(updated_movies) - 10} películas más\\n"

    report += """
===========================================================================
Este es un mensaje automático generado por el sistema de actualización de películas.
"""

    return report


# Función para registrar estadísticas de actualización
def log_update_stats(start_time, new_movies, updated_movies):
    try:
        connection = connect_db()
        cursor = connection.cursor()

        # Obtener estadísticas de la actualización actual
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # en minutos

        # Contar películas añadidas/actualizadas
        new_count = len(new_movies)
        updated_count = len(updated_movies)

        # Contar enlaces añadidos
        new_links_count = sum(len(movie.get("links", [])) for movie in new_movies + updated_movies)

        # Registrar estadísticas en la base de datos
        cursor.execute('''
            INSERT INTO update_stats (update_date, duration_minutes, updated_movies, new_links)
            VALUES (CURDATE(), %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                duration_minutes = duration_minutes + %s,
                updated_movies = updated_movies + %s,
                new_links = new_links + %s
        ''', (
        duration, new_count + updated_count, new_links_count, duration, new_count + updated_count, new_links_count))

        connection.commit()

        logger.info(
            f"Estadísticas de actualización: Duración={duration:.2f} minutos, Películas nuevas={new_count}, Películas actualizadas={updated_count}, Nuevos enlaces={new_links_count}")
        return {
            "duration": duration,
            "new_movies": new_count,
            "updated_movies": updated_count,
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
            CREATE TABLE IF NOT EXISTS update_stats (
                update_date DATE PRIMARY KEY,
                duration_minutes FLOAT,
                updated_movies INT,
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


# Función principal
def main(process_new=True, process_updated=True, max_pages_new=None, max_pages_updated=None):
    start_time = datetime.now()
    logger.info(f"Iniciando actualización de películas: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Configurar la base de datos
        setup_database()

        new_movies = []
        updated_movies = []

        # Procesar películas de estreno
        if process_new:
            new_movies = process_new_movies(max_pages_new)
            logger.info(f"Procesadas {len(new_movies)} películas de estreno")

        # Procesar películas actualizadas
        if process_updated:
            updated_movies = process_updated_movies(max_pages_updated)
            logger.info(f"Procesadas {len(updated_movies)} películas actualizadas")

        # Registrar estadísticas
        stats = log_update_stats(start_time, new_movies, updated_movies)

        # Generar y enviar informe
        if email_config["enabled"]:
            report = generate_update_report(start_time, new_movies, updated_movies)
            send_email_notification(
                f"Informe de actualización de películas - {datetime.now().strftime('%Y-%m-%d')}",
                report
            )

        logger.info(f"Actualización de películas completada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return stats
    except Exception as e:
        logger.critical(f"Error crítico en la actualización de películas: {e}")

        # Enviar notificación de error
        if email_config["enabled"]:
            send_email_notification(
                f"ERROR CRÍTICO - Actualización de películas - {datetime.now().strftime('%Y-%m-%d')}",
                f"Se ha producido un error crítico durante la actualización de películas:\\n\\n{str(e)}\\n\\nPor favor, revise los logs para más detalles."
            )

        return None


# Crear archivo de programación automática para Windows
def create_scheduler_script():
    script_path = os.path.abspath(__file__)
    bat_file_path = os.path.join(os.path.dirname(script_path), "schedule_movie_updates.bat")

    with open(bat_file_path, "w") as f:
        f.write(f"""@echo off
echo Iniciando actualizacion de peliculas...
python "{script_path}" --process-new --process-updated
echo Actualizacion completada.
pause
""")

    # Crear archivo XML para Task Scheduler
    xml_file_path = os.path.join(os.path.dirname(script_path), "movie_update_task.xml")

    # Obtener la ruta completa al intérprete de Python
    python_path = sys.executable

    with open(xml_file_path, "w") as f:
        f.write(f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>2023-06-15T12:00:00</Date>
    <Author>Sistema de Actualización de Películas</Author>
    <Description>Tarea semanal para actualizar la base de datos de películas</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2023-06-15T03:00:00</StartBoundary>
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
      <Arguments>"{script_path}" --process-new --process-updated</Arguments>
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

La tarea está configurada para ejecutarse cada domingo a las 3:00 AM.
""")


# Punto de entrada principal
if __name__ == "__main__":
    import sys

    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Actualización de películas de estreno y actualizadas')
    parser.add_argument('--process-new', action='store_true', help='Procesar películas de estreno')
    parser.add_argument('--process-updated', action='store_true', help='Procesar películas actualizadas')
    parser.add_argument('--max-pages-new', type=int,
                        help='Número máximo de páginas a procesar para películas de estreno')
    parser.add_argument('--max-pages-updated', type=int,
                        help='Número máximo de páginas a procesar para películas actualizadas')
    parser.add_argument('--create-scheduler', action='store_true',
                        help='Crear archivos para programar la tarea en Windows')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')

    args = parser.parse_args()

    # Si no se especifica ninguna acción, procesar ambos tipos de películas
    if not (args.process_new or args.process_updated or args.create_scheduler):
        args.process_new = True
        args.process_updated = True

    # Actualizar configuración de paralelización si se especifica
    if args.max_workers:
        max_workers = args.max_workers

    # Crear archivos para programar la tarea
    if args.create_scheduler:
        create_scheduler_script()
    else:
        # Ejecutar la actualización de películas
        main(
            process_new=args.process_new,
            process_updated=args.process_updated,
            max_pages_new=args.max_pages_new,
            max_pages_updated=args.max_pages_updated
        )