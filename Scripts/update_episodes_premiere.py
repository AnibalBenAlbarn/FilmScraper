import time
import re
import concurrent.futures
import argparse
import os
import traceback
import json
import threading
from datetime import datetime
from queue import Queue, Empty
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# Importar utilidades compartidas
from scraper_utils import (
    setup_logger, create_driver, connect_db, login, setup_database,
    save_progress, load_progress, clear_cache, find_series_by_title_year,
    season_exists, episode_exists, insert_series, insert_season,
    insert_episode, BASE_URL, MAX_WORKERS, MAX_RETRIES, PROJECT_ROOT,
    log_link_insertion, is_url_completed
)

# Configuración específica para este script
SCRIPT_NAME = "update_episodes_premiere"
LOG_FILE = f"{SCRIPT_NAME}.log"
PROGRESS_FILE = os.path.join(PROJECT_ROOT, "progress", "update_series_premiere_progress.json")
NEW_EPISODES_URL = f"{BASE_URL}/episodios#premiere"

# Configurar logger
logger = setup_logger(SCRIPT_NAME, LOG_FILE)

# Colas para comunicación entre workers
url_queue = Queue()  # Worker 1 -> Worker 2
metadata_queue = Queue()  # Worker 2 -> Worker 3
links_queue = Queue()  # Worker 3 -> Worker 4

# Evento para señalizar parada
stop_event = threading.Event()

# Contadores para estadísticas
stats = {
    'new_series': 0,
    'new_seasons': 0,
    'new_episodes': 0,
    'new_links': 0
}

# Locks para sincronización
stats_lock = threading.Lock()
db_lock = threading.Lock()


# Worker 1: Obtiene URLs de episodios de la página de estrenos
def worker1_url_extractor(driver, progress_data):
    logger.info("Worker 1: Iniciando extracción de URLs de episodios de estreno")

    try:
        # Obtener URLs de episodios
        episode_urls = get_episode_urls_from_premiere_page(driver)

        if not episode_urls:
            logger.warning("Worker 1: No se encontraron episodios de estreno")
            return []

        # Cargar URLs ya completadas y procesadas en esta sesión
        completed_urls = set(progress_data.get('completed_urls', []))
        processed_urls = set(progress_data.get('processed_urls', []))

        # Filtrar URLs ya completadas o ya en proceso
        new_urls = [
            url for url in episode_urls
            if url not in processed_urls and not is_url_completed(progress_data, url)
        ]
        logger.info(f"Worker 1: Encontrados {len(new_urls)} episodios nuevos para procesar")

        # Guardar todas las URLs en un archivo JSON para referencia
        save_urls_to_json(episode_urls, new_urls)

        # Poner las URLs en la cola para el Worker 2
        for url in new_urls:
            if stop_event.is_set():
                break
            url_queue.put(url)
            logger.debug(f"Worker 1: URL añadida a la cola: {url}")

        # Actualizar las URLs procesadas en el progreso
        progress_data['processed_urls'] = list(set(processed_urls).union(new_urls))

        return new_urls
    except Exception as e:
        logger.error(f"Worker 1: Error al extraer URLs: {e}")
        logger.debug(traceback.format_exc())
        return []


# Función para guardar URLs en un archivo JSON
def save_urls_to_json(all_urls, new_urls):
    try:
        output_file = os.path.join(PROJECT_ROOT, "data", f"{SCRIPT_NAME}_urls.json")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        data = {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_urls": len(all_urls),
            "new_urls": len(new_urls),
            "all_urls": all_urls,
            "new_urls": new_urls
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"URLs guardadas en {output_file}")
    except Exception as e:
        logger.error(f"Error al guardar URLs en JSON: {e}")


# Función para obtener URLs de episodios de la página de estrenos
def get_episode_urls_from_premiere_page(driver):
    logger.info("Obteniendo URLs de episodios de estreno...")
    try:
        driver.get(NEW_EPISODES_URL)
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

        # Esperar a que aparezca el contenedor de episodios
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "episodes-content"))
            )
        except TimeoutException:
            logger.error("Timeout esperando el contenedor de episodios")
            return []

        # Obtener todos los episodios con scroll infinito
        episode_urls = []
        last_count = 0
        no_new_results_count = 0
        max_no_new_results = 5  # Número máximo de intentos sin nuevos resultados antes de parar
        max_scroll_attempts = 65  # Límite de scroll para evitar bucles infinitos

        # Conjunto para evitar duplicados
        seen_urls = set()

        scroll_attempt = 0
        # Bucle para hacer scroll hasta que no haya más episodios
        while scroll_attempt < max_scroll_attempts:
            # Obtener los episodios actuales
            episode_divs = driver.find_elements(By.CSS_SELECTOR, "#episodes-content .span-6.tt.view.show-view")

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
            if len(episode_urls) == last_count:
                no_new_results_count += 1
                if no_new_results_count >= max_no_new_results:  # Si no hay nuevos resultados después de varios intentos, terminar
                    logger.info(
                        f"No se encontraron nuevos episodios después de {no_new_results_count} intentos. Terminando scroll.")
                    break
            else:
                no_new_results_count = 0  # Reiniciar contador si se encontraron nuevos episodios
                last_count = len(episode_urls)

            # Hacer scroll hacia abajo
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            logger.debug(
                f"Scroll {scroll_attempt + 1}/{max_scroll_attempts}: {len(episode_urls)} episodios encontrados")

            # Esperar a que se carguen más contenidos
            time.sleep(1)
            scroll_attempt += 1

        if scroll_attempt >= max_scroll_attempts:
            logger.info(f"Se alcanzó el límite de {max_scroll_attempts} scrolls, finalizando.")

        logger.info(f"Se encontraron {len(episode_urls)} episodios de estreno")
        return episode_urls
    except Exception as e:
        logger.error(f"Error al obtener URLs de episodios de estreno: {e}")
        logger.debug(traceback.format_exc())
        return []


# Worker 2: Verifica en BD y obtiene datos de la serie
def worker2_metadata_extractor(driver, db_path, worker_id=0):
    logger.info(f"Worker 2 (ID {worker_id}): Iniciando extracción de metadatos")

    while not stop_event.is_set():
        try:
            # Obtener una URL de la cola con timeout
            try:
                episode_url = url_queue.get(timeout=5)
            except Empty:
                # Si la cola está vacía y el Worker 1 ha terminado, salir
                if url_queue.empty():
                    logger.info(f"Worker 2 (ID {worker_id}): No hay más URLs para procesar")
                    break
                continue

            logger.info(f"Worker 2 (ID {worker_id}): Procesando episodio: {episode_url}")

            # Extraer detalles del episodio
            episode_data = extract_episode_details(driver, episode_url, worker_id, db_path)

            if episode_data:
                # Poner los datos en la cola para el Worker 3
                metadata_queue.put({
                    "episode_url": episode_url,
                    "episode_data": episode_data
                })
                logger.debug(f"Worker 2 (ID {worker_id}): Datos añadidos a la cola para Worker 3: {episode_url}")

            # Marcar la tarea como completada
            url_queue.task_done()

        except Exception as e:
            logger.error(f"Worker 2 (ID {worker_id}): Error al procesar URL: {e}")
            logger.debug(traceback.format_exc())
            # Marcar la tarea como completada para no bloquear la cola
            try:
                url_queue.task_done()
            except:
                pass

    logger.info(f"Worker 2 (ID {worker_id}): Finalizado")


# Función para extraer detalles del episodio
def extract_episode_details(driver, episode_url, worker_id=0, db_path=None):
    logger.info(f"[Worker {worker_id}] Extrayendo detalles del episodio de estreno: {episode_url}")

    try:
        # Navegar a la URL del episodio
        driver.get(episode_url)
        time.sleep(1.5)  # Esperar a que se cargue la página

        # Esperar a que aparezca la información del episodio
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "show-details"))
            )
        except TimeoutException:
            logger.error(f"[Worker {worker_id}] Timeout esperando información del episodio en {episode_url}")
            return None

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Extraer información de la serie desde el div summary-title-wrapper
        series_info = soup.find("div", class_="summary-title-wrapper")
        if not series_info:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar información de la serie en {episode_url}")
            return None

        # Extraer título de la serie
        series_title_div = series_info.find("div", id="summary-title")
        if not series_title_div:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar el título de la serie en {episode_url}")
            return None

        series_title = series_title_div.text.strip()

        # Extraer información del episodio desde el span subtitle
        subtitle_span = series_info.find("span", class_="subtitle")
        if not subtitle_span:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar información del episodio en {episode_url}")
            return None

        # El formato suele ser "2x05 Episodio 5"
        episode_info_text = subtitle_span.text.strip()
        episode_match = re.match(r'(\d+)\s*x\s*(\d+)\s*(.*)', episode_info_text)

        if not episode_match:
            logger.error(f"[Worker {worker_id}] No se pudo extraer la información del episodio: {episode_info_text}")
            return None

        season_number = int(episode_match.group(1))
        episode_number = int(episode_match.group(2))
        episode_title = episode_match.group(3).strip()

        if not episode_title:
            # Intentar obtener el título del episodio de los metadatos
            meta_title = soup.find("meta", {"itemprop": "name", "content": lambda x: x and x != series_title})
            if meta_title:
                episode_title = meta_title["content"].strip()
            else:
                episode_title = f"Episodio {episode_number}"

        logger.info(
            f"[Worker {worker_id}] Información extraída: Serie={series_title}, Temporada={season_number}, Episodio={episode_number}, Título={episode_title}")

        # Extraer información adicional de la serie desde show-details
        show_details = soup.find("div", class_="show-details")

        series_year = None
        imdb_rating = None
        genre = None
        status = None
        director = None
        emission_date = None

        if show_details:
            # Extraer estado
            status_p = show_details.find("p", string=lambda s: s and "Estado:" in s if s else False)
            if status_p and status_p.find("a"):
                status = status_p.find("a").text.strip()
                logger.debug(f"[Worker {worker_id}] Estado de la serie extraído: {status}")

            # Extraer año
            year_p = show_details.find("p", string=lambda s: s and "Año:" in s if s else False)
            if year_p and year_p.find("a"):
                try:
                    series_year = int(year_p.find("a").text.strip())
                    logger.debug(f"[Worker {worker_id}] Año de la serie extraído: {series_year}")
                except ValueError:
                    logger.warning(f"[Worker {worker_id}] No se pudo convertir el año a entero")

            # Extraer IMDB rating
            imdb_p = show_details.find("p", string=lambda s: s and "IMDB Rating:" in s if s else False)
            if imdb_p and imdb_p.find("a"):
                rating_text = imdb_p.find("a").text.strip().split()[0]
                try:
                    imdb_rating = float(rating_text)
                    logger.debug(f"[Worker {worker_id}] IMDB Rating extraído: {imdb_rating}")
                except ValueError:
                    logger.warning(f"[Worker {worker_id}] No se pudo convertir el rating a float: {rating_text}")

            # Extraer género
            genre_p = show_details.find("p", string=lambda s: s and "Género:" in s if s else False)
            if genre_p:
                genre_tags = genre_p.find_all("a")
                genre = ", ".join([tag.text.strip() for tag in genre_tags]) if genre_tags else None
                logger.debug(f"[Worker {worker_id}] Género extraído: {genre}")

            # Extraer fecha de emisión
            emission_p = show_details.find("p", string=lambda s: s and "Emitido:" in s if s else False)
            if emission_p:
                emission_text = emission_p.text.replace("Emitido:", "").strip()
                emission_date = emission_text
                logger.debug(f"[Worker {worker_id}] Fecha de emisión extraída: {emission_date}")

        # Crear una conexión a la base de datos para reutilizarla
        connection = connect_db(db_path)

        try:
            # Buscar la serie en la base de datos
            series_data = find_series_by_title_year(series_title, series_year, connection, db_path)
            series_id = None

            # Si la serie no existe, la insertamos
            if not series_data:
                logger.info(
                    f"[Worker {worker_id}] Serie no encontrada en la base de datos. Insertando nueva serie: {series_title} ({series_year})")
                with db_lock:
                    series_id = insert_series(series_title, series_year, imdb_rating, genre, connection, db_path)
                if not series_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar la serie. Abortando.")
                    connection.close()
                    return None
                is_new_series = True
                with stats_lock:
                    stats['new_series'] += 1
            else:
                series_id = series_data['id']
                logger.info(f"[Worker {worker_id}] Serie encontrada en la base de datos con ID {series_id}")
                is_new_series = False

            # Verificar si la temporada existe
            season_exists_flag, season_id = season_exists(series_id, season_number, connection, db_path)

            # Si la temporada no existe, la insertamos
            if not season_exists_flag:
                logger.info(
                    f"[Worker {worker_id}] Temporada {season_number} no encontrada. Insertando nueva temporada.")
                with db_lock:
                    season_id = insert_season(series_id, season_number, connection, db_path)
                if not season_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar la temporada. Abortando.")
                    connection.close()
                    return None
                is_new_season = True
                with stats_lock:
                    stats['new_seasons'] += 1
            else:
                logger.info(f"[Worker {worker_id}] Temporada {season_number} encontrada con ID {season_id}")
                is_new_season = False

            # Verificar si el episodio existe
            episode_exists_flag, episode_id = episode_exists(season_id, episode_number, episode_title, connection,
                                                             db_path)

            # Si el episodio no existe, lo insertamos
            if not episode_exists_flag:
                logger.info(f"[Worker {worker_id}] Episodio {episode_number} no encontrado. Insertando nuevo episodio.")
                with db_lock:
                    episode_id = insert_episode(season_id, episode_number, episode_title, connection, db_path)
                if not episode_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar el episodio. Abortando.")
                    connection.close()
                    return None
                is_new_episode = True
                with stats_lock:
                    stats['new_episodes'] += 1
            else:
                logger.info(f"[Worker {worker_id}] Episodio {episode_number} encontrado con ID {episode_id}")
                is_new_episode = False

            # Cerrar la conexión a la base de datos
            connection.close()

            return {
                "series_id": series_id,
                "series_title": series_title,
                "series_year": series_year,
                "season_id": season_id,
                "season_number": season_number,
                "episode_id": episode_id,
                "episode_number": episode_number,
                "episode_title": episode_title,
                "is_new_series": is_new_series,
                "is_new_season": is_new_season,
                "is_new_episode": is_new_episode,
                "status": status,
                "imdb_rating": imdb_rating,
                "genre": genre,
                "director": director,
                "emission_date": emission_date
            }
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error al procesar el episodio: {e}")
            logger.debug(traceback.format_exc())
            connection.close()
            raise
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles del episodio {episode_url}: {e}")
        logger.debug(traceback.format_exc())
        raise


# Worker 3: Obtiene servidores y enlaces de streaming
def worker3_link_extractor(driver, worker_id=0):
    logger.info(f"Worker 3 (ID {worker_id}): Iniciando extracción de enlaces")

    while not stop_event.is_set():
        try:
            # Obtener datos de la cola con timeout
            try:
                data = metadata_queue.get(timeout=5)
            except Empty:
                # Si la cola está vacía y el Worker 2 ha terminado, salir
                if metadata_queue.empty() and url_queue.empty():
                    logger.info(f"Worker 3 (ID {worker_id}): No hay más metadatos para procesar")
                    break
                continue

            episode_url = data["episode_url"]
            episode_data = data["episode_data"]

            logger.info(f"Worker 3 (ID {worker_id}): Extrayendo enlaces para: {episode_url}")

            # Navegar a la URL del episodio
            driver.get(episode_url)
            time.sleep(2)  # Esperar a que se cargue la página

            # Extraer enlaces del episodio
            server_links = extract_episode_links(driver, episode_data["episode_id"])

            if server_links:
                # Poner los datos en la cola para el Worker 4
                links_queue.put({
                    "episode_url": episode_url,
                    "episode_data": episode_data,
                    "server_links": server_links
                })
                logger.debug(f"Worker 3 (ID {worker_id}): Enlaces añadidos a la cola para Worker 4: {episode_url}")

            # Marcar la tarea como completada
            metadata_queue.task_done()

        except Exception as e:
            logger.error(f"Worker 3 (ID {worker_id}): Error al procesar metadatos: {e}")
            logger.debug(traceback.format_exc())
            # Marcar la tarea como completada para no bloquear la cola
            try:
                metadata_queue.task_done()
            except:
                pass

    logger.info(f"Worker 3 (ID {worker_id}): Finalizado")


# Función para extraer enlaces de un episodio
def extract_episode_links(driver, episode_id):
    """Extrae enlaces de un episodio."""
    server_links = []
    logger.debug(f"Extrayendo enlaces para el episodio ID {episode_id}")

    try:
        # Esperar a que aparezcan los selectores de embed
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "embed-selector"))
        )

        # Encontrar todos los embed-selectors
        embed_selectors = driver.find_elements(By.CLASS_NAME, 'embed-selector')
        logger.debug(f"Número de enlaces encontrados: {len(embed_selectors)}")

        for i, embed_selector in enumerate(embed_selectors):
            try:
                # Extraer idioma y servidor antes de hacer clic
                embed_html = embed_selector.get_attribute('outerHTML')
                embed_soup = BeautifulSoup(embed_html, "html.parser")

                # Determinar el idioma
                language = None
                if "Audio Español" in embed_html:
                    language = "Audio Español"
                elif "Subtítulo Español" in embed_html:
                    language = "Subtítulo Español"
                elif "Audio Latino" in embed_html:
                    language = "Audio Latino"
                elif "Audio Original" in embed_html:
                    language = "Audio Original"
                elif "Subtítulo Ingles" in embed_html:
                    language = "Subtítulo Ingles"

                # Extraer el servidor usando la nueva estructura HTML
                server = None
                provider_tag = embed_soup.find("b", class_="provider")
                if provider_tag:
                    server_text = provider_tag.text.strip()
                    # Extraer el dominio principal del servidor
                    server = server_text.lower().split('.')[0] if '.' in server_text else server_text.lower()
                    logger.debug(f"Servidor extraído de la clase provider: {server}")
                else:
                    # Método alternativo si no se encuentra la clase provider
                    if "Servidor:" in embed_html:
                        server_match = re.search(r'Servidor:([^<]+)', embed_html)
                        if server_match:
                            server_text = server_match.group(1).strip()
                            server = server_text.lower().split('.')[0] if '.' in server_text else server_text.lower()
                            logger.debug(f"Servidor extraído del texto: {server}")

                logger.debug(f"Selector {i + 1}: Idioma={language}, Servidor={server}")

                # Hacer clic en el selector para mostrar el iframe
                driver.execute_script("arguments[0].click();", embed_selector)
                time.sleep(1.5)  # Esperar a que se cargue el iframe

                try:
                    # Esperar a que aparezca el iframe
                    iframe = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".embed-movie iframe"))
                    )

                    # Obtener el enlace del iframe
                    embedded_link = iframe.get_attribute('src')
                    logger.debug(f"Enlace embebido extraído: {embedded_link}")

                    # Modificar el enlace si es powvideo o streamplay
                    if embedded_link and server and server in ["powvideo", "streamplay"]:
                        embedded_link = re.sub(r"embed-([^-]+)-\d+x\d+\.html", r"\1", embedded_link)

                    # Determinar la calidad en función del servidor
                    quality = '1080p' if server in ['streamtape', 'vidmoly', 'mixdrop'] else 'hdrip'

                    # Añadir enlace a la lista si tenemos todos los datos necesarios
                    if server and language and embedded_link:
                        server_links.append({
                            "episode_id": episode_id,
                            "server": server,
                            "language": language,
                            "link": embedded_link,
                            "quality": quality
                        })
                        logger.debug(f"Enlace añadido: {server} - {language}")
                except Exception as e:
                    logger.error(f"Error al obtener el iframe para el selector {i + 1}: {e}")
            except Exception as e:
                logger.error(f"Error al procesar el selector de embed {i + 1}: {e}")

        logger.info(f"Total de enlaces extraídos: {len(server_links)}")
        return server_links
    except Exception as e:
        logger.error(f"Error al extraer enlaces: {e}")
        logger.debug(traceback.format_exc())
        return []


# Worker 4: Inserta datos en la base de datos
def worker4_db_inserter(db_path, progress_data, worker_id=0):
    logger.info(f"Worker 4 (ID {worker_id}): Iniciando inserción en base de datos")

    completed_urls = set(progress_data.get('completed_urls', []))

    while not stop_event.is_set():
        try:
            # Obtener datos de la cola con timeout
            try:
                data = links_queue.get(timeout=5)
            except Empty:
                # Si la cola está vacía y los workers anteriores han terminado, salir
                if links_queue.empty() and metadata_queue.empty() and url_queue.empty():
                    logger.info(f"Worker 4 (ID {worker_id}): No hay más enlaces para procesar")
                    break
                continue

            episode_url = data["episode_url"]
            episode_data = data["episode_data"]
            server_links = data["server_links"]

            logger.info(f"Worker 4 (ID {worker_id}): Insertando enlaces para: {episode_url}")

            # Insertar enlaces en la base de datos
            with db_lock:
                new_links_count = insert_episode_links(episode_data["episode_id"], server_links, db_path=db_path)

            if new_links_count > 0:
                with stats_lock:
                    stats['new_links'] += new_links_count
                logger.info(f"Worker 4 (ID {worker_id}): Se insertaron {new_links_count} nuevos enlaces")

            # Marcar la URL como completada
            completed_urls.add(episode_url)
            progress_data['completed_urls'] = list(completed_urls)

            # Guardar progreso periódicamente
            if len(completed_urls) % 10 == 0:
                save_progress(PROGRESS_FILE, progress_data)

            # Marcar la tarea como completada
            links_queue.task_done()

        except Exception as e:
            logger.error(f"Worker 4 (ID {worker_id}): Error al procesar enlaces: {e}")
            logger.debug(traceback.format_exc())
            # Marcar la tarea como completada para no bloquear la cola
            try:
                links_queue.task_done()
            except:
                pass

    logger.info(f"Worker 4 (ID {worker_id}): Finalizado")


# Función para insertar enlaces de episodios
def insert_episode_links(episode_id, links, connection=None, db_path=None):
    """Inserta enlaces de un episodio en la base de datos."""
    close_connection = False
    if connection is None:
        connection = connect_db(db_path)
        close_connection = True

    cursor = connection.cursor()
    inserted_count = 0

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
                    INSERT INTO links_files_download (episode_id, server_id, language, link, quality_id, created_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                ''', (episode_id, server_id, link["language"], link["link"], quality_id))
                log_link_insertion(
                    logger,
                    episode_id=episode_id,
                    server=link["server"],
                    language=link["language"],
                )
                inserted_count += 1
            else:
                logger.debug(
                    f"Enlace ya existe: episode_id={episode_id}, server={link['server']}, language={link['language']}"
                )

        connection.commit()
        return inserted_count
    except Exception as e:
        logger.error(f"Error al insertar enlaces del episodio: {e}")
        logger.debug(traceback.format_exc())
        connection.rollback()
        return 0
    finally:
        cursor.close()
        if close_connection:
            connection.close()


# Función para generar informe de actualización
def generate_update_report(start_time, processed_episodes):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos

    # Contar episodios nuevos, temporadas nuevas y series nuevas
    new_episodes_count = stats['new_episodes']
    new_seasons_count = stats['new_seasons']
    new_series_count = stats['new_series']

    # Contar enlaces nuevos
    new_links_count = stats['new_links']

    # Generar informe
    report = f"""
INFORME DE ACTUALIZACIÓN DE EPISODIOS DE ESTRENO - {end_time.strftime('%Y-%m-%d %H:%M:%S')}
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
                report += f"    {ep.get('episode_number', '?')}. {ep.get('episode_title', 'Sin título')} - {ep.get('new_links_count', 0)} nuevos enlaces{status_str}\\n"

    report += """
===========================================================================
Este es un mensaje automático generado por el sistema de actualización de episodios.
"""

    return report


# Función para registrar estadísticas de actualización
def log_update_stats(start_time, db_path=None):
    try:
        connection = connect_db(db_path)
        cursor = connection.cursor()

        # Obtener estadísticas de la actualización actual
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # en minutos

        # Contar episodios nuevos, temporadas nuevas y series nuevas
        new_episodes_count = stats['new_episodes']
        new_seasons_count = stats['new_seasons']
        new_series_count = stats['new_series']

        # Contar enlaces nuevos
        new_links_count = stats['new_links']

        # Verificar si ya existe una entrada para hoy
        cursor.execute('''
            SELECT * FROM episode_update_stats WHERE update_date = date('now')
        ''')
        existing_stats = cursor.fetchone()

        if existing_stats:
            # Actualizar estadísticas existentes
            cursor.execute('''
                UPDATE episode_update_stats 
                SET duration_minutes = duration_minutes + ?,
                    new_series = new_series + ?,
                    new_seasons = new_seasons + ?,
                    new_episodes = new_episodes + ?,
                    new_links = new_links + ?
                WHERE update_date = date('now')
            ''', (duration, new_series_count, new_seasons_count, new_episodes_count, new_links_count))
        else:
            # Insertar nuevas estadísticas
            cursor.execute('''
                INSERT INTO episode_update_stats 
                (update_date, duration_minutes, new_series, new_seasons, new_episodes, new_links)
                VALUES (date('now'), ?, ?, ?, ?, ?)
            ''', (duration, new_series_count, new_seasons_count, new_episodes_count, new_links_count))

        connection.commit()

        logger.info(
            f"Estadísticas de actualización: Duración={duration:.2f} minutos, Nuevas series={new_series_count}, "
            f"Nuevas temporadas={new_seasons_count}, Nuevos episodios={new_episodes_count}, Nuevos enlaces={new_links_count}")
        return {
            "duration": duration,
            "new_series": new_series_count,
            "new_seasons": new_seasons_count,
            "new_episodes": new_episodes_count,
            "new_links": new_links_count
        }
    except Exception as e:
        logger.error(f"Error al registrar estadísticas de actualización: {e}")
        logger.debug(traceback.format_exc())
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()


# Función principal para procesar episodios de estreno
def process_premiere_episodes(db_path=None):
    start_time = datetime.now()
    logger.info(f"Iniciando actualización de episodios de estreno: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Configurar la base de datos
        setup_database(logger, db_path)

        # Limpiar caché antes de comenzar
        clear_cache()

        # Cargar progreso anterior
        progress_data = load_progress(PROGRESS_FILE, {})

        # Crear un driver principal para el Worker 1
        main_driver = create_driver()

        # Primero hacer login
        if not login(main_driver, logger):
            logger.error("No se pudo iniciar sesión. Abortando procesamiento de episodios de estreno.")
            main_driver.quit()
            return []

        # Worker 1: Obtener URLs de episodios de estreno
        new_urls = worker1_url_extractor(main_driver, progress_data)

        if not new_urls:
            logger.warning("No se encontraron episodios de estreno nuevos. Finalizando.")
            main_driver.quit()
            return []

        # Crear drivers para los workers 2, 3 y 4
        worker2_drivers = []
        worker3_drivers = []

        # Crear y configurar drivers para Worker 2 (2 instancias)
        for i in range(2):
            driver = create_driver()
            if login(driver, logger):
                worker2_drivers.append(driver)
            else:
                logger.error(
                    f"No se pudo iniciar sesión para Worker 2 (instancia {i + 1}). Continuando con menos workers.")
                driver.quit()

        # Crear y configurar drivers para Worker 3 (1 instancia)
        driver = create_driver()
        if login(driver, logger):
            worker3_drivers.append(driver)
        else:
            logger.error("No se pudo iniciar sesión para Worker 3. Continuando con menos workers.")
            driver.quit()

        # Iniciar workers en hilos separados
        threads = []

        # Iniciar Worker 2 (Verificador)
        for i, driver in enumerate(worker2_drivers):
            thread = threading.Thread(
                target=worker2_metadata_extractor,
                args=(driver, db_path, i + 1),
                name=f"Worker2-{i + 1}"
            )
            threads.append(thread)
            thread.start()

        # Iniciar Worker 3 (Enlazador)
        for i, driver in enumerate(worker3_drivers):
            thread = threading.Thread(
                target=worker3_link_extractor,
                args=(driver, i + 1),
                name=f"Worker3-{i + 1}"
            )
            threads.append(thread)
            thread.start()

        # Iniciar Worker 4 (Almacenador)
        thread = threading.Thread(
            target=worker4_db_inserter,
            args=(db_path, progress_data, 1),
            name="Worker4-1"
        )
        threads.append(thread)
        thread.start()

        # Esperar a que todos los workers terminen
        for thread in threads:
            thread.join()

        # Cerrar todos los drivers
        main_driver.quit()
        for driver in worker2_drivers + worker3_drivers:
            driver.quit()

        # Guardar progreso final
        save_progress(PROGRESS_FILE, progress_data)

        # Registrar estadísticas
        log_update_stats(start_time, db_path)

        # Generar informe
        processed_episodes = []  # Aquí deberías recopilar los episodios procesados
        report = generate_update_report(start_time, processed_episodes)
        logger.info(report)

        logger.info(f"Actualización de episodios de estreno completada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return processed_episodes
    except Exception as e:
        logger.critical(f"Error crítico en la actualización de episodios de estreno: {e}")
        logger.debug(traceback.format_exc())
        return []


# Punto de entrada principal
if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Actualización de episodios de estreno')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')
    parser.add_argument('--reset-progress', action='store_true',
                        help='Reiniciar el progreso (procesar todos los episodios)')

    args = parser.parse_args()

    # Actualizar configuración de paralelización si se especifica
    if args.max_workers:
        MAX_WORKERS = args.max_workers

    # Reiniciar progreso si se solicita
    if args.reset_progress and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        logger.info("Progreso reiniciado. Se procesarán todos los episodios.")

    # Ejecutar la actualización de episodios
    process_premiere_episodes(args.db_path)
