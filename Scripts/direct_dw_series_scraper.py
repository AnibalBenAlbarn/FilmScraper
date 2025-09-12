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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# ver1.9
# Import shared utilities (compatible when run as script or module)
try:
    from .scraper_utils import (
        setup_logger, create_driver, connect_db, login, setup_database,
        save_progress, load_progress, clear_cache, find_series_by_title_year,
        season_exists, episode_exists, insert_series, insert_season,
        insert_episode, BASE_URL, MAX_WORKERS, MAX_RETRIES, PROJECT_ROOT,
    )
except ImportError:  # pragma: no cover - fallback when executed directly
    from scraper_utils import (
        setup_logger, create_driver, connect_db, login, setup_database,
        save_progress, load_progress, clear_cache, find_series_by_title_year,
        season_exists, episode_exists, insert_series, insert_season,
        insert_episode, BASE_URL, MAX_WORKERS, MAX_RETRIES, PROJECT_ROOT,
    )

# Configuración específica para este script
SCRIPT_NAME = "direct_dw_series_scraper"
LOG_FILE = f"{SCRIPT_NAME}.log"
PROGRESS_FILE = os.path.join(PROJECT_ROOT, "progress", f"{SCRIPT_NAME}_progress.json")
SERIES_BASE_URL = f"{BASE_URL}/series/imdb_rating"

# Configurar logger
logger = setup_logger(SCRIPT_NAME, LOG_FILE)

# Colas para comunicación entre workers
url_queue_odd = Queue()  # Worker 1 -> Worker 2/3 (páginas impares)
url_queue_even = Queue()  # Worker 4 -> Worker 5/6 (páginas pares)
series_data_queue_odd = Queue()  # Worker 2 -> Worker 3 (datos de series de páginas impares)
series_data_queue_even = Queue()  # Worker 5 -> Worker 6 (datos de series de páginas pares)
processed_series_queue = Queue()  # Worker 3/6 -> Estadísticas finales

# Evento para señalizar parada
stop_event = threading.Event()

# Eventos para señalizar que los workers están activos
worker1_active = threading.Event()
worker1_active.set()  # Inicialmente activo

worker2_active = threading.Event()
worker2_active.set()  # Inicialmente activo

worker4_active = threading.Event()
worker4_active.set()  # Inicialmente activo

worker5_active = threading.Event()
worker5_active.set()  # Inicialmente activo

# Contadores para estadísticas
stats = {
    'pages_processed': 0,
    'series_found': 0,
    'series_processed': 0,
    'new_series': 0,
    'new_seasons': 0,
    'new_episodes': 0,
    'new_links': 0,
    'errors': 0,
    'skipped_series': 0,
    'skipped_seasons': 0,
    'skipped_episodes': 0
}

# Locks para sincronización
stats_lock = threading.Lock()
db_lock = threading.Lock()
progress_lock = threading.Lock()


# Función para reintentar operaciones propensas a fallar
def retry_operation(operation, max_retries=3, retry_delay=1):
    """Reintenta una operación hasta max_retries veces con un delay entre intentos."""
    for attempt in range(max_retries):
        try:
            return operation()
        except (StaleElementReferenceException, NoSuchElementException, TimeoutException) as e:
            if attempt == max_retries - 1:
                # Si es el último intento, propagar la excepción
                raise
            logger.warning(f"Error en intento {attempt + 1}/{max_retries}: {e}. Reintentando en {retry_delay}s...")
            time.sleep(retry_delay)
    # No debería llegar aquí, pero por si acaso
    raise Exception("Todos los reintentos fallaron")


# Worker 1: Obtiene URLs de series por páginas impares
def worker1_url_extractor(driver, progress_data, start_page=1, max_pages=None):
    logger.info(f"Worker 1: Iniciando extracción de URLs de series desde páginas impares (inicio: {start_page})")

    current_page = start_page
    empty_pages_count = 0
    all_series_urls = []

    # Inicializar o cargar el seguimiento de páginas
    if 'pages_odd' not in progress_data:
        progress_data['pages_odd'] = {}

    try:
        while not stop_event.is_set():
            # Si se especificó un máximo de páginas y ya lo alcanzamos, salir
            if max_pages and current_page > start_page + max_pages - 1:
                logger.info(f"Worker 1: Se alcanzó el límite de {max_pages} páginas. Finalizando.")
                break

            # Verificar si la página ya fue procesada
            page_key = str(current_page)
            if page_key in progress_data['pages_odd'] and progress_data['pages_odd'][page_key].get('processed', False):
                logger.info(f"Worker 1: Página {current_page} ya procesada anteriormente. Saltando.")
                current_page += 2  # Incrementar en 2 para procesar solo páginas impares
                continue

            # Obtener URLs de series de la página actual
            logger.info(f"Worker 1: Procesando página {current_page}")
            page_series_urls = get_series_urls_from_page(driver, current_page)

            # Actualizar estadísticas
            with stats_lock:
                stats['pages_processed'] += 1
                stats['series_found'] += len(page_series_urls)

            # Si no hay series en la página, incrementar contador de páginas vacías
            if not page_series_urls:
                empty_pages_count += 1
                logger.warning(
                    f"Worker 1: Página {current_page} vacía. Contador de páginas vacías: {empty_pages_count}")

                # Si encontramos 3 páginas vacías consecutivas, asumimos que no hay más series
                if empty_pages_count >= 3:
                    logger.info(
                        f"Worker 1: Se encontraron {empty_pages_count} páginas vacías consecutivas. Finalizando.")
                    break
            else:
                # Reiniciar contador de páginas vacías si encontramos series
                empty_pages_count = 0

            # Añadir URLs a la lista total
            all_series_urls.extend(page_series_urls)

            # Actualizar el progreso de la página
            with progress_lock:
                progress_data['pages_odd'][page_key] = {
                    'processed': True,
                    'url_count': len(page_series_urls),
                    'urls': page_series_urls,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

            # Cargar URLs ya procesadas
            processed_urls = set(progress_data.get('processed_urls_odd', []))

            # Filtrar URLs ya procesadas
            new_urls = [url for url in page_series_urls if url not in processed_urls]
            logger.info(f"Worker 1: Encontrados {len(new_urls)} series nuevas en la página {current_page}")

            # Poner las URLs en la cola para el Worker 2
            for url in new_urls:
                if stop_event.is_set():
                    break
                url_queue_odd.put(url)
                logger.debug(f"Worker 1: URL añadida a la cola: {url}")

            # Guardar progreso periódicamente
            if current_page % 5 == 1:  # Solo para páginas impares
                save_progress(PROGRESS_FILE, progress_data)

            # Pasar a la siguiente página impar
            current_page += 2

            # Pequeña pausa para no sobrecargar el servidor
            time.sleep(1)

            # Verificar si hay suficientes URLs en la cola para los workers
            # Si hay más de 20 URLs, hacer una pausa para permitir que los workers procesen
            if url_queue_odd.qsize() > 20:
                logger.info(
                    f"Worker 1: Haciendo pausa para permitir que los workers procesen la cola (tamaño: {url_queue_odd.qsize()})")
                time.sleep(5)  # Pausa reducida a 5 segundos para ser más eficiente

        # Guardar todas las URLs en un archivo JSON para referencia
        save_urls_to_json(all_series_urls, "odd")

        logger.info(f"Worker 1: Finalizado. Total de series encontradas en páginas impares: {len(all_series_urls)}")
        return all_series_urls

    except Exception as e:
        logger.error(f"Worker 1: Error al extraer URLs: {e}")
        logger.debug(traceback.format_exc())
        with stats_lock:
            stats['errors'] += 1
        return all_series_urls
    finally:
        # Indicar que Worker 1 ha terminado
        worker1_active.clear()
        logger.info("Worker 1: Marcado como inactivo. Los demás workers continuarán hasta vaciar las colas.")


# Worker 4: Obtiene URLs de series por páginas pares
def worker4_url_extractor(driver, progress_data, start_page=2, max_pages=None):
    logger.info(f"Worker 4: Iniciando extracción de URLs de series desde páginas pares (inicio: {start_page})")

    current_page = start_page
    empty_pages_count = 0
    all_series_urls = []

    # Inicializar o cargar el seguimiento de páginas
    if 'pages_even' not in progress_data:
        progress_data['pages_even'] = {}

    try:
        while not stop_event.is_set():
            # Si se especificó un máximo de páginas y ya lo alcanzamos, salir
            if max_pages and current_page > start_page + max_pages - 1:
                logger.info(f"Worker 4: Se alcanzó el límite de {max_pages} páginas. Finalizando.")
                break

            # Verificar si la página ya fue procesada
            page_key = str(current_page)
            if page_key in progress_data['pages_even'] and progress_data['pages_even'][page_key].get('processed',
                                                                                                     False):
                logger.info(f"Worker 4: Página {current_page} ya procesada anteriormente. Saltando.")
                current_page += 2  # Incrementar en 2 para procesar solo páginas pares
                continue

            # Obtener URLs de series de la página actual
            logger.info(f"Worker 4: Procesando página {current_page}")
            page_series_urls = get_series_urls_from_page(driver, current_page)

            # Actualizar estadísticas
            with stats_lock:
                stats['pages_processed'] += 1
                stats['series_found'] += len(page_series_urls)

            # Si no hay series en la página, incrementar contador de páginas vacías
            if not page_series_urls:
                empty_pages_count += 1
                logger.warning(
                    f"Worker 4: Página {current_page} vacía. Contador de páginas vacías: {empty_pages_count}")

                # Si encontramos 3 páginas vacías consecutivas, asumimos que no hay más series
                if empty_pages_count >= 3:
                    logger.info(
                        f"Worker 4: Se encontraron {empty_pages_count} páginas vacías consecutivas. Finalizando.")
                    break
            else:
                # Reiniciar contador de páginas vacías si encontramos series
                empty_pages_count = 0

            # Añadir URLs a la lista total
            all_series_urls.extend(page_series_urls)

            # Actualizar el progreso de la página
            with progress_lock:
                progress_data['pages_even'][page_key] = {
                    'processed': True,
                    'url_count': len(page_series_urls),
                    'urls': page_series_urls,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

            # Cargar URLs ya procesadas
            processed_urls = set(progress_data.get('processed_urls_even', []))

            # Filtrar URLs ya procesadas
            new_urls = [url for url in page_series_urls if url not in processed_urls]
            logger.info(f"Worker 4: Encontrados {len(new_urls)} series nuevas en la página {current_page}")

            # Poner las URLs en la cola para el Worker 5
            for url in new_urls:
                if stop_event.is_set():
                    break
                url_queue_even.put(url)
                logger.debug(f"Worker 4: URL añadida a la cola: {url}")

            # Guardar progreso periódicamente
            if current_page % 6 == 0:  # Solo para páginas pares
                save_progress(PROGRESS_FILE, progress_data)

            # Pasar a la siguiente página par
            current_page += 2

            # Pequeña pausa para no sobrecargar el servidor
            time.sleep(1)

            # Verificar si hay suficientes URLs en la cola para los workers
            # Si hay más de 20 URLs, hacer una pausa para permitir que los workers procesen
            if url_queue_even.qsize() > 20:
                logger.info(
                    f"Worker 4: Haciendo pausa para permitir que los workers procesen la cola (tamaño: {url_queue_even.qsize()})")
                time.sleep(5)  # Pausa reducida a 5 segundos para ser más eficiente

        # Guardar todas las URLs en un archivo JSON para referencia
        save_urls_to_json(all_series_urls, "even")

        logger.info(f"Worker 4: Finalizado. Total de series encontradas en páginas pares: {len(all_series_urls)}")
        return all_series_urls

    except Exception as e:
        logger.error(f"Worker 4: Error al extraer URLs: {e}")
        logger.debug(traceback.format_exc())
        with stats_lock:
            stats['errors'] += 1
        return all_series_urls
    finally:
        # Indicar que Worker 4 ha terminado
        worker4_active.clear()
        logger.info("Worker 4: Marcado como inactivo. Los demás workers continuarán hasta vaciar las colas.")


# Función para guardar URLs en un archivo JSON
def save_urls_to_json(all_urls, suffix=""):
    try:
        output_file = os.path.join(PROJECT_ROOT, "data", f"{SCRIPT_NAME}_urls_{suffix}.json")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        data = {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_urls": len(all_urls),
            "all_urls": all_urls
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"URLs guardadas en {output_file}")
    except Exception as e:
        logger.error(f"Error al guardar URLs en JSON: {e}")


# Función para obtener URLs de series de una página específica
def get_series_urls_from_page(driver, page_number):
    """Obtiene las URLs de todas las series en una página específica."""
    page_url = f"{SERIES_BASE_URL}/{page_number}"
    logger.info(f"Obteniendo series de la página {page_number}: {page_url}")

    try:
        driver.get(page_url)
        time.sleep(2)  # Esperar a que se cargue la página

        # Esperar a que aparezca el contenedor de series
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".span-6.inner-6.tt.view"))
            )
        except TimeoutException:
            logger.warning(f"Timeout esperando el contenedor de series en la página {page_number}")
            return []

        # Obtener el HTML de la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Buscar todos los divs de series
        series_divs = soup.find_all("div", class_="span-6 inner-6 tt view")
        logger.info(f"Encontrados {len(series_divs)} series en la página {page_number}")

        if not series_divs:
            logger.warning(f"No se encontraron series en la página {page_number}")
            return []

        # Extraer las URLs de las series
        series_urls = []
        for div in series_divs:
            try:
                # Buscar el enlace principal de la serie
                link_tag = div.find("a", class_="spec-border-ie")
                if link_tag and 'href' in link_tag.attrs:
                    series_href = link_tag['href']
                    series_url = BASE_URL + series_href if not series_href.startswith('http') else series_href
                    series_urls.append(series_url)
            except Exception as e:
                logger.error(f"Error al procesar un div de serie: {e}")
                continue

        logger.info(f"Se extrajeron {len(series_urls)} URLs de series de la página {page_number}")
        return series_urls

    except Exception as e:
        logger.error(f"Error al obtener series de la página {page_number}: {e}")
        logger.debug(traceback.format_exc())
        return []


# Worker 2: Extrae datos de las series de páginas impares y verifica en la base de datos
def worker2_series_extractor(driver, db_path, worker_id=0):
    logger.info(f"Worker 2 (ID {worker_id}): Iniciando extracción de datos de series de páginas impares")

    while not stop_event.is_set():
        try:
            # Obtener una URL de la cola con timeout
            try:
                series_url = url_queue_odd.get(timeout=5)
            except Empty:
                # Si la cola está vacía y el Worker 1 ha terminado, salir
                if url_queue_odd.empty() and not worker1_active.is_set():
                    logger.info(f"Worker 2 (ID {worker_id}): No hay más URLs para procesar y Worker 1 ha terminado")
                    break
                # Si la cola está vacía pero Worker 1 sigue activo, esperar
                logger.debug(f"Worker 2 (ID {worker_id}): Cola vacía, pero Worker 1 sigue activo. Esperando...")
                time.sleep(2)
                continue

            logger.info(f"Worker 2 (ID {worker_id}): Procesando serie: {series_url}")

            # Extraer información básica de la serie para verificar en la BD
            basic_info = extract_basic_series_info(driver, series_url, worker_id)

            if not basic_info:
                logger.error(
                    f"Worker 2 (ID {worker_id}): No se pudo extraer información básica de la serie: {series_url}")
                url_queue_odd.task_done()
                continue

            # Verificar si la serie ya existe en la BD
            series_exists_flag, series_id = check_series_in_db(basic_info, worker_id)

            if series_exists_flag:
                logger.info(
                    f"Worker 2 (ID {worker_id}): Serie '{basic_info['title']}' ya existe en la BD con ID {series_id}")

                # Verificar si necesitamos actualizar temporadas/episodios
                need_update = check_if_series_needs_update(driver, series_url, series_id, worker_id)

                if not need_update:
                    logger.info(f"Worker 2 (ID {worker_id}): Serie '{basic_info['title']}' está actualizada. Saltando.")
                    with stats_lock:
                        stats['skipped_series'] += 1
                    url_queue_odd.task_done()
                    continue

            # Si llegamos aquí, necesitamos extraer los detalles completos de la serie
            logger.info(f"Worker 2 (ID {worker_id}): Extrayendo detalles completos de la serie: {series_url}")
            series_data = extract_series_details(driver, series_url, basic_info, worker_id)

            if series_data:
                # Añadir el ID de la serie si ya existe
                if series_exists_flag:
                    series_data["id"] = series_id

                # Poner los datos en la cola para el Worker 3
                series_data_queue_odd.put({
                    "series_url": series_url,
                    "series_data": series_data,
                    "exists": series_exists_flag
                })
                logger.debug(f"Worker 2 (ID {worker_id}): Datos añadidos a la cola para Worker 3: {series_url}")

            # Marcar la tarea como completada
            url_queue_odd.task_done()

        except Exception as e:
            logger.error(f"Worker 2 (ID {worker_id}): Error al procesar URL: {e}")
            logger.debug(traceback.format_exc())
            with stats_lock:
                stats['errors'] += 1
            # Marcar la tarea como completada para no bloquear la cola
            try:
                url_queue_odd.task_done()
            except:
                pass

    logger.info(f"Worker 2 (ID {worker_id}): Finalizado")
    # Indicar que Worker 2 ha terminado
    worker2_active.clear()


# Worker 5: Extrae datos de las series de páginas pares y verifica en la base de datos
def worker5_series_extractor(driver, db_path, worker_id=0):
    logger.info(f"Worker 5 (ID {worker_id}): Iniciando extracción de datos de series de páginas pares")

    while not stop_event.is_set():
        try:
            # Obtener una URL de la cola con timeout
            try:
                series_url = url_queue_even.get(timeout=5)
            except Empty:
                # Si la cola está vacía y el Worker 4 ha terminado, salir
                if url_queue_even.empty() and not worker4_active.is_set():
                    logger.info(f"Worker 5 (ID {worker_id}): No hay más URLs para procesar y Worker 4 ha terminado")
                    break
                # Si la cola está vacía pero Worker 4 sigue activo, esperar
                logger.debug(f"Worker 5 (ID {worker_id}): Cola vacía, pero Worker 4 sigue activo. Esperando...")
                time.sleep(2)
                continue

            logger.info(f"Worker 5 (ID {worker_id}): Procesando serie: {series_url}")

            # Extraer información básica de la serie para verificar en la BD
            basic_info = extract_basic_series_info(driver, series_url, worker_id)

            if not basic_info:
                logger.error(
                    f"Worker 5 (ID {worker_id}): No se pudo extraer información básica de la serie: {series_url}")
                url_queue_even.task_done()
                continue

            # Verificar si la serie ya existe en la BD
            series_exists_flag, series_id = check_series_in_db(basic_info, worker_id)

            if series_exists_flag:
                logger.info(
                    f"Worker 5 (ID {worker_id}): Serie '{basic_info['title']}' ya existe en la BD con ID {series_id}")

                # Verificar si necesitamos actualizar temporadas/episodios
                need_update = check_if_series_needs_update(driver, series_url, series_id, worker_id)

                if not need_update:
                    logger.info(f"Worker 5 (ID {worker_id}): Serie '{basic_info['title']}' está actualizada. Saltando.")
                    with stats_lock:
                        stats['skipped_series'] += 1
                    url_queue_even.task_done()
                    continue

            # Si llegamos aquí, necesitamos extraer los detalles completos de la serie
            logger.info(f"Worker 5 (ID {worker_id}): Extrayendo detalles completos de la serie: {series_url}")
            series_data = extract_series_details(driver, series_url, basic_info, worker_id)

            if series_data:
                # Añadir el ID de la serie si ya existe
                if series_exists_flag:
                    series_data["id"] = series_id

                # Poner los datos en la cola para el Worker 6
                series_data_queue_even.put({
                    "series_url": series_url,
                    "series_data": series_data,
                    "exists": series_exists_flag
                })
                logger.debug(f"Worker 5 (ID {worker_id}): Datos añadidos a la cola para Worker 6: {series_url}")

            # Marcar la tarea como completada
            url_queue_even.task_done()

        except Exception as e:
            logger.error(f"Worker 5 (ID {worker_id}): Error al procesar URL: {e}")
            logger.debug(traceback.format_exc())
            with stats_lock:
                stats['errors'] += 1
            # Marcar la tarea como completada para no bloquear la cola
            try:
                url_queue_even.task_done()
            except:
                pass

    logger.info(f"Worker 5 (ID {worker_id}): Finalizado")
    # Indicar que Worker 5 ha terminado
    worker5_active.clear()


# Función para extraer información básica de la serie
def extract_basic_series_info(driver, series_url, worker_id=0):
    """Extrae información básica de la serie para verificar en la BD."""
    logger.info(f"[Worker {worker_id}] Extrayendo información básica de la serie: {series_url}")

    try:
        driver.get(series_url)
        time.sleep(2)  # Esperar a que se cargue la página

        # Esperar a que aparezca la información de la serie
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "summary-title"))
            )
        except TimeoutException:
            logger.error(f"[Worker {worker_id}] Timeout esperando información de la serie en {series_url}")
            return None

        # Obtener el HTML de la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Extraer título de la serie
        title_div = soup.find("div", id="summary-title")
        if not title_div:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar el título de la serie en {series_url}")
            return None

        series_title = title_div.text.strip()

        # Extraer información adicional de la serie desde show-details
        show_details = soup.find("div", class_="show-details")

        # Buscar el contenedor de información detallada
        summary_overview = soup.find("div", id="summary-overview-wrapper")

        series_year = None
        imdb_rating = None
        genre = None
        status = None
        director = None

        # Extraer información del contenedor principal
        if summary_overview:
            info_div = summary_overview.find("div")
            if info_div:
                # Extraer estado
                status_p = info_div.find("p", string=lambda s: s and "Estado:" in s if s else False)
                if status_p and status_p.find("a"):
                    status = status_p.find("a").text.strip()

                # Extraer año
                year_p = info_div.find("p", string=lambda s: s and "Año:" in s if s else False)
                if year_p and year_p.find("a"):
                    try:
                        series_year = int(year_p.find("a").text.strip())
                    except ValueError:
                        logger.warning(f"[Worker {worker_id}] No se pudo convertir el año a entero")

                # Extraer IMDB rating
                imdb_p = info_div.find("p", string=lambda s: s and "IMDB Rating:" in s if s else False)
                if imdb_p and imdb_p.find("a"):
                    rating_text = imdb_p.find("a").text.strip().split()[0]
                    try:
                        imdb_rating = float(rating_text)
                    except ValueError:
                        logger.warning(f"[Worker {worker_id}] No se pudo convertir el rating a float: {rating_text}")

                # Extraer género
                genre_p = info_div.find("p", string=lambda s: s and "Género:" in s if s else False)
                if genre_p:
                    genre_tags = genre_p.find_all("a")
                    genre = ", ".join([tag.text.strip() for tag in genre_tags]) if genre_tags else None

                # Extraer director
                director_p = info_div.find("p", string=lambda s: s and "Director:" in s if s else False)
                if director_p:
                    director_tags = director_p.find_all("a")
                    director = ", ".join([tag.text.strip() for tag in director_tags]) if director_tags else None

        # Si no se encontró información en el contenedor principal, intentar con show-details
        if show_details and (series_year is None or imdb_rating is None or genre is None or status is None):
            # Extraer estado
            if status is None:
                status_p = show_details.find("p", string=lambda s: s and "Estado:" in s if s else False)
                if status_p and status_p.find("a"):
                    status = status_p.find("a").text.strip()

            # Extraer año
            if series_year is None:
                year_p = show_details.find("p", string=lambda s: s and "Año:" in s if s else False)
                if year_p and year_p.find("a"):
                    try:
                        series_year = int(year_p.find("a").text.strip())
                    except ValueError:
                        logger.warning(f"[Worker {worker_id}] No se pudo convertir el año a entero")

            # Extraer IMDB rating
            if imdb_rating is None:
                imdb_p = show_details.find("p", string=lambda s: s and "IMDB Rating:" in s if s else False)
                if imdb_p and imdb_p.find("a"):
                    rating_text = imdb_p.find("a").text.strip().split()[0]
                    try:
                        imdb_rating = float(rating_text)
                    except ValueError:
                        logger.warning(f"[Worker {worker_id}] No se pudo convertir el rating a float: {rating_text}")

            # Extraer género
            if genre is None:
                genre_p = show_details.find("p", string=lambda s: s and "Género:" in s if s else False)
                if genre_p:
                    genre_tags = genre_p.find_all("a")
                    genre = ", ".join([tag.text.strip() for tag in genre_tags]) if genre_tags else None

            # Extraer director
            if director is None:
                director_p = show_details.find("p", string=lambda s: s and "Director:" in s if s else False)
                if director_p:
                    director_tags = director_p.find_all("a")
                    director = ", ".join([tag.text.strip() for tag in director_tags]) if director_tags else None

        logger.info(f"[Worker {worker_id}] Información básica extraída: Título={series_title}, Año={series_year}, "
                    f"IMDB={imdb_rating}, Género={genre}, Estado={status}, Director={director}")

        return {
            "title": series_title,
            "url": series_url,
            "year": series_year,
            "imdb_rating": imdb_rating,
            "genre": genre,
            "status": status,
            "director": director
        }

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer información básica de la serie {series_url}: {e}")
        logger.debug(traceback.format_exc())
        return None


# Función para verificar si la serie existe en la BD
def check_series_in_db(series_info, worker_id=0):
    """Verifica si la serie ya existe en la base de datos."""
    if not series_info:
        return False, None

    with db_lock:
        connection = connect_db()
        cursor = connection.cursor()
        try:
            # Primero intentamos buscar por título exacto y año
            if series_info["year"]:
                cursor.execute('''
                    SELECT id FROM media_downloads 
                    WHERE title=? AND year=? AND type='serie'
                ''', (series_info["title"], series_info["year"]))
                result = cursor.fetchone()
                if result:
                    logger.info(
                        f"[Worker {worker_id}] Serie encontrada por título y año: {series_info['title']} ({series_info['year']})")
                    return True, result['id']

            # Si no se encuentra, intentamos buscar solo por título
            cursor.execute('''
                SELECT id FROM media_downloads 
                WHERE title=? AND type='serie'
            ''', (series_info["title"],))
            result = cursor.fetchone()

            exists = result is not None
            logger.debug(
                f"[Worker {worker_id}] Verificación de existencia de serie: {series_info['title']} - {'Existe' if exists else 'No existe'}")
            return exists, result['id'] if exists else None

        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error al verificar si la serie existe en la BD: {e}")
            return False, None
        finally:
            cursor.close()
            connection.close()


# Función para verificar si una serie necesita actualización
def check_if_series_needs_update(driver, series_url, series_id, worker_id=0):
    """Verifica si una serie necesita actualización (nuevas temporadas o episodios)."""
    logger.info(f"[Worker {worker_id}] Verificando si la serie con ID {series_id} necesita actualización")

    try:
        # Obtener las temporadas disponibles en la web
        available_seasons = get_available_seasons(driver, series_url, worker_id)

        if not available_seasons:
            logger.warning(f"[Worker {worker_id}] No se encontraron temporadas para la serie con ID {series_id}")
            return False

        # Verificar las temporadas en la BD
        with db_lock:
            connection = connect_db()
            cursor = connection.cursor()

            try:
                # Obtener las temporadas existentes en la BD
                cursor.execute('''
                    SELECT season FROM series_seasons 
                    WHERE movie_id=?
                ''', (series_id,))

                existing_seasons = [row['season'] for row in cursor.fetchall()]

                # Verificar si hay nuevas temporadas
                for season_number, season_url, episode_count in available_seasons:
                    if season_number not in existing_seasons:
                        logger.info(
                            f"[Worker {worker_id}] Se encontró una nueva temporada {season_number} para la serie con ID {series_id}")
                        return True

                    # Si la temporada existe, verificar si hay nuevos episodios
                    cursor.execute('''
                        SELECT ss.id, COUNT(se.id) as episode_count
                        FROM series_seasons ss
                        LEFT JOIN series_episodes se ON ss.id = se.season_id
                        WHERE ss.movie_id=? AND ss.season=?
                        GROUP BY ss.id
                    ''', (series_id, season_number))

                    result = cursor.fetchone()
                    if result:
                        season_id = result['id']
                        db_episode_count = result['episode_count']

                        if episode_count > db_episode_count:
                            logger.info(
                                f"[Worker {worker_id}] Se encontraron nuevos episodios en temporada {season_number} para la serie con ID {series_id} (Web: {episode_count}, BD: {db_episode_count})")
                            return True

                logger.info(f"[Worker {worker_id}] La serie con ID {series_id} está actualizada")
                return False

            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error al verificar si la serie necesita actualización: {e}")
                return True  # En caso de error, asumimos que necesita actualización
            finally:
                cursor.close()
                connection.close()

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al verificar si la serie necesita actualización: {e}")
        return True  # En caso de error, asumimos que necesita actualización


# Función para obtener las temporadas disponibles
def get_available_seasons(driver, series_url, worker_id=0):
    """Obtiene las temporadas disponibles para una serie."""
    logger.info(f"[Worker {worker_id}] Obteniendo temporadas disponibles para: {series_url}")

    seasons = []
    season_number = 1
    max_empty_seasons = 3  # Máximo número de temporadas vacías consecutivas antes de parar
    empty_seasons_count = 0

    while empty_seasons_count < max_empty_seasons and not stop_event.is_set():
        season_url = f"{series_url}/temporada-{season_number}"
        logger.info(f"[Worker {worker_id}] Verificando temporada {season_number}: {season_url}")

        # Verificar si la temporada existe y tiene episodios
        season_exists, episode_count = check_season_exists(driver, season_url, worker_id)

        if season_exists and episode_count > 0:
            seasons.append((season_number, season_url, episode_count))
            logger.info(f"[Worker {worker_id}] Temporada {season_number} encontrada con {episode_count} episodios")
            empty_seasons_count = 0  # Reiniciar contador de temporadas vacías
        else:
            empty_seasons_count += 1
            logger.info(
                f"[Worker {worker_id}] Temporada {season_number} no encontrada o sin episodios. Contador: {empty_seasons_count}/{max_empty_seasons}")

        season_number += 1

        # Evitar bucles infinitos si hay demasiadas temporadas
        if season_number > 100:  # Límite arbitrario para evitar bucles infinitos
            logger.warning(f"[Worker {worker_id}] Se alcanzó el límite de 100 temporadas. Finalizando búsqueda.")
            break

    return seasons


# Función para verificar si una temporada existe comprobando si tiene episodios
def check_season_exists(driver, season_url, worker_id=0):
    """Verifica si una temporada existe comprobando si tiene episodios."""
    logger.info(f"[Worker {worker_id}] Verificando si la temporada existe: {season_url}")

    try:
        # Usar un enfoque más robusto para cargar la página
        for attempt in range(3):  # Intentar hasta 3 veces
            try:
                driver.get(season_url)
                time.sleep(2)  # Esperar a que cargue la página
                break
            except Exception as e:
                if attempt < 2:  # Si no es el último intento
                    logger.warning(f"[Worker {worker_id}] Error al cargar la temporada, reintentando: {e}")
                    time.sleep(2)
                else:
                    raise  # Si es el último intento, propagar la excepción

        # Usar BeautifulSoup para analizar la página en lugar de interactuar directamente con el DOM
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Buscar el contenedor de episodios
        season_episodes_div = soup.find("div", id="season-episodes")
        if not season_episodes_div:
            logger.info(f"[Worker {worker_id}] No se encontró el contenedor de episodios en {season_url}")
            return False, 0

        # Contar los episodios
        episode_divs = season_episodes_div.find_all("div", class_="span-6 tt view show-view")
        episode_count = len(episode_divs)

        logger.info(f"[Worker {worker_id}] Temporada tiene {episode_count} episodios")

        # Si hay al menos un episodio, la temporada existe
        return episode_count > 0, episode_count

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al verificar temporada: {e}")
        logger.debug(traceback.format_exc())
        return False, 0


# Función para procesar episodios usando BeautifulSoup
def process_episodes_with_soup(driver, season_url, worker_id=0):
    """Procesa los episodios de una temporada usando BeautifulSoup para evitar errores de elementos obsoletos."""
    logger.info(f"[Worker {worker_id}] Procesando episodios con BeautifulSoup: {season_url}")

    episodes_data = []

    try:
        # Usar un enfoque más robusto para cargar la página
        for attempt in range(3):  # Intentar hasta 3 veces
            try:
                driver.get(season_url)
                time.sleep(2)  # Esperar a que cargue la página
                break
            except Exception as e:
                if attempt < 2:  # Si no es el último intento
                    logger.warning(f"[Worker {worker_id}] Error al cargar la temporada, reintentando: {e}")
                    time.sleep(2)
                else:
                    raise  # Si es el último intento, propagar la excepción

        # Obtener el HTML de la página
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Buscar el contenedor de episodios
        season_episodes_div = soup.find("div", id="season-episodes")
        if not season_episodes_div:
            logger.warning(f"[Worker {worker_id}] No se encontró el contenedor de episodios en {season_url}")
            return episodes_data

        # Encontrar todos los episodios
        episode_divs = season_episodes_div.find_all("div", class_="span-6 tt view show-view")
        logger.info(f"[Worker {worker_id}] Encontrados {len(episode_divs)} episodios en {season_url}")

        for episode_div in episode_divs:
            try:
                # Extraer número de episodio
                rating_div = episode_div.find("div", class_="rating")
                if not rating_div:
                    logger.warning(f"[Worker {worker_id}] No se encontró el div de rating para un episodio")
                    continue

                episode_text = rating_div.text.strip()
                episode_match = re.search(r'(\d+)x(\d+)', episode_text)

                if not episode_match:
                    logger.warning(f"[Worker {worker_id}] No se pudo extraer el número de episodio: {episode_text}")
                    continue

                episode_number = int(episode_match.group(2))

                # Extraer título del episodio
                title_elem = episode_div.find("a", class_="link title-ellipsis")
                if not title_elem:
                    logger.warning(f"[Worker {worker_id}] No se encontró el título para el episodio {episode_number}")
                    continue

                episode_title = title_elem.get("title", "").split(" - ")[-1]
                episode_url = title_elem.get("href", "")

                if not episode_url.startswith("http"):
                    episode_url = BASE_URL + episode_url

                logger.info(
                    f"[Worker {worker_id}] Procesando episodio {episode_number}: {episode_title} - {episode_url}")

                # Extraer enlaces del episodio
                episode_links = extract_episode_links(driver, episode_url, worker_id)

                episodes_data.append({
                    "number": episode_number,
                    "title": episode_title,
                    "links": episode_links
                })

            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error al procesar episodio: {e}")
                logger.debug(traceback.format_exc())
                continue

        return episodes_data

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al procesar episodios con BeautifulSoup: {e}")
        logger.debug(traceback.format_exc())
        return episodes_data


# Función para extraer detalles de una serie
def extract_series_details(driver, series_url, basic_info, worker_id=0):
    """Extrae los detalles completos de una serie desde su página."""
    logger.info(f"[Worker {worker_id}] Extrayendo detalles completos de la serie: {series_url}")

    try:
        # Usar la información básica que ya tenemos
        series_title = basic_info["title"]
        series_year = basic_info["year"]
        imdb_rating = basic_info["imdb_rating"]
        genre = basic_info["genre"]
        status = basic_info["status"]
        director = basic_info.get("director")

        # Extraer temporadas
        seasons_data = []

        # Obtener todas las temporadas disponibles
        seasons = get_available_seasons(driver, series_url, worker_id)

        if not seasons:
            logger.warning(f"[Worker {worker_id}] No se encontraron temporadas para la serie: {series_title}")
            # Devolver la información básica sin temporadas
            return {
                "title": series_title,
                "url": series_url,
                "year": series_year,
                "imdb_rating": imdb_rating,
                "genre": genre,
                "status": status,
                "director": director,
                "seasons": []
            }

        # Procesar cada temporada encontrada
        for season_number, season_url, episode_count in seasons:
            logger.info(
                f"[Worker {worker_id}] Procesando temporada {season_number}: {season_url} con {episode_count} episodios")

            try:
                # Usar BeautifulSoup para procesar los episodios y evitar errores de elementos obsoletos
                episodes_data = process_episodes_with_soup(driver, season_url, worker_id)

                if episodes_data:
                    seasons_data.append({
                        "number": season_number,
                        "episodes": episodes_data
                    })
                else:
                    logger.warning(
                        f"[Worker {worker_id}] No se encontraron episodios para la temporada {season_number}")

            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error al procesar temporada {season_number}: {e}")
                logger.debug(traceback.format_exc())
                continue

        return {
            "title": series_title,
            "url": series_url,
            "year": series_year,
            "imdb_rating": imdb_rating,
            "genre": genre,
            "status": status,
            "director": director,
            "seasons": seasons_data
        }

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles de la serie {series_url}: {e}")
        logger.debug(traceback.format_exc())
        return None


# Función para extraer enlaces de un episodio
def extract_episode_links(driver, episode_url, worker_id=0):
    """Extrae los enlaces de streaming de un episodio."""
    logger.info(f"[Worker {worker_id}] Extrayendo enlaces del episodio: {episode_url}")

    links = []
    max_retries = 3

    try:
        # Implementar reintentos para cargar la página
        for attempt in range(max_retries):
            try:
                driver.get(episode_url)
                time.sleep(2)  # Esperar a que cargue la página

                # Esperar a que aparezca la lista de enlaces
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "embed-list"))
                )
                break  # Si llegamos aquí, la página cargó correctamente
            except TimeoutException:
                if attempt < max_retries - 1:
                    logger.warning(f"[Worker {worker_id}] Intento {attempt + 1}/{max_retries} fallido. Reintentando...")
                    time.sleep(2)
                else:
                    logger.warning(
                        f"[Worker {worker_id}] Timeout esperando enlaces en episodio {episode_url} después de {max_retries} intentos")
                    return links
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"[Worker {worker_id}] Error al cargar la página: {e}. Reintentando...")
                    time.sleep(2)
                else:
                    logger.error(
                        f"[Worker {worker_id}] Error al cargar la página después de {max_retries} intentos: {e}")
                    return links

        # Obtener el HTML de la página para analizar con BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Buscar todos los selectores de enlaces
        embed_selectors = soup.find_all("div", class_="embed-selector")
        logger.info(f"[Worker {worker_id}] Encontrados {len(embed_selectors)} enlaces en episodio {episode_url}")

        # Procesar cada selector de enlace
        for i, selector_soup in enumerate(embed_selectors):
            try:
                # Extraer información del enlace
                # Buscar el idioma en el elemento h5 con clase "left"
                h5_left = selector_soup.find("h5", class_="left")
                language = "Desconocido"

                if h5_left:
                    # Buscar el elemento b con clase "key" que contiene "Idioma:"
                    idioma_key = h5_left.find("b", class_="key", string=lambda s: s and "Idioma:" in s if s else False)
                    if idioma_key:
                        # El idioma está en el texto que sigue al elemento b
                        idioma_text = idioma_key.next_sibling
                        if idioma_text:
                            language = idioma_text.strip()

                # Si no se encontró el idioma con el método anterior, usar el método alternativo
                if language == "Desconocido":
                    if "Audio Español" in selector_soup.text:
                        language = "Audio Español"
                    elif "Subtítulo Español" in selector_soup.text:
                        language = "Subtítulo Español"
                    elif "Audio Latino" in selector_soup.text:
                        language = "Audio Latino"
                    elif "Audio Original" in selector_soup.text:
                        language = "Audio Original"

                # Extraer servidor
                server_elem = selector_soup.find("b", class_="provider")
                server = server_elem.text.strip() if server_elem else "Desconocido"

                # Extraer calidad
                quality = "HD1080"  # Valor por defecto
                if "HD1080" in selector_soup.text:
                    quality = "HD1080"
                elif "HD720" in selector_soup.text:
                    quality = "HD720"
                elif "SD" in selector_soup.text:
                    quality = "SD"

                logger.debug(
                    f"[Worker {worker_id}] Enlace {i + 1}: Idioma={language}, Servidor={server}, Calidad={quality}")

                # Ahora necesitamos hacer clic en el selector para mostrar el enlace
                # Para esto, necesitamos usar Selenium
                try:
                    # Crear un nuevo driver para cada enlace para evitar problemas de conexión
                    temp_driver = None
                    try:
                        # Usar el driver existente primero
                        # Encontrar el selector en la página actual usando un identificador único
                        selector_elements = driver.find_elements(By.CLASS_NAME, "embed-selector")

                        if i < len(selector_elements):
                            # Hacer clic en el selector
                            try:
                                selector_elements[i].click()
                                time.sleep(1)  # Esperar a que se muestre el enlace
                            except Exception as click_error:
                                logger.warning(
                                    f"[Worker {worker_id}] Error al hacer clic en el selector {i + 1}: {click_error}")
                                # Si falla el clic, intentar con JavaScript
                                driver.execute_script("arguments[0].click();", selector_elements[i])
                                time.sleep(1)

                            # Buscar el iframe con el enlace
                            try:
                                embed_movie = WebDriverWait(driver, 5).until(
                                    EC.presence_of_element_located((By.CLASS_NAME, "embed-movie"))
                                )
                                iframe = embed_movie.find_element(By.TAG_NAME, "iframe")
                                link_url = iframe.get_attribute("src")

                                # Añadir el enlace a la lista
                                links.append({
                                    "language": language,
                                    "server": server,
                                    "quality": quality,
                                    "url": link_url
                                })

                                logger.info(f"[Worker {worker_id}] Enlace extraído: {link_url}")
                            except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                                logger.error(f"[Worker {worker_id}] Error al obtener iframe: {e}")
                                # Intentar extraer el enlace directamente del HTML
                                try:
                                    updated_page_source = driver.page_source
                                    updated_soup = BeautifulSoup(updated_page_source, "html.parser")
                                    embed_movie_div = updated_soup.find("div", class_="embed-movie")
                                    if embed_movie_div:
                                        iframe_tag = embed_movie_div.find("iframe")
                                        if iframe_tag and 'src' in iframe_tag.attrs:
                                            link_url = iframe_tag['src']
                                            links.append({
                                                "language": language,
                                                "server": server,
                                                "quality": quality,
                                                "url": link_url
                                            })
                                            logger.info(f"[Worker {worker_id}] Enlace extraído desde HTML: {link_url}")
                                except Exception as html_error:
                                    logger.error(
                                        f"[Worker {worker_id}] Error al extraer enlace desde HTML: {html_error}")
                        else:
                            logger.warning(
                                f"[Worker {worker_id}] No se pudo encontrar el selector {i + 1} en la página")

                    except Exception as e:
                        logger.error(f"[Worker {worker_id}] Error al hacer clic en el selector {i + 1}: {e}")
                        continue
                    finally:
                        if temp_driver:
                            temp_driver.quit()

                except Exception as e:
                    logger.error(f"[Worker {worker_id}] Error al procesar selector {i + 1}: {e}")
                    continue

            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error al procesar selector {i + 1}: {e}")
                logger.debug(traceback.format_exc())
                continue

        return links

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer enlaces del episodio {episode_url}: {e}")
        logger.debug(traceback.format_exc())
        return links


# Worker 3: Procesa las series verificadas de páginas impares, extrae enlaces y los inserta en la BD
def worker3_db_processor(db_path, progress_data, worker_id=0):
    logger.info(f"Worker 3 (ID {worker_id}): Iniciando procesamiento de series de páginas impares en la base de datos")

    processed_urls = set(progress_data.get('processed_urls_odd', []))

    while not stop_event.is_set():
        try:
            # Obtener datos de la cola con timeout
            try:
                data = series_data_queue_odd.get(timeout=5)
            except Empty:
                # Si la cola está vacía y los workers anteriores han terminado, salir
                if series_data_queue_odd.empty() and (not worker1_active.is_set() and not worker2_active.is_set()):
                    logger.info(
                        f"Worker 3 (ID {worker_id}): No hay más series para procesar y workers anteriores han terminado")
                    break
                # Si la cola está vacía pero algún worker anterior sigue activo, esperar
                logger.debug(
                    f"Worker 3 (ID {worker_id}): Cola vacía, pero workers anteriores siguen activos. Esperando...")
                time.sleep(2)
                continue

            series_url = data["series_url"]
            series_data = data["series_data"]
            series_exists = data.get("exists", False)

            logger.info(f"Worker 3 (ID {worker_id}): Procesando serie en BD: {series_url}")

            # Guardar la serie en la base de datos
            with db_lock:
                result = save_series_to_db(series_data, series_exists, db_path)

            if result:
                logger.info(f"Worker 3 (ID {worker_id}): Serie guardada correctamente: {series_url}")

                # Actualizar estadísticas
                with stats_lock:
                    stats['series_processed'] += 1

                # Poner los datos en la cola para estadísticas finales
                processed_series_queue.put({
                    "series_url": series_url,
                    "series_data": series_data,
                    "result": result
                })

                # Marcar la URL como procesada
                processed_urls.add(series_url)
                with progress_lock:
                    progress_data['processed_urls_odd'] = list(processed_urls)

                # Guardar progreso periódicamente
                if len(processed_urls) % 10 == 0:
                    save_progress(PROGRESS_FILE, progress_data)
            else:
                logger.error(f"Worker 3 (ID {worker_id}): Error al guardar la serie: {series_url}")
                with stats_lock:
                    stats['errors'] += 1

            # Marcar la tarea como completada
            series_data_queue_odd.task_done()

        except Exception as e:
            logger.error(f"Worker 3 (ID {worker_id}): Error al procesar serie: {e}")
            logger.debug(traceback.format_exc())
            with stats_lock:
                stats['errors'] += 1
            # Marcar la tarea como completada para no bloquear la cola
            try:
                series_data_queue_odd.task_done()
            except:
                pass

    logger.info(f"Worker 3 (ID {worker_id}): Finalizado")


# Worker 6: Procesa las series verificadas de páginas pares, extrae enlaces y los inserta en la BD
def worker6_db_processor(db_path, progress_data, worker_id=0):
    logger.info(f"Worker 6 (ID {worker_id}): Iniciando procesamiento de series de páginas pares en la base de datos")

    processed_urls = set(progress_data.get('processed_urls_even', []))

    while not stop_event.is_set():
        try:
            # Obtener datos de la cola con timeout
            try:
                data = series_data_queue_even.get(timeout=5)
            except Empty:
                # Si la cola está vacía y los workers anteriores han terminado, salir
                if series_data_queue_even.empty() and (not worker4_active.is_set() and not worker5_active.is_set()):
                    logger.info(
                        f"Worker 6 (ID {worker_id}): No hay más series para procesar y workers anteriores han terminado")
                    break
                # Si la cola está vacía pero algún worker anterior sigue activo, esperar
                logger.debug(
                    f"Worker 6 (ID {worker_id}): Cola vacía, pero workers anteriores siguen activos. Esperando...")
                time.sleep(2)
                continue

            series_url = data["series_url"]
            series_data = data["series_data"]
            series_exists = data.get("exists", False)

            logger.info(f"Worker 6 (ID {worker_id}): Procesando serie en BD: {series_url}")

            # Guardar la serie en la base de datos
            with db_lock:
                result = save_series_to_db(series_data, series_exists, db_path)

            if result:
                logger.info(f"Worker 6 (ID {worker_id}): Serie guardada correctamente: {series_url}")

                # Actualizar estadísticas
                with stats_lock:
                    stats['series_processed'] += 1

                # Poner los datos en la cola para estadísticas finales
                processed_series_queue.put({
                    "series_url": series_url,
                    "series_data": series_data,
                    "result": result
                })

                # Marcar la URL como procesada
                processed_urls.add(series_url)
                with progress_lock:
                    progress_data['processed_urls_even'] = list(processed_urls)

                # Guardar progreso periódicamente
                if len(processed_urls) % 10 == 0:
                    save_progress(PROGRESS_FILE, progress_data)
            else:
                logger.error(f"Worker 6 (ID {worker_id}): Error al guardar la serie: {series_url}")
                with stats_lock:
                    stats['errors'] += 1

            # Marcar la tarea como completada
            series_data_queue_even.task_done()

        except Exception as e:
            logger.error(f"Worker 6 (ID {worker_id}): Error al procesar serie: {e}")
            logger.debug(traceback.format_exc())
            with stats_lock:
                stats['errors'] += 1
            # Marcar la tarea como completada para no bloquear la cola
            try:
                series_data_queue_even.task_done()
            except:
                pass

    logger.info(f"Worker 6 (ID {worker_id}): Finalizado")


# Función para guardar una serie en la base de datos
def save_series_to_db(series_data, series_exists=False, db_path=None):
    """Guarda una serie y sus temporadas/episodios en la base de datos."""
    if not series_data:
        return False

    connection = connect_db(db_path)
    cursor = connection.cursor()

    try:
        # Si la serie ya existe, usar el ID existente
        if series_exists:
            series_id = series_data.get("id")
            logger.info(f"Usando serie existente con ID {series_id}: {series_data['title']}")

            # Actualizar información de la serie si es necesario
            cursor.execute('''
                UPDATE media_downloads
                SET year=?, imdb_rating=?, genre=?, updated_at=datetime('now')
                WHERE id=?
            ''', (series_data["year"], series_data["imdb_rating"], series_data["genre"], series_id))
        else:
            # Insertar nueva serie
            logger.info(f"Insertando nueva serie: {series_data['title']} ({series_data['year']})")
            cursor.execute('''
                INSERT INTO media_downloads (title, year, imdb_rating, genre, type)
                VALUES (?, ?, ?, ?, 'serie')
            ''', (series_data["title"], series_data["year"], series_data["imdb_rating"], series_data["genre"]))
            series_id = cursor.lastrowid
            if not series_id:
                logger.error(f"Error al insertar la serie. Abortando.")
                connection.close()
                return False
            with stats_lock:
                stats['new_series'] += 1

        # Procesar temporadas y episodios
        for season_data in series_data["seasons"]:
            season_number = season_data["number"]

            # Verificar si la temporada existe
            season_exists_flag, season_id = season_exists(series_id, season_number, connection, db_path)

            # Si la temporada no existe, insertarla
            if not season_exists_flag:
                logger.info(f"Temporada {season_number} no encontrada. Insertando nueva temporada.")
                cursor.execute('''
                    INSERT INTO series_seasons (movie_id, season)
                    VALUES (?, ?)
                ''', (series_id, season_number))
                season_id = cursor.lastrowid
                if not season_id:
                    logger.error(f"Error al insertar la temporada. Abortando.")
                    connection.close()
                    return False
                with stats_lock:
                    stats['new_seasons'] += 1
            else:
                logger.info(f"Temporada {season_number} encontrada con ID {season_id}")
                with stats_lock:
                    stats['skipped_seasons'] += 1

            # Procesar episodios
            for episode_data in season_data["episodes"]:
                episode_number = episode_data["number"]
                episode_title = episode_data["title"]

                # Verificar si el episodio existe
                episode_exists_flag, episode_id = episode_exists(season_id, episode_number, episode_title)

                # Si el episodio no existe, insertarlo
                if not episode_exists_flag:
                    logger.info(f"Episodio {episode_number} no encontrado. Insertando nuevo episodio.")
                    cursor.execute('''
                        INSERT INTO series_episodes (season_id, episode, title)
                        VALUES (?, ?, ?)
                    ''', (season_id, episode_number, episode_title))
                    episode_id = cursor.lastrowid
                    if not episode_id:
                        logger.error(f"Error al insertar el episodio. Abortando.")
                        connection.close()
                        return False
                    with stats_lock:
                        stats['new_episodes'] += 1
                else:
                    logger.info(f"Episodio {episode_number} encontrado con ID {episode_id}")
                    with stats_lock:
                        stats['skipped_episodes'] += 1

                # Procesar enlaces del episodio
                if "links" in episode_data and episode_data["links"]:
                    for link_data in episode_data["links"]:
                        # Insertar servidor si no existe
                        cursor.execute("SELECT id FROM servers WHERE name = ?", (link_data["server"],))
                        server_result = cursor.fetchone()
                        if server_result:
                            server_id = server_result[0]
                        else:
                            cursor.execute("INSERT INTO servers (name) VALUES (?)", (link_data["server"],))
                            server_id = cursor.lastrowid

                        # Insertar calidad si no existe
                        cursor.execute("SELECT quality_id FROM qualities WHERE quality = ?", (link_data["quality"],))
                        quality_result = cursor.fetchone()
                        if quality_result:
                            quality_id = quality_result[0]
                        else:
                            cursor.execute("INSERT INTO qualities (quality) VALUES (?)", (link_data["quality"],))
                            quality_id = cursor.lastrowid

                        # Verificar si el enlace ya existe
                        cursor.execute(
                            "SELECT id FROM links_files_download WHERE episode_id = ? AND server_id = ? AND link = ?",
                            (episode_id, server_id, link_data["url"])
                        )
                        link_exists = cursor.fetchone()

                        if not link_exists:
                            # Insertar el enlace
                            cursor.execute(
                                """
                                INSERT INTO links_files_download 
                                (movie_id, server_id, language, link, quality_id, episode_id) 
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (series_id, server_id, link_data["language"], link_data["url"], quality_id, episode_id)
                            )
                            logger.info(
                                f"Nuevo enlace insertado para episodio {episode_number}: {link_data['server']} - {link_data['language']}")
                            with stats_lock:
                                stats['new_links'] += 1
                        else:
                            logger.debug(
                                f"Enlace ya existe para episodio {episode_number}: {link_data['server']} - {link_data['language']}")

        connection.commit()
        return True

    except Exception as e:
        logger.error(f"Error al guardar la serie en la base de datos: {e}")
        logger.debug(traceback.format_exc())
        connection.rollback()
        return False

    finally:
        cursor.close()
        connection.close()


# Función para generar informe de actualización
def generate_update_report(start_time):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos

    # Generar informe
    report = f"""
INFORME DE ACTUALIZACIÓN DE SERIES - {end_time.strftime('%Y-%m-%d %H:%M:%S')}
===========================================================================

Duración: {duration:.2f} minutos

RESUMEN:
- Páginas procesadas: {stats['pages_processed']}
- Series encontradas: {stats['series_found']}
- Series procesadas: {stats['series_processed']}
- Series saltadas (ya actualizadas): {stats['skipped_series']}
- Nuevas series añadidas: {stats['new_series']}
- Temporadas saltadas (ya existentes): {stats['skipped_seasons']}
- Nuevas temporadas añadidas: {stats['new_seasons']}
- Episodios saltados (ya existentes): {stats['skipped_episodes']}
- Nuevos episodios añadidos: {stats['new_episodes']}
- Nuevos enlaces añadidos: {stats['new_links']}
- Errores: {stats['errors']}

===========================================================================
Este es un mensaje automático generado por el sistema de actualización de series.
"""

    return report


# Función principal para procesar todas las series
def process_all_series(start_page=1, max_pages=None, db_path=None, max_workers=None):
    """Procesa todas las series disponibles."""
    start_time = datetime.now()
    logger.info(f"Iniciando procesamiento de series: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Usar el número de workers especificado o el valor por defecto
    if max_workers is None:
        max_workers = MAX_WORKERS

    try:
        # Configurar la base de datos
        setup_database(logger, db_path)

        # Limpiar caché antes de comenzar
        clear_cache()

        # Cargar progreso anterior
        progress_data = load_progress(PROGRESS_FILE, {})

        # Crear drivers para los workers
        driver_odd = create_driver()  # Para Worker 1 (páginas impares)
        driver_even = create_driver()  # Para Worker 4 (páginas pares)
        driver_worker2 = create_driver()  # Para Worker 2 (procesa series de páginas impares)
        driver_worker5 = create_driver()  # Para Worker 5 (procesa series de páginas pares)

        # Iniciar sesión con todos los drivers
        if not login(driver_odd, logger):
            logger.error("No se pudo iniciar sesión con driver_odd. Abortando procesamiento de series.")
            driver_odd.quit()
            driver_even.quit()
            driver_worker2.quit()
            driver_worker5.quit()
            return

        if not login(driver_even, logger):
            logger.error("No se pudo iniciar sesión con driver_even. Abortando procesamiento de series.")
            driver_odd.quit()
            driver_even.quit()
            driver_worker2.quit()
            driver_worker5.quit()
            return

        if not login(driver_worker2, logger):
            logger.error("No se pudo iniciar sesión con driver_worker2. Abortando procesamiento de series.")
            driver_odd.quit()
            driver_even.quit()
            driver_worker2.quit()
            driver_worker5.quit()
            return

        if not login(driver_worker5, logger):
            logger.error("No se pudo iniciar sesión con driver_worker5. Abortando procesamiento de series.")
            driver_odd.quit()
            driver_even.quit()
            driver_worker2.quit()
            driver_worker5.quit()
            return

        # Iniciar todos los workers en paralelo
        threads = []

        # Iniciar Worker 3 (Procesador de BD para páginas impares)
        thread = threading.Thread(
            target=worker3_db_processor,
            args=(db_path, progress_data, 1),
            name="Worker3-1"
        )
        threads.append(thread)
        thread.start()

        # Iniciar Worker 6 (Procesador de BD para páginas pares)
        thread = threading.Thread(
            target=worker6_db_processor,
            args=(db_path, progress_data, 1),
            name="Worker6-1"
        )
        threads.append(thread)
        thread.start()

        # Iniciar Worker 2 (Extractor de datos de series de páginas impares)
        thread = threading.Thread(
            target=worker2_series_extractor,
            args=(driver_worker2, db_path, 1),
            name="Worker2-1"
        )
        threads.append(thread)
        thread.start()

        # Iniciar Worker 5 (Extractor de datos de series de páginas pares)
        thread = threading.Thread(
            target=worker5_series_extractor,
            args=(driver_worker5, db_path, 1),
            name="Worker5-1"
        )
        threads.append(thread)
        thread.start()

        # Iniciar Worker 1 (Extractor de URLs de páginas impares)
        thread = threading.Thread(
            target=worker1_url_extractor,
            args=(driver_odd, progress_data, start_page, max_pages),
            name="Worker1"
        )
        threads.append(thread)
        thread.start()

        # Iniciar Worker 4 (Extractor de URLs de páginas pares)
        thread = threading.Thread(
            target=worker4_url_extractor,
            args=(driver_even, progress_data, start_page + 1, max_pages),
            name="Worker4"
        )
        threads.append(thread)
        thread.start()

        # Esperar a que todos los workers terminen
        for thread in threads:
            thread.join()

        # Cerrar todos los drivers
        driver_odd.quit()
        driver_even.quit()
        driver_worker2.quit()
        driver_worker5.quit()

        # Guardar progreso final
        save_progress(PROGRESS_FILE, progress_data)

        # Generar informe
        report = generate_update_report(start_time)
        logger.info(report)

        logger.info(f"Procesamiento de series completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        logger.critical(f"Error crítico en el procesamiento de series: {e}")
        logger.debug(traceback.format_exc())
        stop_event.set()  # Señalizar a todos los workers que deben detenerse

    finally:
        # Asegurarse de que todos los drivers se cierren
        try:
            if 'driver_odd' in locals() and driver_odd:
                driver_odd.quit()
            if 'driver_even' in locals() and driver_even:
                driver_even.quit()
            if 'driver_worker2' in locals() and driver_worker2:
                driver_worker2.quit()
            if 'driver_worker5' in locals() and driver_worker5:
                driver_worker5.quit()
        except:
            pass


# Punto de entrada principal
if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Procesamiento de series por rating IMDB')
    parser.add_argument('--start-page', type=int, default=1, help='Página inicial para comenzar el procesamiento')
    parser.add_argument('--max-pages', type=int, help='Número máximo de páginas a procesar')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')
    parser.add_argument('--reset-progress', action='store_true',
                        help='Reiniciar el progreso (procesar todas las series)')

    args = parser.parse_args()

    # Actualizar configuración de paralelización si se especifica
    max_workers = args.max_workers if args.max_workers else MAX_WORKERS

    # Reiniciar progreso si se solicita
    if args.reset_progress and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        logger.info("Progreso reiniciado. Se procesarán todas las series.")

    # Ejecutar el procesamiento de series
    process_all_series(args.start_page, args.max_pages, args.db_path, max_workers)