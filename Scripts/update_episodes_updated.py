import time
import re
import concurrent.futures
import argparse
import os
import traceback
import threading
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Importar utilidades compartidas
from scraper_utils import (
    setup_logger, create_driver, connect_db, login, setup_database,
    save_progress, load_progress, extract_links,
    insert_links_batch, clear_cache, find_series_by_title_year,
    season_exists, episode_exists, insert_series, insert_season,
    insert_episode, BASE_URL, MAX_WORKERS, MAX_RETRIES, PROJECT_ROOT
)

# Configuración específica para este script
SCRIPT_NAME = "update_episodes_updated"
LOG_FILE = f"{SCRIPT_NAME}.log"
PROGRESS_FILE = os.path.join(PROJECT_ROOT, "progress", f"{SCRIPT_NAME}_progress.json")
UPDATED_EPISODES_URL = f"{BASE_URL}/episodios#updated"

# Configurar logger
logger = setup_logger(SCRIPT_NAME, LOG_FILE)


# Manejador de drivers por hilo para evitar múltiples inicios de sesión
_thread_local = threading.local()
_active_drivers = []


def get_logged_in_driver():
    """Obtiene un driver asociado al hilo actual ya autenticado."""
    if not hasattr(_thread_local, "driver"):
        driver = create_driver()
        if not login(driver, logger):
            logger.error("No se pudo iniciar sesión en el driver")
            driver.quit()
            raise RuntimeError("Login failed")
        _thread_local.driver = driver
        _active_drivers.append(driver)
    return _thread_local.driver


def close_all_drivers():
    for driver in _active_drivers:
        try:
            driver.quit()
        except Exception:
            pass
    _active_drivers.clear()


# Función para obtener URLs de episodios de la página de actualizados
def get_episode_urls_from_updated_page(driver):
    logger.info("Obteniendo URLs de episodios actualizados...")
    try:
        driver.get(UPDATED_EPISODES_URL)
        time.sleep(3)  # Esperar a que se cargue la página y el contenido dinámico

        # Hacer clic en la pestaña "Actualizados" si es necesario
        try:
            updated_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Actualizados')]"))
            )
            updated_tab.click()
            time.sleep(2)  # Esperar a que se cargue el contenido
        except Exception as e:
            logger.warning(f"No se pudo hacer clic en la pestaña 'Actualizados': {e}")
            # Continuamos de todos modos, ya que podríamos estar ya en la pestaña correcta

        # Esperar a que aparezca el contenedor de episodios
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "episodes-content"))
            )
        except Exception as e:
            logger.error(f"Timeout esperando el contenedor de episodios: {e}")
            return []

        # Obtener todos los episodios con scroll infinito
        episode_urls = []
        last_count = 0
        no_new_results_count = 0
        max_no_new_results = 5  # Número máximo de intentos sin nuevos resultados antes de parar

        # Conjunto para evitar duplicados
        seen_urls = set()

        # Bucle para hacer scroll hasta que no haya más episodios
        while True:
            # Obtener los episodios actuales
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            episode_divs = soup.find_all("div", class_="span-6 tt view show-view")

            # Procesar los episodios visibles actualmente
            for episode_div in episode_divs:
                link_tag = episode_div.find("a", href=re.compile(r"/episodio/"))
                if link_tag:
                    episode_href = link_tag['href']
                    episode_url = BASE_URL + episode_href if not episode_href.startswith('http') else episode_href

                    # Añadir solo si no lo hemos visto antes
                    if episode_url not in seen_urls:
                        episode_urls.append(episode_url)
                        seen_urls.add(episode_url)

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
            logger.debug(f"Scroll realizado: {len(episode_urls)} episodios encontrados")

            # Esperar a que se carguen más contenidos
            time.sleep(2)

        logger.info(f"Se encontraron {len(episode_urls)} episodios actualizados")
        return episode_urls
    except Exception as e:
        logger.error(f"Error al obtener URLs de episodios actualizados: {e}")
        logger.debug(traceback.format_exc())
        return []


# Función para extraer detalles del episodio
def extract_episode_details(episode_url, worker_id=0, db_path=None):
    logger.info(f"[Worker {worker_id}] Extrayendo detalles del episodio actualizado: {episode_url}")
    driver = get_logged_in_driver()

    try:
        driver.get(episode_url)
        time.sleep(1.5)  # Reducido para optimizar

        # Esperar a que aparezca la información del episodio
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "show-details"))
            )
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Timeout esperando información del episodio en {episode_url}: {e}")
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

        if show_details:
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
                series_id = insert_series(series_title, series_year, imdb_rating, genre, connection, db_path)
                if not series_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar la serie. Abortando.")
                    connection.close()
                    return None
                is_new_series = True
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
                season_id = insert_season(series_id, season_number, connection, db_path)
                if not season_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar la temporada. Abortando.")
                    connection.close()
                    return None
                is_new_season = True
            else:
                logger.info(f"[Worker {worker_id}] Temporada {season_number} encontrada con ID {season_id}")
                is_new_season = False

            # Verificar si el episodio existe
            episode_exists_flag, episode_id = episode_exists(season_id, episode_number, episode_title, connection,
                                                             db_path)

            # Si el episodio no existe, lo insertamos
            if not episode_exists_flag:
                logger.info(f"[Worker {worker_id}] Episodio {episode_number} no encontrado. Insertando nuevo episodio.")
                episode_id = insert_episode(season_id, episode_number, episode_title, connection, db_path)
                if not episode_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar el episodio. Abortando.")
                    connection.close()
                    return None
                is_new_episode = True
            else:
                logger.info(f"[Worker {worker_id}] Episodio {episode_number} encontrado con ID {episode_id}")
                is_new_episode = False

            # Extraer enlaces usando la función mejorada
            server_links = extract_episode_links(driver, episode_id)

            # Insertar los enlaces en la base de datos en lote
            new_links_count = 0
            if server_links:
                new_links_count = insert_links_batch(server_links, logger, connection, db_path)
                logger.info(f"[Worker {worker_id}] Se insertaron {new_links_count} nuevos enlaces para el episodio")
            else:
                logger.warning(f"[Worker {worker_id}] No se encontraron enlaces para el episodio")

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
                "new_links_count": new_links_count,
                "is_new_series": is_new_series,
                "is_new_season": is_new_season,
                "is_new_episode": is_new_episode
            }
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error al procesar el episodio: {e}")
            logger.debug(traceback.format_exc())
            connection.close()
            return None
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles del episodio {episode_url}: {e}")
        logger.debug(traceback.format_exc())
        return None


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


# Función para procesar un episodio con reintentos
def process_episode_with_retries(episode_url, worker_id, db_path=None):
    for attempt in range(MAX_RETRIES):
        try:
            return extract_episode_details(episode_url, worker_id, db_path)
        except Exception as e:
            logger.error(
                f"[Worker {worker_id}] Error al procesar el episodio {episode_url} (intento {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(30)  # Esperar 30 segundos antes de reintentar

    logger.error(
        f"[Worker {worker_id}] No se pudo procesar el episodio {episode_url} después de {MAX_RETRIES} intentos")
    return None


# Función para procesar episodios en paralelo
def process_episodes_in_parallel(episode_urls, db_path=None):
    logger.info(f"Procesando {len(episode_urls)} episodios actualizados en paralelo con {MAX_WORKERS} workers")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Crear un diccionario de futuros/tareas
        future_to_url = {
            executor.submit(process_episode_with_retries, url, i % MAX_WORKERS, db_path): url
            for i, url in enumerate(episode_urls)
        }

        # Procesar los resultados a medida que se completan
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                episode_data = future.result()
                if episode_data:
                    results.append(episode_data)
                    logger.info(f"Episodio actualizado procesado correctamente: {url}")
            except Exception as e:
                logger.error(f"Error al procesar el episodio actualizado {url}: {e}")
                logger.debug(traceback.format_exc())

    close_all_drivers()
    return results


# Función para generar informe de actualización
def generate_update_report(start_time, processed_episodes):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos

    # Contar episodios nuevos, temporadas nuevas y series nuevas
    new_episodes_count = sum(1 for ep in processed_episodes if ep.get("is_new_episode", False))
    new_seasons_count = sum(1 for ep in processed_episodes if ep.get("is_new_season", False))
    new_series_count = sum(1 for ep in processed_episodes if ep.get("is_new_series", False))

    # Contar enlaces nuevos
    new_links_count = sum(ep.get("new_links_count", 0) for ep in processed_episodes)

    # Generar informe
    report = f"""
INFORME DE ACTUALIZACIÓN DE EPISODIOS ACTUALIZADOS - {end_time.strftime('%Y-%m-%d %H:%M:%S')}
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
def log_update_stats(start_time, processed_episodes, db_path=None):
    try:
        connection = connect_db(db_path)
        cursor = connection.cursor()

        # Obtener estadísticas de la actualización actual
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # en minutos

        # Contar episodios nuevos, temporadas nuevas y series nuevas
        new_episodes_count = sum(1 for ep in processed_episodes if ep.get("is_new_episode", False))
        new_seasons_count = sum(1 for ep in processed_episodes if ep.get("is_new_season", False))
        new_series_count = sum(1 for ep in processed_episodes if ep.get("is_new_series", False))

        # Contar enlaces nuevos
        new_links_count = sum(ep.get("new_links_count", 0) for ep in processed_episodes)

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


# Función principal para procesar episodios actualizados
def process_updated_episodes(db_path=None):
    start_time = datetime.now()
    logger.info(f"Iniciando actualización de episodios actualizados: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Configurar la base de datos
        setup_database(logger, db_path)

        # Limpiar caché antes de comenzar
        clear_cache()

        # Crear un driver principal para obtener las URLs de los episodios
        main_driver = create_driver()
        if not login(main_driver, logger):
            logger.error("No se pudo iniciar sesión. Abortando procesamiento de episodios actualizados.")
            main_driver.quit()
            return []

        # Obtener URLs de episodios actualizados
        episode_urls = get_episode_urls_from_updated_page(main_driver)
        main_driver.quit()

        if not episode_urls:
            logger.warning("No se encontraron episodios actualizados. Finalizando.")
            return []

        # Cargar progreso anterior
        processed_urls = load_progress(PROGRESS_FILE, {}).get('processed_urls', [])

        # Filtrar URLs ya procesadas
        new_urls = [url for url in episode_urls if url not in processed_urls]
        logger.info(f"Encontrados {len(new_urls)} episodios nuevos para procesar")

        if not new_urls:
            logger.info("No hay episodios nuevos para procesar. Finalizando.")
            return []

        # Procesar episodios en paralelo
        processed_episodes = process_episodes_in_parallel(new_urls, db_path)

        # Actualizar la lista de URLs procesadas
        processed_urls.extend(new_urls)

        # Guardar progreso
        save_progress(PROGRESS_FILE, {
            'processed_urls': processed_urls,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        # Registrar estadísticas
        stats = log_update_stats(start_time, processed_episodes, db_path)

        # Generar informe
        report = generate_update_report(start_time, processed_episodes)
        logger.info(report)

        logger.info(
            f"Actualización de episodios actualizados completada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return processed_episodes
    except Exception as e:
        logger.critical(f"Error crítico en la actualización de episodios actualizados: {e}")
        logger.debug(traceback.format_exc())
        return []


# Punto de entrada principal
if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Actualización de episodios actualizados')
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
    process_updated_episodes(args.db_path)