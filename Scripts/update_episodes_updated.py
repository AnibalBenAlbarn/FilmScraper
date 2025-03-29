import time
import re
import concurrent.futures
import argparse
import os
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
    insert_episode, BASE_URL, MAX_WORKERS, MAX_RETRIES
)

from main import PROJECT_ROOT

# Configuración específica para este script
SCRIPT_NAME = "update_episodes_updated"
LOG_FILE = f"{SCRIPT_NAME}.log"
PROGRESS_FILE = os.path.join(PROJECT_ROOT, "progress", f"{SCRIPT_NAME}_progress.json")
UPDATED_EPISODES_URL = f"{BASE_URL}/episodios#updated"

# Configurar logger
logger = setup_logger(SCRIPT_NAME, LOG_FILE)

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

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Buscar todos los divs de los episodios
        episode_divs = soup.find_all("div", class_="span-6 tt view show-view")
        logger.info(f"Encontrados {len(episode_divs)} episodios actualizados")

        episode_urls = []
        for episode_div in episode_divs:
            link_tag = episode_div.find("a", href=re.compile(r"/episodio/"))
            if link_tag:
                episode_href = link_tag['href']
                episode_url = BASE_URL + episode_href
                episode_urls.append(episode_url)

        return episode_urls
    except Exception as e:
        logger.error(f"Error al obtener URLs de episodios actualizados: {e}")
        return []

# Función para extraer detalles del episodio
def extract_episode_details(episode_url, worker_id=0, db_path=None):
    logger.info(f"[Worker {worker_id}] Extrayendo detalles del episodio actualizado: {episode_url}")

    # Crear un nuevo driver para este worker
    driver = create_driver()

    try:
        # Iniciar sesión con este driver
        if not login(driver, logger):
            logger.error(f"[Worker {worker_id}] No se pudo iniciar sesión. Abortando extracción de {episode_url}")
            driver.quit()
            return None

        driver.get(episode_url)
        time.sleep(1.5)  # Reducido para optimizar
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
        series_url = BASE_URL + series_title_tag['href']

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
        imdb_rating = None
        genre = None

        try:
            driver.get(series_url)
            time.sleep(1.5)  # Reducido para optimizar
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
                if imdb_rating_tag and imdb_rating_tag.find("a"):
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
                    driver.quit()
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
                    driver.quit()
                    connection.close()
                    return None
                is_new_season = True
            else:
                logger.info(f"[Worker {worker_id}] Temporada {season_number} encontrada con ID {season_id}")
                is_new_season = False

            # Verificar si el episodio existe
            episode_exists_flag, episode_id = episode_exists(season_id, episode_number, episode_title, connection, db_path)

            # Si el episodio no existe, lo insertamos
            if not episode_exists_flag:
                logger.info(f"[Worker {worker_id}] Episodio {episode_number} no encontrado. Insertando nuevo episodio.")
                episode_id = insert_episode(season_id, episode_number, episode_title, connection, db_path)
                if not episode_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar el episodio. Abortando.")
                    driver.quit()
                    connection.close()
                    return None
                is_new_episode = True
            else:
                logger.info(f"[Worker {worker_id}] Episodio {episode_number} encontrado con ID {episode_id}")
                is_new_episode = False

            # Volver a la página del episodio para extraer los enlaces
            driver.get(episode_url)
            time.sleep(1.5)  # Reducido para optimizar

            # Extraer enlaces usando la función compartida
            server_links = extract_links(driver, episode_id=episode_id, logger=logger)

            # Insertar los enlaces en la base de datos en lote
            new_links_count = 0
            if server_links:
                new_links_count = insert_links_batch(server_links, logger, connection, db_path)
                logger.info(f"[Worker {worker_id}] Se insertaron {new_links_count} nuevos enlaces para el episodio")
            else:
                logger.warning(f"[Worker {worker_id}] No se encontraron enlaces para el episodio")

            # Cerrar la conexión a la base de datos
            connection.close()

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
                "new_links_count": new_links_count,
                "is_new_series": is_new_series,
                "is_new_season": is_new_season,
                "is_new_episode": is_new_episode
            }
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error al procesar el episodio: {e}")
            connection.close()
            raise
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles del episodio {episode_url}: {e}")
        if driver:
            driver.quit()
        raise

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

        logger.info(
            f"Actualización de episodios actualizados completada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return processed_episodes
    except Exception as e:
        logger.critical(f"Error crítico en la actualización de episodios actualizados: {e}")
        return []

# Punto de entrada principal
if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Actualización de episodios actualizados')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')

    args = parser.parse_args()

    # Actualizar configuración de paralelización si se especifica
    if args.max_workers:
        MAX_WORKERS = args.max_workers

    # Ejecutar la actualización de episodios
    process_updated_episodes(args.db_path)