import time
import re
import concurrent.futures
import argparse
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

# Importar utilidades compartidas
from scraper_utils import (
    setup_logger, create_driver, connect_db, login, setup_database,
    save_progress, load_progress, extract_links,
    insert_links_batch, has_next_page, clear_cache, movie_exists,
    insert_or_update_movie, BASE_URL, MAX_WORKERS, MAX_RETRIES
)

from main import PROJECT_ROOT

# Configuración específica para este script
SCRIPT_NAME = "update_movies_updated"
LOG_FILE = f"{SCRIPT_NAME}.log"
PROGRESS_FILE = os.path.join(PROJECT_ROOT, "progress", f"{SCRIPT_NAME}_progress.json")
UPDATED_MOVIES_URL = f"{BASE_URL}/peliculas-actualizadas"

# Configurar logger
logger = setup_logger(SCRIPT_NAME, LOG_FILE)

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
                movie_url = BASE_URL + movie_href
                movie_urls.append(movie_url)

        return movie_urls
    except Exception as e:
        logger.error(f"Error al obtener URLs de películas de la página {page_url}: {e}")
        return []

# Función para extraer detalles de la película
def extract_movie_details(movie_url, worker_id=0, db_path=None):
    logger.info(f"[Worker {worker_id}] Extrayendo detalles de la película actualizada: {movie_url}")

    # Crear un nuevo driver para este worker
    driver = create_driver()

    try:
        # Iniciar sesión con este driver
        if not login(driver, logger):
            logger.error(f"[Worker {worker_id}] No se pudo iniciar sesión. Abortando extracción de {movie_url}")
            driver.quit()
            return None

        driver.get(movie_url)
        time.sleep(1.5)  # Reducido para optimizar
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Extraer el título de la película
        title_tag = soup.find("div", id="summary-title")
        if not title_tag:
            logger.error(f"[Worker {worker_id}] No se pudo encontrar el título de la película en {movie_url}")
            driver.quit()
            return None

        title = title_tag.text.strip()
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
            if imdb_rating_tag and imdb_rating_tag.find("a"):
                rating_text = imdb_rating_tag.find("a").text.strip()
                try:
                    imdb_rating = float(rating_text)
                    logger.debug(f"[Worker {worker_id}] IMDB Rating extraído: {imdb_rating}")
                except ValueError:
                    logger.warning(f"[Worker {worker_id}] No se pudo convertir el rating a float: {rating_text}")

            genre_tags = show_details.find_all("a", href=re.compile(r"/tags-peliculas"))
            genre = ", ".join([tag.text.strip() for tag in genre_tags]) if genre_tags else None
            logger.debug(f"[Worker {worker_id}] Género extraído: {genre}")

        # Crear una conexión a la base de datos para reutilizarla
        connection = connect_db(db_path)

        try:
            # Verificar si la película ya existe en la base de datos
            exists, movie_id = movie_exists(title, year, imdb_rating, genre, connection, db_path)

            # Si la película existe exactamente igual, solo extraemos los enlaces
            if exists:
                logger.info(
                    f"[Worker {worker_id}] La película '{title}' ({year}) ya existe en la base de datos con ID {movie_id}.")
                is_new_movie = False
            else:
                # Si no existe o hay diferencias, insertamos/actualizamos la película
                movie_id, is_new_movie = insert_or_update_movie({
                    "title": title,
                    "year": year,
                    "imdb_rating": imdb_rating,
                    "genre": genre,
                    "type": "movie",
                    "existing_id": movie_id  # Puede ser None si no existe
                }, connection, db_path)

                if not movie_id:
                    logger.error(f"[Worker {worker_id}] Error al insertar/actualizar la película: {title}")
                    driver.quit()
                    connection.close()
                    return None

            # Extraer enlaces usando la función compartida
            server_links = extract_links(driver, movie_id=movie_id, logger=logger)

            # Insertar los enlaces en la base de datos en lote
            new_links_count = 0
            if server_links:
                new_links_count = insert_links_batch(server_links, logger, connection, db_path)
                logger.info(f"[Worker {worker_id}] Se insertaron {new_links_count} nuevos enlaces para la película")
            else:
                logger.warning(f"[Worker {worker_id}] No se encontraron enlaces para la película")

            # Cerrar la conexión a la base de datos
            connection.close()

            # Cerrar el driver
            driver.quit()

            return {
                "id": movie_id,
                "title": title,
                "year": year,
                "imdb_rating": imdb_rating,
                "genre": genre,
                "links": server_links,
                "new_links_count": new_links_count,
                "is_new_movie": is_new_movie,
                "url": movie_url
            }
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error al procesar la película: {e}")
            connection.close()
            raise
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Error al extraer detalles de la película {movie_url}: {e}")
        if driver:
            driver.quit()
        raise

# Función para procesar una película con reintentos
def process_movie_with_retries(movie_url, worker_id, db_path=None):
    for attempt in range(MAX_RETRIES):
        try:
            return extract_movie_details(movie_url, worker_id, db_path)
        except Exception as e:
            logger.error(
                f"[Worker {worker_id}] Error al procesar la película {movie_url} (intento {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(30)  # Esperar 30 segundos antes de reintentar

    logger.error(f"[Worker {worker_id}] No se pudo procesar la película {movie_url} después de {MAX_RETRIES} intentos")
    return None

# Función para procesar películas en paralelo
def process_movies_in_parallel(movie_urls, db_path=None):
    logger.info(f"Procesando {len(movie_urls)} películas en paralelo con {MAX_WORKERS} workers")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Crear un diccionario de futuros/tareas
        future_to_url = {
            executor.submit(process_movie_with_retries, url, i % MAX_WORKERS, db_path): url
            for i, url in enumerate(movie_urls)
        }

        # Procesar los resultados a medida que se completan
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                movie_data = future.result()
                if movie_data:
                    results.append(movie_data)
                    logger.info(f"Película actualizada procesada correctamente: {url}")
            except Exception as e:
                logger.error(f"Error al procesar la película actualizada {url}: {e}")

    return results

# Función para generar informe de actualización
def generate_update_report(start_time, processed_movies):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos

    # Contar películas nuevas y enlaces nuevos
    new_movies_count = sum(1 for movie in processed_movies if movie.get("is_new_movie", False))
    new_links_count = sum(movie.get("new_links_count", 0) for movie in processed_movies)

    # Generar informe
    report = f"""
INFORME DE ACTUALIZACIÓN DE PELÍCULAS ACTUALIZADAS - {end_time.strftime('%Y-%m-%d %H:%M:%S')}
===========================================================================

Duración: {duration:.2f} minutos

RESUMEN:
- Películas procesadas: {len(processed_movies)}
- Nuevas películas añadidas: {new_movies_count}
- Total de nuevos enlaces: {new_links_count}

DETALLES:
"""

    # Ordenar películas por título
    sorted_movies = sorted(processed_movies, key=lambda x: x.get("title", ""))

    # Añadir detalles de cada película
    for movie in sorted_movies:
        status = "NUEVA PELÍCULA" if movie.get("is_new_movie", False) else "ACTUALIZADA"
        report += f"- {movie.get('title', 'Sin título')} ({movie.get('year', 'N/A')}) - {movie.get('new_links_count', 0)} nuevos enlaces ({status})\\n"

    report += """
===========================================================================
Este es un mensaje automático generado por el sistema de actualización de películas.
"""

    return report

# Función para registrar estadísticas de actualización
def log_update_stats(start_time, processed_movies, db_path=None):
    try:
        connection = connect_db(db_path)
        cursor = connection.cursor()

        # Obtener estadísticas de la actualización actual
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # en minutos

        # Contar películas nuevas y enlaces nuevos
        new_movies_count = sum(1 for movie in processed_movies if movie.get("is_new_movie", False))
        updated_movies_count = len(processed_movies) - new_movies_count
        new_links_count = sum(movie.get("new_links_count", 0) for movie in processed_movies)

        # Verificar si ya existe una entrada para hoy
        cursor.execute('''
            SELECT * FROM update_stats WHERE update_date = date('now')
        ''')
        existing_stats = cursor.fetchone()

        if existing_stats:
            # Actualizar estadísticas existentes
            cursor.execute('''
                UPDATE update_stats 
                SET duration_minutes = duration_minutes + ?,
                    updated_movies = updated_movies + ?,
                    new_links = new_links + ?
                WHERE update_date = date('now')
            ''', (duration, new_movies_count + updated_movies_count, new_links_count))
        else:
            # Insertar nuevas estadísticas
            cursor.execute('''
                INSERT INTO update_stats 
                (update_date, duration_minutes, updated_movies, new_links)
                VALUES (date('now'), ?, ?, ?)
            ''', (duration, new_movies_count + updated_movies_count, new_links_count))

        connection.commit()

        logger.info(
            f"Estadísticas de actualización: Duración={duration:.2f} minutos, Películas nuevas={new_movies_count}, "
            f"Películas actualizadas={updated_movies_count}, Nuevos enlaces={new_links_count}")
        return {
            "duration": duration,
            "new_movies": new_movies_count,
            "updated_movies": updated_movies_count,
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

# Función principal para procesar películas actualizadas
def process_updated_movies(max_pages=None, db_path=None):
    start_time = datetime.now()
    logger.info(f"Iniciando procesamiento de películas actualizadas: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Configurar la base de datos
        setup_database(logger, db_path)

        # Limpiar caché antes de comenzar
        clear_cache()

        # Crear un driver principal para navegar por las páginas
        main_driver = create_driver()
        if not login(main_driver, logger):
            logger.error("No se pudo iniciar sesión. Abortando procesamiento de películas actualizadas.")
            main_driver.quit()
            return []

        # Cargar progreso anterior
        progress_data = load_progress(PROGRESS_FILE, {"page_number": 1, "processed_urls": []})
        page_number = progress_data.get("page_number", 1)
        processed_urls = progress_data.get("processed_urls", [])

        all_processed_movies = []

        processed_pages = 0
        while True:
            if max_pages and processed_pages >= max_pages:
                logger.info(f"Se ha alcanzado el límite de {max_pages} páginas para películas actualizadas.")
                break

            try:
                page_url = f"{UPDATED_MOVIES_URL}/{page_number}" if page_number > 1 else UPDATED_MOVIES_URL

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
                    processed_movies = process_movies_in_parallel(new_urls, db_path)
                    all_processed_movies.extend(processed_movies)

                    # Actualizar la lista de URLs procesadas
                    processed_urls.extend(new_urls)

                    # Guardar progreso
                    save_progress(PROGRESS_FILE, {
                        'page_number': page_number,
                        'processed_urls': processed_urls,
                        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

                # Avanzar a la siguiente página
                page_number += 1
                processed_pages += 1

                # Pequeña pausa entre páginas para no sobrecargar el servidor
                time.sleep(3)
            except Exception as e:
                logger.error(f"Error al procesar la página {page_number} de películas actualizadas: {e}")
                time.sleep(60)  # Esperar 1 minuto antes de intentar nuevamente

        main_driver.quit()

        # Registrar estadísticas
        stats = log_update_stats(start_time, all_processed_movies, db_path)

        logger.info(f"Procesamiento de películas actualizadas completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return all_processed_movies
    except Exception as e:
        logger.critical(f"Error crítico en el procesamiento de películas actualizadas: {e}")
        return []

# Punto de entrada principal
if __name__ == "__main__":
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Actualización de películas actualizadas')
    parser.add_argument('--max-pages', type=int, help='Número máximo de páginas a procesar')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')

    args = parser.parse_args()

    # Actualizar configuración de paralelización si se especifica
    if args.max_workers:
        MAX_WORKERS = args.max_workers

    # Ejecutar la actualización de películas
    process_updated_movies(args.max_pages, args.db_path)