import os
import argparse
import traceback
from datetime import datetime

from scraper_utils import (
    setup_logger, create_driver, login, setup_database,
    save_progress, load_progress, clear_cache,
    BASE_URL, PROJECT_ROOT
)

# Reutilizamos funciones del script de películas actualizadas
import update_movies_updated as movies_updated

SCRIPT_NAME = "update_movies_premiere"
LOG_FILE = f"{SCRIPT_NAME}.log"
PROGRESS_FILE = os.path.join(PROJECT_ROOT, "progress", f"{SCRIPT_NAME}_progress.json")
PREMIERE_MOVIES_URL = f"{BASE_URL}/peliculas-estreno"

# Configurar logger propio y reemplazar el del módulo reutilizado
logger = setup_logger(SCRIPT_NAME, LOG_FILE)
movies_updated.logger = logger

def process_premiere_movies(db_path=None):
    """Procesa las películas de estreno obteniendo enlaces y actualizando la BD."""
    start_time = datetime.now()
    logger.info(
        f"Iniciando procesamiento de películas de estreno: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        setup_database(logger, db_path)
        clear_cache()

        main_driver = create_driver()
        if not login(main_driver, logger):
            logger.error("No se pudo iniciar sesión. Abortando procesamiento de películas de estreno.")
            main_driver.quit()
            return []

        processed_urls = load_progress(PROGRESS_FILE, {}).get('processed_urls', [])
        movie_urls = movies_updated.get_movie_urls_from_page(PREMIERE_MOVIES_URL, main_driver)
        main_driver.quit()

        if not movie_urls:
            logger.warning("No se encontraron películas de estreno. Finalizando.")
            return []

        new_urls = [url for url in movie_urls if url not in processed_urls]
        logger.info(f"Encontradas {len(new_urls)} películas nuevas para procesar")
        if not new_urls:
            logger.info("No hay películas nuevas para procesar. Finalizando.")
            return []

        processed_movies = movies_updated.process_movies_in_parallel(new_urls, db_path)
        processed_urls.extend(new_urls)

        save_progress(PROGRESS_FILE, {
            'processed_urls': processed_urls,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        movies_updated.log_update_stats(start_time, processed_movies, db_path)
        report = movies_updated.generate_update_report(start_time, processed_movies)
        logger.info(report)

        logger.info(
            f"Procesamiento de películas de estreno completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return processed_movies
    except Exception as e:
        logger.critical(f"Error crítico en el procesamiento de películas de estreno: {e}")
        logger.debug(traceback.format_exc())
        return []
    finally:
        movies_updated.close_all_drivers()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Actualización de películas de estreno')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')
    parser.add_argument('--reset-progress', action='store_true',
                        help='Reiniciar el progreso (procesar todas las películas)')

    args = parser.parse_args()

    if args.max_workers:
        movies_updated.MAX_WORKERS = args.max_workers

    if args.reset_progress and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        logger.info("Progreso reiniciado. Se procesarán todas las películas.")

    process_premiere_movies(args.db_path)
