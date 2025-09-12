import os
import sys
import subprocess
import time
import argparse
from datetime import datetime

# Obtener el directorio raíz del proyecto
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configurar logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, "logs", "run_all.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Scripts directos (actualizaciones)
DIRECT_SCRIPTS = [
    {
        "name": "Películas de Estreno",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_movies_premiere.py"),
        "description": "Extrae información de películas de estreno",
    },
    {
        "name": "Películas Actualizadas",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_movies_updated.py"),
        "description": "Extrae información de películas actualizadas",
    },
    {
        "name": "Episodios de Estreno",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_episodes_premiere.py"),
        "description": "Extrae información de episodios de estreno",
    },
    {
        "name": "Episodios Actualizados",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "update_episodes_updated.py"),
        "description": "Extrae información de episodios actualizados",
    },
]

# Scripts torrent (se actualizan reejecutando)
TORRENT_SCRIPTS = [
    {
        "name": "Torrent Series",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "torrent_dw_series_scraper.py"),
        "description": "Extrae información de series torrent",
    },
    {
        "name": "Torrent Películas",
        "file": os.path.join(PROJECT_ROOT, "Scripts", "torrent_dw_films_scraper.py"),
        "description": "Extrae información de películas torrent",
    },
]


def run_all_scripts(scraper_type="direct", db_path=None, max_pages=None, max_workers=None):
    """Ejecuta todos los scripts en secuencia según el tipo seleccionado."""
    start_time = datetime.now()
    logger.info(
        f"Iniciando ejecución de scripts ({scraper_type}): {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    scripts = DIRECT_SCRIPTS if scraper_type == "direct" else TORRENT_SCRIPTS

    for script in scripts:
        script_name = script["name"]
        script_file = script["file"]

        if not os.path.exists(script_file):
            logger.error(f"El script {script_file} no existe. Saltando...")
            continue

        logger.info(f"Ejecutando {script_name}...")
        print(f"\\n{'=' * 80}")
        print(f"EJECUTANDO: {script_name}")
        print(f"{'=' * 80}")

        cmd = [sys.executable, script_file]

        # Los scripts de tipo torrent manejan su propia configuración
        if scraper_type == "direct":
            if db_path:
                cmd.extend(["--db-path", db_path])

            if "premiere" in script_file or "updated" in script_file:
                if "movies" in script_file and max_pages:
                    cmd.extend(["--max-pages", str(max_pages)])

            if max_workers:
                cmd.extend(["--max-workers", str(max_workers)])

        try:
            subprocess.run(cmd, check=True)
            logger.info(f"{script_name} ejecutado correctamente")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error al ejecutar {script_name}: {e}")
            print(f"\\nError al ejecutar {script_name}: {e}")
        except Exception as e:
            logger.error(f"Error inesperado al ejecutar {script_name}: {e}")
            print(f"\\nError inesperado al ejecutar {script_name}: {e}")

        # Pequeña pausa entre scripts
        time.sleep(5)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60  # en minutos
    logger.info(f"Ejecución de todos los scripts completada en {duration:.2f} minutos")
    print(f"\\n{'=' * 80}")
    print(f"EJECUCIÓN COMPLETADA EN {duration:.2f} MINUTOS")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ejecutar varios scrapers en secuencia')
    parser.add_argument('--scraper', choices=['direct', 'torrent'], default='direct',
                        help='Tipo de scrapers a ejecutar')
    parser.add_argument('--db-path', type=str, help='Ruta a la base de datos SQLite')
    parser.add_argument('--max-pages', type=int, help='Número máximo de páginas a procesar para películas')
    parser.add_argument('--max-workers', type=int, help='Número máximo de workers para procesamiento paralelo')

    args = parser.parse_args()

    run_all_scripts(args.scraper, args.db_path, args.max_pages, args.max_workers)