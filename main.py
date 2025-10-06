"""Punto de entrada principal de FilmScraper.

Este módulo delega toda la funcionalidad de la aplicación en la
interfaz gráfica implementada en :mod:`gui`. Se mantiene en un archivo
separado para preservar la compatibilidad con scripts externos o
empaquetadores que invoquen ``python main.py`` como punto de entrada.
"""

from __future__ import annotations

from Scripts.scraper_utils import setup_logger
from gui import run_gui

# Importaciones necesarias para que PyInstaller incluya todos los scrapers
import Scripts.direct_dw_films_scraper  # noqa: F401
import Scripts.direct_dw_series_scraper  # noqa: F401
import Scripts.update_movies_premiere  # noqa: F401
import Scripts.update_movies_updated  # noqa: F401
import Scripts.update_episodes_premiere  # noqa: F401
import Scripts.update_episodes_updated  # noqa: F401
import Scripts.torrent_dw_films_scraper  # noqa: F401
import Scripts.torrent_dw_series_scraper  # noqa: F401


logger = setup_logger("main", "main.log")


def main() -> int:
    """Inicia la interfaz gráfica y devuelve el código de salida."""

    logger.info("Iniciando interfaz gráfica de FilmScraper…")
    return run_gui()


if __name__ == "__main__":  # pragma: no cover - punto de entrada manual
    raise SystemExit(main())
