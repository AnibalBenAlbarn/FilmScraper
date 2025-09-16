"""Compatibilidad para lanzar el menú de scrapers desde el módulo original."""

from main import run_scrapers_menu


def main():
    """Punto de entrada manteniendo compatibilidad con versiones anteriores."""
    run_scrapers_menu()


if __name__ == "__main__":
    main()
