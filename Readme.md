# HDFull Scrapers

## Overview (English)
HDFull Scrapers is a command-line toolkit designed to collect metadata about movies and TV series from the HDFull website. It automates browsing the catalog, extracting details for each title, and synchronising the collected information with local SQLite databases. The application is organised around interactive menus that let you configure scraping options, update existing records, or maintain the databases used by the download managers.

### Key features
- Guided menus for launching individual scrapers or running batch jobs.
- Separate workflows for direct-download links and torrent releases.
- Database utilities to create, migrate, or relocate the direct and torrent SQLite files.
- Logging to monitor scraping progress and diagnose connectivity issues.
- Command-line flags to jump directly into common tasks without navigating the full menu.

### How it works
1. Launch the application with `python main.py` (or via the provided helper scripts).
2. Choose a scraper or database action from the interactive menu.
3. The selected scraper browses HDFull, parses the result pages, and stores structured data in the configured database.
4. Progress and errors are reported in the console and in the `logs/` directory.

### Project layout
- `main.py`: Entry point that renders the menus and handles command-line arguments.
- `Scripts/`: Helper modules and individual scrapers.
- `resources/`: Static assets used by the scrapers.
- `logs/`: Output directory for execution logs (created at runtime).
- `run.bat`: Windows helper to bootstrap a virtual environment and launch the app.

### Quick start
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

To skip the main menu and open the scraping shortcuts directly, run:

```bash
python main.py --scrapers-menu
```

## Resumen (Español)
HDFull Scrapers es un conjunto de herramientas de línea de comandos que recopila metadatos de películas y series desde el sitio web HDFull. Automatiza la navegación por el catálogo, extrae la información de cada título y sincroniza los datos con bases de datos SQLite locales. La aplicación se organiza en menús interactivos que permiten configurar los scrapers, actualizar registros existentes o mantener las bases de datos utilizadas por los gestores de descargas.

### Características principales
- Menús guiados para ejecutar scrapers individuales o trabajos por lotes.
- Flujos independientes para enlaces de descarga directa y lanzamientos torrent.
- Utilidades de base de datos para crear, migrar o cambiar la ubicación de los archivos SQLite de directos y torrents.
- Registro de actividad para supervisar el progreso y diagnosticar problemas de conectividad.
- Parámetros por consola para acceder rápidamente a tareas comunes sin recorrer todo el menú.

### Funcionamiento
1. Inicia la aplicación con `python main.py` (o mediante los scripts auxiliares incluidos).
2. Elige un scraper o una acción de base de datos desde el menú interactivo.
3. El scraper seleccionado recorre HDFull, interpreta las páginas de resultados y guarda los datos estructurados en la base configurada.
4. El progreso y los errores se muestran en la consola y se almacenan en el directorio `logs/`.

### Estructura del proyecto
- `main.py`: Punto de entrada que muestra los menús y gestiona los argumentos de línea de comandos.
- `Scripts/`: Módulos auxiliares y scrapers individuales.
- `resources/`: Archivos estáticos utilizados por los scrapers.
- `logs/`: Carpeta creada en tiempo de ejecución para los registros.
- `run.bat`: Script de Windows que prepara el entorno virtual y ejecuta la aplicación.

### Inicio rápido
```bash
python -m venv venv
source venv/bin/activate  # En Windows usa: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Para acceder directamente a los atajos de scraping sin pasar por el menú principal, ejecuta:

```bash
python main.py --scrapers-menu
```
