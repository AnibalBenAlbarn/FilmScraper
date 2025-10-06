# HDFull Scrapers

## Overview (English)
HDFull Scrapers now provides a full graphical interface to collect metadata about movies and TV series from the HDFull website. The GUI centralises the launch of scrapers, database maintenance tasks, and runtime monitoring in a single window that works across platforms.

### Key features
- Tabs for launching individual scrapers, running batch jobs, and reviewing progress.
- Separate workflows for direct-download links and torrent releases.
- Database utilities to create, migrate, or relocate the direct and torrent SQLite files.
- Real-time logging panel to monitor scraping progress and diagnose connectivity issues.
- Configuration controls to adjust worker counts, retry limits, and cache usage without leaving the interface.

### How it works
1. Launch the application with `python main.py` (or via the provided helper scripts).
2. Use the GUI tabs to choose a scraper, configure database paths, or tweak runtime settings.
3. The selected scraper runs in a background process while the log view and progress indicators update in real time.
4. Activity logs remain available in the `logs/` directory for later review.

### Project layout
- `main.py`: Entry point that launches the graphical interface.
- `gui.py`: Implementation of the PyQt6 application.
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

## Resumen (Español)
HDFull Scrapers ahora ofrece una interfaz gráfica completa para recopilar metadatos de películas y series desde el sitio web HDFull. La GUI concentra en una sola ventana el lanzamiento de los scrapers, el mantenimiento de bases de datos y la supervisión en tiempo real, con compatibilidad multiplataforma.

### Características principales
- Pestañas dedicadas para ejecutar scrapers, lanzar trabajos por lotes y revisar el progreso almacenado.
- Flujos independientes para enlaces de descarga directa y lanzamientos torrent.
- Utilidades de base de datos para crear, migrar o cambiar la ubicación de los archivos SQLite directos y torrent.
- Panel de registro en tiempo real para supervisar el progreso del scraping y diagnosticar incidencias.
- Controles de configuración para ajustar workers, reintentos y uso de caché sin salir de la interfaz.

### Funcionamiento
1. Inicia la aplicación con `python main.py` (o mediante los scripts auxiliares incluidos).
2. Utiliza las pestañas de la GUI para elegir un scraper, configurar rutas de bases de datos o ajustar parámetros de ejecución.
3. El scraper seleccionado se ejecuta en un proceso en segundo plano mientras la vista de registro y los indicadores de progreso se actualizan en tiempo real.
4. Los registros de actividad quedan disponibles en el directorio `logs/` para consultarlos más tarde.

### Estructura del proyecto
- `main.py`: Punto de entrada que inicia la interfaz gráfica.
- `gui.py`: Implementación de la aplicación en PyQt6.
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
