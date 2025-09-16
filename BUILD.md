# Build and Deployment Guide / Guía de Construcción

This document summarises the supported ways to run or package HDFull Scrapers on different platforms. Each section is provided in English and Spanish for convenience.

## 1. Windows helper script / Script auxiliar para Windows

### English
Use the bundled `run.bat` file to automate environment creation and dependency installation.

1. Double-click `run.bat` or execute it from **Command Prompt**.
2. The script creates (or reuses) a `venv` virtual environment inside the project folder.
3. Dependencies listed in `requirements.txt` are installed automatically.
4. The application starts with `python main.py`; any extra arguments are forwarded to the Python program.

Example:
```bat
run.bat --scrapers-menu
```

### Español
Utiliza el archivo incluido `run.bat` para automatizar la creación del entorno y la instalación de dependencias.

1. Haz doble clic en `run.bat` o ejecútalo desde **Command Prompt**.
2. El script crea (o reutiliza) un entorno virtual `venv` dentro de la carpeta del proyecto.
3. Las dependencias definidas en `requirements.txt` se instalan automáticamente.
4. La aplicación inicia con `python main.py`; cualquier argumento adicional se envía al programa en Python.

Ejemplo:
```bat
run.bat --scrapers-menu
```

## 2. Manual setup for Linux/macOS / Configuración manual para Linux/macOS

### English
If you are on a Unix-like system, replicate the steps performed by the batch script:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py [options]
```

Use `python main.py --help` to view the available command-line options.

### Español
En sistemas tipo Unix puedes reproducir los pasos que realiza el script por lotes:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py [opciones]
```

Ejecuta `python main.py --help` para ver las opciones disponibles desde la línea de comandos.

## 3. Building a standalone executable / Crear un ejecutable autónomo

### English
Generate a console executable with [PyInstaller](https://pyinstaller.org/):

1. Activate your virtual environment (optional but recommended).
2. Install PyInstaller:
   ```bash
   pip install pyinstaller
   ```
3. Build the project using the provided specification file:
   ```bash
   pyinstaller hdfull.spec
   ```
4. The executable will be available under `dist/HdfullScrappers/`.

To distribute the program, copy the generated folder to the target machine and run the executable from a terminal.

### Español
Genera un ejecutable de consola con [PyInstaller](https://pyinstaller.org/):

1. Activa tu entorno virtual (opcional pero recomendado).
2. Instala PyInstaller:
   ```bash
   pip install pyinstaller
   ```
3. Compila el proyecto usando el archivo de especificación incluido:
   ```bash
   pyinstaller hdfull.spec
   ```
4. El ejecutable quedará disponible en `dist/HdfullScrappers/`.

Para distribuir el programa, copia la carpeta generada a la máquina destino y ejecuta el archivo desde una terminal.

## 4. Updating dependencies / Actualizar dependencias

### English
When `requirements.txt` changes, rerun the installation command inside the virtual environment:
```bash
pip install -r requirements.txt
```

### Español
Cuando `requirements.txt` cambie, vuelve a ejecutar la instalación dentro del entorno virtual:
```bash
pip install -r requirements.txt
```
