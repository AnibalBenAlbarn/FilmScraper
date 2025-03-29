#!/usr/bin/env python
import os
import sys
import subprocess
from pathlib import Path

def ensure_virtualenv():
    """
    Verifica si se está ejecutando en un entorno virtual.
    Si no es así, crea un entorno virtual en la raíz del proyecto, instala los requerimientos
    y reinicia el script usando el intérprete del entorno virtual.
    """
    # Suponemos que __init__.py está en la carpeta Scripts y el requirements.txt en la raíz del proyecto.
    project_root = Path(__file__).resolve().parent.parent
    print("Directorio del proyecto:", project_root)

    # Si sys.prefix y sys.base_prefix son iguales, no estamos en un entorno virtual.
    if sys.prefix == sys.base_prefix:
        print("No se detectó entorno virtual. Creando uno...")

        # Definir la ruta del entorno virtual en la raíz del proyecto (p.ej., .venv)
        venv_dir = project_root / ".venv"
        if not venv_dir.exists():
            try:
                subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])
                print(f"Entorno virtual creado en: {venv_dir}")
            except Exception as e:
                print(f"Error al crear el entorno virtual: {e}")
                sys.exit(1)
        else:
            print("El entorno virtual ya existe.")

        # Determinar la ubicación de pip y python en el entorno virtual
        if os.name == 'nt':
            pip_exe = venv_dir / "Scripts" / "pip.exe"
            python_exe = venv_dir / "Scripts" / "python.exe"
        else:
            pip_exe = venv_dir / "bin" / "pip"
            python_exe = venv_dir / "bin" / "python"

        # Ubicar el archivo de requerimientos en la raíz del proyecto
        requirements_file = project_root / "requirements.txt"
        if not requirements_file.exists():
            print(f"Error: No se encontró el archivo de requerimientos en {requirements_file}")
            sys.exit(1)

        # Instalar las dependencias
        try:
            print("Instalando dependencias desde:", requirements_file)
            subprocess.check_call([str(pip_exe), "install", "-r", str(requirements_file)])
        except Exception as e:
            print(f"Error al instalar dependencias: {e}")
            sys.exit(1)

        print("Dependencias instaladas correctamente. Reiniciando el script con el entorno virtual...")
        # Reiniciar el script utilizando el intérprete del entorno virtual
        os.execv(str(python_exe), [str(python_exe)] + sys.argv)
    else:
        print("Entorno virtual detectado. Continuando la ejecución...")

def main():
    # Aquí inicia el resto de tu aplicación.
    print("Ejecutando la aplicación principal.")
    # Por ejemplo, podrías iniciar el menú principal de gestión de scrapers o llamar a otra función.

if __name__ == "__main__":
    ensure_virtualenv()
    main()