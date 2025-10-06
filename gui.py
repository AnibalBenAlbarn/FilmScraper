"""Interfaz gráfica para gestionar los scrapers de HDFull."""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import json
from typing import Callable, Dict, List, Optional

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QTextCursor
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QStatusBar,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QCheckBox,
)

from Scripts import scraper_utils
from Scripts.db_setup import create_direct_db, create_torrent_db
from Scripts.scraper_utils import (
    connect_db,
    execute_sql_script,
    setup_logger,
    clear_stop_request,
    request_stop,
)


PROGRESS_DIR = os.path.join(scraper_utils.PROJECT_ROOT, "progress")
DIRECT_MOVIES_PROGRESS_FILE = os.path.join(PROGRESS_DIR, "movies_direct_progress.json")
DIRECT_SERIES_PROGRESS_FILE = os.path.join(PROGRESS_DIR, "series_direct_progress.json")
TORRENT_MOVIES_PROGRESS_FILE = os.path.join(PROGRESS_DIR, "movies_torrent_progress.json")
TORRENT_SERIES_PROGRESS_FILE = os.path.join(PROGRESS_DIR, "series_torrent_progress.json")
UPDATE_PROGRESS_FILES = {
    "update_movies_premiere": os.path.join(PROGRESS_DIR, "update_movies_premiere_progress.json"),
    "update_movies_updated": os.path.join(PROGRESS_DIR, "update_movies_updated_progress.json"),
    "update_episodes_premiere": os.path.join(PROGRESS_DIR, "update_episodes_premiere_progress.json"),
    "update_episodes_updated": os.path.join(PROGRESS_DIR, "update_episodes_updated_progress.json"),
}


def _ensure_utf8_streams() -> None:
    """Fuerza la codificación UTF-8 en stdout/stderr cuando es posible."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is not None and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, AttributeError, OSError):
                pass


_ensure_utf8_streams()


class ScriptRunner(QThread):
    """Ejecuta un módulo scraper en un hilo independiente."""

    output = pyqtSignal(str)
    finished = pyqtSignal(bool)

    def __init__(self, module_name: str, extra_args: Optional[List[str]] = None) -> None:
        super().__init__()
        self.module_name = module_name
        self.extra_args = extra_args or []
        self._process: Optional[subprocess.Popen[str]] = None
        self._stop_requested = False
        self._supports_graceful_stop = module_name in {
            "torrent_dw_films_scraper",
            "torrent_dw_series_scraper",
        }
        self._force_stop_attempted = False

    def run(self) -> None:  # type: ignore[override]
        clear_stop_request()

        cmd = [
            sys.executable,
            "-m",
            f"Scripts.{self.module_name}",
            *self.extra_args,
        ]
        self.output.emit(f"\n▶ Ejecutando: {' '.join(cmd)}")

        try:
            popen_kwargs: dict = {
                "cwd": scraper_utils.PROJECT_ROOT,
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "text": True,
                "bufsize": 1,
                "encoding": "utf-8",
                "errors": "replace",
            }
            if os.name == "nt":  # pragma: no cover - dependiente de plataforma
                popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

            self._process = subprocess.Popen(cmd, **popen_kwargs)
        except Exception as exc:  # pragma: no cover - errores de entorno
            self.output.emit(f"[ERROR] No se pudo iniciar el proceso: {exc}")
            self.finished.emit(False)
            return

        assert self._process.stdout is not None
        try:
            for line in self._process.stdout:
                if not line:
                    break
                self.output.emit(line.rstrip())
        finally:
            self._process.stdout.close()

        return_code = self._process.wait()
        success = return_code == 0 and not self._stop_requested
        if self._stop_requested:
            self.output.emit("[INFO] Proceso detenido por el usuario.")
        elif success:
            self.output.emit("[OK] Proceso finalizado correctamente.")
        else:
            self.output.emit(f"[ERROR] El proceso terminó con código {return_code}.")
        self.finished.emit(success)

    def stop(self) -> None:
        self._stop_requested = True
        request_stop()
        if self._process and self._process.poll() is None:
            if self._supports_graceful_stop and not self._force_stop_attempted:
                self._force_stop_attempted = True
                self.output.emit("[INFO] Solicitud de parada recibida. Esperando a que termine el elemento actual…")
                return

            if self._supports_graceful_stop and self._force_stop_attempted:
                self.output.emit("[INFO] Forzando la detención del proceso…")

            try:
                if os.name == "nt" and hasattr(signal, "CTRL_BREAK_EVENT"):
                    self._process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    self._process.send_signal(signal.SIGINT)
            except Exception:
                try:
                    self._process.terminate()
                except Exception:
                    pass


class ScrapersTab(QWidget):
    """Pestaña para ejecutar scrapers y actualizaciones."""

    run_script_requested = pyqtSignal(str, list)

    def __init__(self, log_callback: Callable[[str], None], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.log_callback = log_callback
        self._buttons: List[QPushButton] = []
        self._spinboxes: List[QSpinBox] = []
        self.direct_movies_spin: Optional[QSpinBox] = None
        self.direct_series_spin: Optional[QSpinBox] = None
        self.torrent_movies_spin: Optional[QSpinBox] = None
        self.torrent_series_spin: Optional[QSpinBox] = None
        self.torrent_movies_failures_spin: Optional[QSpinBox] = None
        self.torrent_series_failures_spin: Optional[QSpinBox] = None
        self.direct_movies_progress_label: Optional[QLabel] = None
        self.direct_series_progress_label: Optional[QLabel] = None
        self.torrent_movies_progress_label: Optional[QLabel] = None
        self.torrent_series_progress_label: Optional[QLabel] = None
        self.update_progress_labels: Dict[str, QLabel] = {}
        self._build_ui()

    def _register_button(self, button: QPushButton) -> None:
        self._buttons.append(button)

    def _register_spinbox(self, spinbox: QSpinBox) -> None:
        self._spinboxes.append(spinbox)

    def _get_torrent_movies_max_failures(self) -> int:
        if self.torrent_movies_failures_spin:
            return max(1, int(self.torrent_movies_failures_spin.value()))
        return max(1, int(getattr(scraper_utils, "TORRENT_MOVIES_MAX_FAILURES", 10)))

    def _get_torrent_series_max_failures(self) -> int:
        if self.torrent_series_failures_spin:
            return max(1, int(self.torrent_series_failures_spin.value()))
        return max(1, int(getattr(scraper_utils, "TORRENT_SERIES_MAX_FAILURES", 10)))

    def _torrent_movies_args(self, base_args: Optional[List[str]] = None) -> List[str]:
        args = list(base_args or [])
        args.extend(["--max-failures", str(self._get_torrent_movies_max_failures())])
        return args

    def _torrent_series_args(self, base_args: Optional[List[str]] = None) -> List[str]:
        args = list(base_args or [])
        args.extend(["--max-failures", str(self._get_torrent_series_max_failures())])
        return args

    def _sync_failure_limits(self) -> None:
        if self.torrent_movies_failures_spin:
            self.torrent_movies_failures_spin.blockSignals(True)
            self.torrent_movies_failures_spin.setValue(
                int(getattr(scraper_utils, "TORRENT_MOVIES_MAX_FAILURES", 10))
            )
            self.torrent_movies_failures_spin.blockSignals(False)
        if self.torrent_series_failures_spin:
            self.torrent_series_failures_spin.blockSignals(True)
            self.torrent_series_failures_spin.setValue(
                int(getattr(scraper_utils, "TORRENT_SERIES_MAX_FAILURES", 10))
            )
            self.torrent_series_failures_spin.blockSignals(False)

    def _on_torrent_movies_failures_changed(self, value: int) -> None:
        scraper_utils.set_torrent_movies_max_failures(value)
        self.log_callback(
            f"Máximo de fallos consecutivos para películas torrent actualizado a {value}."
        )

    def _on_torrent_series_failures_changed(self, value: int) -> None:
        scraper_utils.set_torrent_series_max_failures(value)
        self.log_callback(
            f"Máximo de fallos consecutivos para series torrent actualizado a {value}."
        )

    def _emit_run(self, module: str, args: Optional[List[str]] = None) -> None:
        if args is None:
            args = []
        self.run_script_requested.emit(module, args)

    @staticmethod
    def _load_json(path: str) -> Optional[Dict[str, object]]:
        if not path or not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return None

    @staticmethod
    def _format_section(texts: List[str]) -> str:
        cleaned = [text for text in texts if text]
        return " • ".join(cleaned) if cleaned else "Sin registros disponibles."

    @staticmethod
    def _extract_last_series_page(progress_data: Dict[str, object]) -> Optional[Dict[str, Optional[object]]]:
        best_page: Optional[int] = None
        best_timestamp: Optional[str] = None
        for key in ("pages_odd", "pages_even"):
            pages = progress_data.get(key, {})
            if not isinstance(pages, dict):
                continue
            for page_key, info in pages.items():
                if not isinstance(info, dict) or not info.get("processed"):
                    continue
                try:
                    page_num = int(page_key)
                except (TypeError, ValueError):
                    continue
                timestamp = info.get("timestamp")
                if best_page is None or page_num > best_page or (
                    page_num == best_page and str(timestamp or "") > str(best_timestamp or "")
                ):
                    best_page = page_num
                    best_timestamp = timestamp if isinstance(timestamp, str) else str(timestamp or "")
        if best_page is None:
            return None
        return {"page": best_page, "timestamp": best_timestamp}

    def _reset_progress(self, path: str, description: str) -> None:
        if not path:
            return

        try:
            if os.path.exists(path):
                os.remove(path)
                self._apply_progress_defaults(path)
                QMessageBox.information(
                    self,
                    "Progreso reiniciado",
                    f"Se eliminó el progreso almacenado de {description}.",
                )
                self.log_callback(f"Progreso de {description} reiniciado.")
            else:
                QMessageBox.information(
                    self,
                    "Progreso reiniciado",
                    f"No se encontró progreso previo para {description}.",
                )
                self.log_callback(f"No se encontró progreso previo para {description}.")
                self._apply_progress_defaults(path)
        except OSError as exc:
            QMessageBox.warning(
                self,
                "Error",
                f"No se pudo reiniciar el progreso de {description}: {exc}",
            )
            self.log_callback(f"Error al reiniciar el progreso de {description}: {exc}")
            return

        self.refresh_progress_info()

    def _apply_progress_defaults(self, path: str) -> None:
        """Restaura los valores por defecto en la interfaz tras limpiar el progreso."""

        if path == DIRECT_MOVIES_PROGRESS_FILE:
            if self.direct_movies_progress_label:
                self.direct_movies_progress_label.setText("Sin registros disponibles.")
            if self.direct_movies_spin:
                self.direct_movies_spin.setValue(1)
        elif path == DIRECT_SERIES_PROGRESS_FILE:
            if self.direct_series_progress_label:
                self.direct_series_progress_label.setText("Sin registros disponibles.")
            if self.direct_series_spin:
                self.direct_series_spin.setValue(1)
        elif path == TORRENT_MOVIES_PROGRESS_FILE:
            if self.torrent_movies_progress_label:
                self.torrent_movies_progress_label.setText("Sin registros disponibles.")
            if self.torrent_movies_spin:
                self.torrent_movies_spin.setValue(1)
        elif path == TORRENT_SERIES_PROGRESS_FILE:
            if self.torrent_series_progress_label:
                self.torrent_series_progress_label.setText("Sin registros disponibles.")
            if self.torrent_series_spin:
                self.torrent_series_spin.setValue(1)

    def refresh_progress_info(self) -> None:
        self._update_direct_movies_info()
        self._update_direct_series_info()
        self._update_torrent_movies_info()
        self._update_torrent_series_info()
        self._update_updates_info()
        self._sync_failure_limits()

    def _update_direct_movies_info(self) -> None:
        if not self.direct_movies_progress_label or not self.direct_movies_spin:
            return

        data = self._load_json(DIRECT_MOVIES_PROGRESS_FILE) or {}
        page_number = data.get("page_number")
        try:
            page_value = max(1, int(page_number))
        except (TypeError, ValueError):
            page_value = 1

        last_title = data.get("last_movie_title")
        last_index = data.get("last_movie_index")
        if last_title:
            if isinstance(last_index, int) and last_index >= 0:
                title_text = f"Última película: {last_title} (índice {last_index})"
            else:
                title_text = f"Última película: {last_title}"
        else:
            title_text = "Última película: sin datos"

        total_saved = data.get("total_saved")
        total_text = f"Enlaces guardados: {total_saved}" if isinstance(total_saved, int) else ""

        page_text = f"Siguiente página: {page_value}"

        self.direct_movies_progress_label.setText(
            self._format_section([page_text, title_text, total_text])
        )
        self.direct_movies_spin.setValue(page_value)

    def _update_direct_series_info(self) -> None:
        if not self.direct_series_progress_label or not self.direct_series_spin:
            return

        data = self._load_json(DIRECT_SERIES_PROGRESS_FILE) or {}
        page_info = self._extract_last_series_page(data)
        last_title = data.get("last_series_title")
        total_saved = data.get("total_saved")

        if page_info:
            try:
                page_value = max(1, int(page_info.get("page", 1)))
            except (TypeError, ValueError):
                page_value = 1
            page_text = f"Última página completada: {page_value}"
            timestamp = page_info.get("timestamp")
            timestamp_text = f"Actualizado: {timestamp}" if timestamp else ""
        else:
            page_value = 1
            page_text = "Última página completada: sin datos"
            timestamp_text = ""

        title_text = f"Última serie: {last_title}" if last_title else "Última serie: sin datos"
        total_text = f"Enlaces guardados: {total_saved}" if isinstance(total_saved, int) else ""

        self.direct_series_progress_label.setText(
            self._format_section([page_text, title_text, timestamp_text, total_text])
        )
        self.direct_series_spin.setValue(max(1, page_value))

    def _update_torrent_movies_info(self) -> None:
        if not self.torrent_movies_progress_label or not self.torrent_movies_spin:
            return

        data = self._load_json(TORRENT_MOVIES_PROGRESS_FILE) or {}
        current_id = data.get("current_id")
        try:
            next_id = max(1, int(current_id))
        except (TypeError, ValueError):
            next_id = 1
        last_id = next_id - 1 if next_id > 1 else None
        total_saved = data.get("total_saved")
        last_update = data.get("last_update")

        texts = [f"Siguiente ID: {next_id}"]
        if last_id:
            texts.append(f"Último ID completado: {last_id}")
        if isinstance(total_saved, int):
            texts.append(f"Registros guardados: {total_saved}")
        if last_update:
            texts.append(f"Actualizado: {last_update}")

        self.torrent_movies_progress_label.setText(self._format_section(texts))
        self.torrent_movies_spin.setValue(next_id)

    def _update_torrent_series_info(self) -> None:
        if not self.torrent_series_progress_label or not self.torrent_series_spin:
            return

        data = self._load_json(TORRENT_SERIES_PROGRESS_FILE) or {}
        current_id = data.get("current_id")
        try:
            next_id = max(1, int(current_id))
        except (TypeError, ValueError):
            next_id = 1
        last_id = next_id - 1 if next_id > 1 else None
        total_saved = data.get("total_saved")
        last_update = data.get("last_update")

        texts = [f"Siguiente ID: {next_id}"]
        if last_id:
            texts.append(f"Último ID completado: {last_id}")
        if isinstance(total_saved, int):
            texts.append(f"Registros guardados: {total_saved}")
        if last_update:
            texts.append(f"Actualizado: {last_update}")

        self.torrent_series_progress_label.setText(self._format_section(texts))
        self.torrent_series_spin.setValue(next_id)

    def _update_updates_info(self) -> None:
        for module, label in self.update_progress_labels.items():
            path = UPDATE_PROGRESS_FILES.get(module)
            data = self._load_json(path) or {}
            last_update = data.get("last_update")
            if last_update:
                text = f"Última ejecución: {last_update}"
            else:
                text = "Última ejecución: sin registros"
            label.setText(text)

    def _build_direct_group(self) -> QGroupBox:
        group = QGroupBox("Scrapers directos")
        layout = QVBoxLayout()

        # Películas directas
        movies_box = QGroupBox("Películas")
        movies_layout = QVBoxLayout()
        btn_movies_normal = QPushButton("Ejecutar completo")
        btn_movies_normal.clicked.connect(lambda: self._emit_run("direct_dw_films_scraper"))
        self._register_button(btn_movies_normal)

        movies_start_layout = QHBoxLayout()
        movies_start_label = QLabel("Iniciar desde página:")
        movies_start_spin = QSpinBox()
        movies_start_spin.setRange(1, 999999)
        movies_start_spin.setValue(1)
        self._register_spinbox(movies_start_spin)
        self.direct_movies_spin = movies_start_spin
        btn_movies_from_page = QPushButton("Ejecutar desde página")
        btn_movies_from_page.clicked.connect(
            lambda: self._emit_run(
                "direct_dw_films_scraper",
                ["--start-page", str(movies_start_spin.value())],
            )
        )
        self._register_button(btn_movies_from_page)
        movies_start_layout.addWidget(movies_start_label)
        movies_start_layout.addWidget(movies_start_spin)
        movies_start_layout.addWidget(btn_movies_from_page)

        movies_layout.addWidget(btn_movies_normal)
        movies_layout.addLayout(movies_start_layout)
        self.direct_movies_progress_label = QLabel("Sin registros disponibles.")
        self.direct_movies_progress_label.setWordWrap(True)
        movies_layout.addWidget(self.direct_movies_progress_label)
        btn_movies_reset = QPushButton("Reiniciar progreso")
        btn_movies_reset.clicked.connect(
            lambda: self._reset_progress(
                DIRECT_MOVIES_PROGRESS_FILE,
                "películas directas",
            )
        )
        self._register_button(btn_movies_reset)
        movies_layout.addWidget(btn_movies_reset)
        movies_box.setLayout(movies_layout)

        # Series directas
        series_box = QGroupBox("Series")
        series_layout = QVBoxLayout()
        btn_series_normal = QPushButton("Ejecutar completo")
        btn_series_normal.clicked.connect(lambda: self._emit_run("direct_dw_series_scraper"))
        self._register_button(btn_series_normal)

        series_start_layout = QHBoxLayout()
        series_start_label = QLabel("Iniciar desde página:")
        series_start_spin = QSpinBox()
        series_start_spin.setRange(1, 999999)
        series_start_spin.setValue(1)
        self._register_spinbox(series_start_spin)
        self.direct_series_spin = series_start_spin
        btn_series_from_page = QPushButton("Ejecutar desde página")
        btn_series_from_page.clicked.connect(
            lambda: self._emit_run(
                "direct_dw_series_scraper",
                ["--start-page", str(series_start_spin.value())],
            )
        )
        self._register_button(btn_series_from_page)
        series_start_layout.addWidget(series_start_label)
        series_start_layout.addWidget(series_start_spin)
        series_start_layout.addWidget(btn_series_from_page)

        series_layout.addWidget(btn_series_normal)
        series_layout.addLayout(series_start_layout)
        self.direct_series_progress_label = QLabel("Sin registros disponibles.")
        self.direct_series_progress_label.setWordWrap(True)
        series_layout.addWidget(self.direct_series_progress_label)
        btn_series_reset = QPushButton("Reiniciar progreso")
        btn_series_reset.clicked.connect(
            lambda: self._reset_progress(
                DIRECT_SERIES_PROGRESS_FILE,
                "series directas",
            )
        )
        self._register_button(btn_series_reset)
        series_layout.addWidget(btn_series_reset)
        series_box.setLayout(series_layout)

        # Actualizaciones
        updates_box = QGroupBox("Actualizaciones")
        updates_layout = QVBoxLayout()
        update_buttons = [
            ("Películas (estrenos)", "update_movies_premiere"),
            ("Películas (actualizadas)", "update_movies_updated"),
            ("Series (estrenos)", "update_episodes_premiere"),
            ("Series (actualizadas)", "update_episodes_updated"),
        ]
        for label, module in update_buttons:
            row_layout = QHBoxLayout()
            button = QPushButton(label)
            button.clicked.connect(lambda _, m=module: self._emit_run(m))
            self._register_button(button)
            row_layout.addWidget(button)
            progress_label = QLabel("Última ejecución: Sin registros.")
            progress_label.setWordWrap(True)
            row_layout.addWidget(progress_label, 1)
            row_layout.addStretch(1)
            updates_layout.addLayout(row_layout)
            self.update_progress_labels[module] = progress_label
        updates_box.setLayout(updates_layout)

        layout.addWidget(movies_box)
        layout.addWidget(series_box)
        layout.addWidget(updates_box)
        group.setLayout(layout)
        return group

    def _build_torrent_group(self) -> QGroupBox:
        group = QGroupBox("Scrapers torrent")
        layout = QVBoxLayout()

        movies_box = QGroupBox("Películas")
        movies_layout = QVBoxLayout()
        btn_resume = QPushButton("Reanudar/Actualizar")
        btn_resume.clicked.connect(
            lambda: self._emit_run(
                "torrent_dw_films_scraper",
                self._torrent_movies_args(["--resume"]),
            )
        )
        self._register_button(btn_resume)

        movies_start_layout = QHBoxLayout()
        label = QLabel("Iniciar desde página:")
        start_spin = QSpinBox()
        start_spin.setRange(1, 999999)
        start_spin.setValue(1)
        self._register_spinbox(start_spin)
        self.torrent_movies_spin = start_spin
        btn_from_page = QPushButton("Ejecutar desde página")
        btn_from_page.clicked.connect(
            lambda: self._emit_run(
                "torrent_dw_films_scraper",
                self._torrent_movies_args([
                    "--start-page",
                    str(start_spin.value()),
                ]),
            )
        )
        self._register_button(btn_from_page)
        movies_start_layout.addWidget(label)
        movies_start_layout.addWidget(start_spin)
        movies_start_layout.addWidget(btn_from_page)

        movies_layout.addWidget(btn_resume)
        movies_layout.addLayout(movies_start_layout)
        failures_layout = QHBoxLayout()
        failures_label = QLabel("Máx. fallos consecutivos:")
        failures_spin = QSpinBox()
        failures_spin.setRange(1, 999999)
        failures_spin.setValue(
            int(getattr(scraper_utils, "TORRENT_MOVIES_MAX_FAILURES", 10))
        )
        failures_spin.valueChanged.connect(self._on_torrent_movies_failures_changed)
        self._register_spinbox(failures_spin)
        self.torrent_movies_failures_spin = failures_spin
        failures_layout.addWidget(failures_label)
        failures_layout.addWidget(failures_spin)
        movies_layout.addLayout(failures_layout)
        self.torrent_movies_progress_label = QLabel("Sin registros disponibles.")
        self.torrent_movies_progress_label.setWordWrap(True)
        movies_layout.addWidget(self.torrent_movies_progress_label)
        btn_movies_reset = QPushButton("Reiniciar progreso")
        btn_movies_reset.clicked.connect(
            lambda: self._reset_progress(
                TORRENT_MOVIES_PROGRESS_FILE,
                "películas torrent",
            )
        )
        self._register_button(btn_movies_reset)
        movies_layout.addWidget(btn_movies_reset)
        movies_box.setLayout(movies_layout)

        series_box = QGroupBox("Series")
        series_layout = QVBoxLayout()
        btn_series_resume = QPushButton("Reanudar/Actualizar")
        btn_series_resume.clicked.connect(
            lambda: self._emit_run(
                "torrent_dw_series_scraper",
                self._torrent_series_args(["--resume"]),
            )
        )
        self._register_button(btn_series_resume)

        series_start_layout = QHBoxLayout()
        series_label = QLabel("Iniciar desde página:")
        series_spin = QSpinBox()
        series_spin.setRange(1, 999999)
        series_spin.setValue(1)
        self._register_spinbox(series_spin)
        self.torrent_series_spin = series_spin
        btn_series_from_page = QPushButton("Ejecutar desde página")
        btn_series_from_page.clicked.connect(
            lambda: self._emit_run(
                "torrent_dw_series_scraper",
                self._torrent_series_args([
                    "--start-page",
                    str(series_spin.value()),
                ]),
            )
        )
        self._register_button(btn_series_from_page)
        series_start_layout.addWidget(series_label)
        series_start_layout.addWidget(series_spin)
        series_start_layout.addWidget(btn_series_from_page)

        series_layout.addWidget(btn_series_resume)
        series_layout.addLayout(series_start_layout)
        series_failures_layout = QHBoxLayout()
        series_failures_label = QLabel("Máx. fallos consecutivos:")
        series_failures_spin = QSpinBox()
        series_failures_spin.setRange(1, 999999)
        series_failures_spin.setValue(
            int(getattr(scraper_utils, "TORRENT_SERIES_MAX_FAILURES", 10))
        )
        series_failures_spin.valueChanged.connect(self._on_torrent_series_failures_changed)
        self._register_spinbox(series_failures_spin)
        self.torrent_series_failures_spin = series_failures_spin
        series_failures_layout.addWidget(series_failures_label)
        series_failures_layout.addWidget(series_failures_spin)
        series_layout.addLayout(series_failures_layout)
        self.torrent_series_progress_label = QLabel("Sin registros disponibles.")
        self.torrent_series_progress_label.setWordWrap(True)
        series_layout.addWidget(self.torrent_series_progress_label)
        btn_series_reset = QPushButton("Reiniciar progreso")
        btn_series_reset.clicked.connect(
            lambda: self._reset_progress(
                TORRENT_SERIES_PROGRESS_FILE,
                "series torrent",
            )
        )
        self._register_button(btn_series_reset)
        series_layout.addWidget(btn_series_reset)
        series_box.setLayout(series_layout)

        layout.addWidget(movies_box)
        layout.addWidget(series_box)
        group.setLayout(layout)
        return group

    def _build_sequences_group(self) -> QGroupBox:
        group = QGroupBox("Ejecuciones secuenciales")
        layout = QVBoxLayout()
        btn_direct_sequence = QPushButton("Ejecutar todos (Direct)")
        btn_direct_sequence.clicked.connect(lambda: self._emit_run("run_all", ["--scraper", "direct"]))
        self._register_button(btn_direct_sequence)
        btn_torrent_sequence = QPushButton("Ejecutar todos (Torrent)")
        btn_torrent_sequence.clicked.connect(lambda: self._emit_run("run_all", ["--scraper", "torrent"]))
        self._register_button(btn_torrent_sequence)
        layout.addWidget(btn_direct_sequence)
        layout.addWidget(btn_torrent_sequence)
        group.setLayout(layout)
        return group

    def _build_ui(self) -> None:
        layout = QVBoxLayout()

        tabs = QTabWidget()

        direct_tab = QWidget()
        direct_layout = QVBoxLayout()
        direct_layout.addWidget(self._build_direct_group())
        direct_layout.addStretch(1)
        direct_tab.setLayout(direct_layout)

        torrent_tab = QWidget()
        torrent_layout = QVBoxLayout()
        torrent_layout.addWidget(self._build_torrent_group())
        torrent_layout.addStretch(1)
        torrent_tab.setLayout(torrent_layout)

        sequences_tab = QWidget()
        sequences_layout = QVBoxLayout()
        sequences_layout.addWidget(self._build_sequences_group())
        sequences_layout.addStretch(1)
        sequences_tab.setLayout(sequences_layout)

        tabs.addTab(direct_tab, "Direct")
        tabs.addTab(torrent_tab, "Torrent")
        tabs.addTab(sequences_tab, "Secuencias")

        layout.addWidget(tabs)
        self.setLayout(layout)
        self.refresh_progress_info()

    def set_running(self, running: bool) -> None:
        for button in self._buttons:
            button.setDisabled(running)
        for spinbox in self._spinboxes:
            spinbox.setDisabled(running)


class DatabaseTab(QWidget):
    """Pestaña para gestionar bases de datos."""

    def __init__(self, log_callback: Callable[[str], None], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.log_callback = log_callback
        self.direct_path_label = QLabel()
        self.direct_path_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
            | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        self.direct_path_label.setToolTip("Ruta actual de la base de datos directa utilizada por los scrapers.")
        self.torrent_path_label = QLabel()
        self.torrent_path_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
            | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        self.torrent_path_label.setToolTip("Ruta actual de la base de datos torrent utilizada por los scrapers.")
        self.db_logger = setup_logger("db_setup_ui", "db_setup_ui.log")
        self.sql_logger = setup_logger("sql_runner", "sql_runner.log")
        self._build_ui()
        self.refresh_paths()

    def refresh_paths(self) -> None:
        self.direct_path_label.setText(scraper_utils.DB_PATH)
        self.torrent_path_label.setText(scraper_utils.TORRENT_DB_PATH)

    def _build_ui(self) -> None:
        main_layout = QVBoxLayout()

        # Sección rutas
        paths_group = QGroupBox("Rutas de bases de datos")
        paths_layout = QFormLayout()

        direct_layout = QHBoxLayout()
        direct_layout.addWidget(self.direct_path_label)
        btn_change_direct = QPushButton("Cambiar…")
        btn_change_direct.clicked.connect(self.change_direct_path)
        direct_layout.addWidget(btn_change_direct)

        torrent_layout = QHBoxLayout()
        torrent_layout.addWidget(self.torrent_path_label)
        btn_change_torrent = QPushButton("Cambiar…")
        btn_change_torrent.clicked.connect(self.change_torrent_path)
        torrent_layout.addWidget(btn_change_torrent)

        paths_layout.addRow("Direct:", direct_layout)
        paths_layout.addRow("Torrent:", torrent_layout)
        paths_group.setLayout(paths_layout)

        # Sección creación
        create_group = QGroupBox("Creación de bases")
        create_layout = QHBoxLayout()
        btn_create_direct = QPushButton("Crear base Direct")
        btn_create_direct.clicked.connect(self.create_direct_db)
        btn_create_torrent = QPushButton("Crear base Torrent")
        btn_create_torrent.clicked.connect(self.create_torrent_db)
        btn_create_both = QPushButton("Crear ambas")
        btn_create_both.clicked.connect(self.create_both_db)
        create_layout.addWidget(btn_create_direct)
        create_layout.addWidget(btn_create_torrent)
        create_layout.addWidget(btn_create_both)
        create_group.setLayout(create_layout)

        # Ejecutar script SQL
        script_group = QGroupBox("Ejecutar script SQL")
        script_layout = QVBoxLayout()
        btn_run_sql = QPushButton("Seleccionar y ejecutar script…")
        btn_run_sql.clicked.connect(self.execute_sql)
        script_layout.addWidget(btn_run_sql)
        script_group.setLayout(script_layout)

        main_layout.addWidget(paths_group)
        main_layout.addWidget(create_group)
        main_layout.addWidget(script_group)
        main_layout.addStretch(1)
        self.setLayout(main_layout)

    def change_direct_path(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecciona la base de datos directa",
            scraper_utils.DB_PATH,
            "Bases de datos (*.db);;Todos los archivos (*)",
        )
        if not path:
            return
        if not os.path.exists(path):
            QMessageBox.warning(self, "Ruta no válida", "La ruta seleccionada no existe.")
            return
        scraper_utils.set_db_path(path)
        self.refresh_paths()
        self.log_callback(f"Ruta de base directa actualizada: {scraper_utils.DB_PATH}")
        QMessageBox.information(self, "Ruta actualizada", "Se guardó la nueva ruta de la base directa.")

    def change_torrent_path(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecciona la base de datos torrent",
            scraper_utils.TORRENT_DB_PATH,
            "Bases de datos (*.db);;Todos los archivos (*)",
        )
        if not path:
            return
        if not os.path.exists(path):
            QMessageBox.warning(self, "Ruta no válida", "La ruta seleccionada no existe.")
            return
        scraper_utils.set_torrent_db_path(path)
        self.refresh_paths()
        self.log_callback(f"Ruta de base torrent actualizada: {scraper_utils.TORRENT_DB_PATH}")
        QMessageBox.information(self, "Ruta actualizada", "Se guardó la nueva ruta de la base torrent.")

    def create_direct_db(self) -> None:
        selected_path, _ = QFileDialog.getSaveFileName(
            self,
            "Selecciona la ubicación de la base directa",
            scraper_utils.DB_PATH,
            "Bases de datos (*.db);;Todos los archivos (*)",
        )
        if not selected_path:
            return

        if not selected_path.endswith(".db"):
            selected_path = f"{selected_path}.db"
        selected_path = os.path.abspath(selected_path)

        if create_direct_db(selected_path) and scraper_utils.setup_database(self.db_logger, selected_path):
            try:
                conn = connect_db(selected_path)
                conn.close()
                scraper_utils.set_db_path(selected_path)
                self.refresh_paths()
                QMessageBox.information(
                    self,
                    "Base creada",
                    f"La base directa se creó y verificó correctamente en:\n{selected_path}",
                )
                self.log_callback(f"Base directa creada en {selected_path}.")
            except Exception as exc:  # pragma: no cover - errores de conexión
                QMessageBox.warning(self, "Error de conexión", f"No se pudo verificar la base directa: {exc}")
                self.log_callback(f"Error al verificar la base directa en {selected_path}: {exc}")
        else:
            QMessageBox.warning(self, "Error", "No se pudo crear la base directa.")
            self.log_callback(f"No se pudo crear la base directa en {selected_path}.")

    def create_torrent_db(self) -> None:
        selected_path, _ = QFileDialog.getSaveFileName(
            self,
            "Selecciona la ubicación de la base torrent",
            scraper_utils.TORRENT_DB_PATH,
            "Bases de datos (*.db);;Todos los archivos (*)",
        )
        if not selected_path:
            return

        if not selected_path.endswith(".db"):
            selected_path = f"{selected_path}.db"
        selected_path = os.path.abspath(selected_path)

        if create_torrent_db(selected_path):
            try:
                conn = sqlite3.connect(selected_path)
                conn.close()
                scraper_utils.set_torrent_db_path(selected_path)
                self.refresh_paths()
                QMessageBox.information(
                    self,
                    "Base creada",
                    f"La base torrent se creó y verificó correctamente en:\n{selected_path}",
                )
                self.log_callback(f"Base torrent creada en {selected_path}.")
            except Exception as exc:  # pragma: no cover - errores de conexión
                QMessageBox.warning(self, "Error de conexión", f"No se pudo verificar la base torrent: {exc}")
                self.log_callback(f"Error al verificar la base torrent en {selected_path}: {exc}")
        else:
            QMessageBox.warning(self, "Error", "No se pudo crear la base torrent.")
            self.log_callback(f"No se pudo crear la base torrent en {selected_path}.")

    def create_both_db(self) -> None:
        self.create_direct_db()
        self.create_torrent_db()

    def execute_sql(self) -> None:
        script_path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecciona script SQL",
            scraper_utils.PROJECT_ROOT,
            "Archivos SQL (*.sql);;Todos los archivos (*)",
        )
        if not script_path:
            return

        db_path, _ = QFileDialog.getOpenFileName(
            self,
            "Selecciona la base donde ejecutar",
            scraper_utils.DB_PATH,
            "Bases de datos (*.db);;Todos los archivos (*)",
        )
        if not db_path:
            db_path = scraper_utils.DB_PATH

        if execute_sql_script(script_path, db_path, self.sql_logger):
            QMessageBox.information(self, "Script ejecutado", "El script SQL se ejecutó correctamente.")
            self.log_callback(f"Script SQL ejecutado en {db_path}.")
        else:
            QMessageBox.warning(self, "Error", "No se pudo ejecutar el script SQL.")
            self.log_callback("Fallo al ejecutar el script SQL.")


class SettingsTab(QWidget):
    """Pestaña de configuración de ejecución."""

    def __init__(self, log_callback: Callable[[str], None], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.log_callback = log_callback
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QFormLayout()

        self.max_workers_spin = QSpinBox()
        self.max_workers_spin.setRange(1, 32)
        self.max_workers_spin.setValue(int(scraper_utils.MAX_WORKERS))
        self.max_workers_spin.valueChanged.connect(self.update_max_workers)

        self.max_retries_spin = QSpinBox()
        self.max_retries_spin.setRange(1, 999)
        self.max_retries_spin.setValue(int(scraper_utils.MAX_RETRIES))

        self.apply_max_retries_button = QPushButton("Aplicar")
        self.apply_max_retries_button.clicked.connect(self.apply_max_retries)

        self.cache_checkbox = QCheckBox("Activar caché de peticiones")
        self.cache_checkbox.setChecked(bool(scraper_utils.CACHE_ENABLED))
        self.cache_checkbox.stateChanged.connect(self.update_cache)

        layout.addRow("Máximo de workers:", self.max_workers_spin)

        retries_row = QHBoxLayout()
        retries_row.addWidget(self.max_retries_spin)
        retries_row.addWidget(self.apply_max_retries_button)
        retries_container = QWidget()
        retries_container.setLayout(retries_row)
        layout.addRow("Máximo de reintentos:", retries_container)
        layout.addRow(self.cache_checkbox)

        self.setLayout(layout)

    def refresh(self) -> None:
        self.max_workers_spin.setValue(int(scraper_utils.MAX_WORKERS))
        self.max_retries_spin.setValue(int(scraper_utils.MAX_RETRIES))
        self.cache_checkbox.setChecked(bool(scraper_utils.CACHE_ENABLED))

    def update_max_workers(self, value: int) -> None:
        scraper_utils.set_max_workers(value)
        self.log_callback(f"Máximo de workers actualizado a {value}.")

    def apply_max_retries(self) -> None:
        value = int(self.max_retries_spin.value())
        scraper_utils.set_max_retries(value)
        self.log_callback(f"Máximo de reintentos actualizado a {value}.")

    def update_cache(self, state: int) -> None:
        scraper_utils.set_cache_enabled(state == Qt.CheckState.Checked)
        status = "activada" if scraper_utils.CACHE_ENABLED else "desactivada"
        self.log_callback(f"Caché {status}.")


class MainWindow(QMainWindow):
    """Ventana principal de la aplicación."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("HDFull Scraper")
        self.resize(900, 700)

        self.logger = setup_logger("gui", "gui.log")
        self.runner: Optional[ScriptRunner] = None
        self.progress_label = QLabel("Listo")
        self.progress_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
            | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        self.progress_label.setMinimumWidth(320)

        self.tabs = QTabWidget()
        self.scrapers_tab = ScrapersTab(self.append_output)
        self.database_tab = DatabaseTab(self.append_output)
        self.settings_tab = SettingsTab(self.append_output)
        self.tabs.addTab(self.scrapers_tab, "Scrapers")
        self.tabs.addTab(self.database_tab, "Bases de datos")
        self.tabs.addTab(self.settings_tab, "Ajustes")

        self.scrapers_tab.run_script_requested.connect(self.start_script)

        central_widget = QWidget()
        central_layout = QVBoxLayout()
        central_layout.addWidget(self.tabs)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setPlaceholderText("Aquí aparecerá la salida de los procesos y mensajes.")
        self.log_output.setMinimumHeight(200)

        controls_layout = QHBoxLayout()
        self.stop_button = QPushButton("Detener ejecución")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_current_script)
        btn_clear = QPushButton("Limpiar registro")
        btn_clear.clicked.connect(self.log_output.clear)
        controls_layout.addWidget(self.stop_button)
        controls_layout.addWidget(btn_clear)
        controls_layout.addStretch(1)

        log_group = QGroupBox("Registro de ejecución")
        log_layout = QVBoxLayout()
        log_layout.addLayout(controls_layout)
        log_layout.addWidget(self.log_output)
        log_group.setLayout(log_layout)

        central_layout.addWidget(log_group)

        central_widget.setLayout(central_layout)
        self.setCentralWidget(central_widget)

        status_bar = QStatusBar()
        status_bar.addPermanentWidget(self.progress_label, 1)
        self.setStatusBar(status_bar)

    def append_output(self, message: str) -> None:
        encoding = getattr(sys.stdout, "encoding", "utf-8") or "utf-8"
        safe_message = message
        try:
            message.encode(encoding)
        except UnicodeEncodeError:
            safe_message = message.encode(encoding, errors="replace").decode(encoding)

        try:
            self.logger.info(safe_message)
        except UnicodeEncodeError:
            fallback = safe_message.encode("ascii", errors="replace").decode("ascii")
            self.logger.info(fallback)

        display_text = message.rstrip("\n")
        self.log_output.append(display_text)
        self.log_output.moveCursor(QTextCursor.MoveOperation.End)
        self._update_progress_indicator(message)

    @staticmethod
    def _strip_log_prefix(text: str) -> str:
        stripped = text.strip()
        parts = stripped.split(" - ", 2)
        if len(parts) == 3 and parts[1].upper() in {"INFO", "ERROR", "WARNING", "DEBUG", "CRITICAL"}:
            return parts[2]
        return stripped

    def _update_progress_indicator(self, message: str) -> None:
        payload = self._strip_log_prefix(message)
        if not payload:
            return

        lower_payload = payload.lower()
        if "extrayendo:" in payload:
            info = payload.split("Extrayendo:", 1)[1].strip()
            self.progress_label.setText(f"Extrayendo: {info}")
        elif "progreso guardado:" in payload:
            info = payload.split("Progreso guardado:", 1)[1].strip()
            self.progress_label.setText(f"Progreso guardado: {info}")
        elif payload.startswith("Guardado:"):
            self.progress_label.setText(payload.strip())
        elif "proceso detenido" in lower_payload:
            self.progress_label.setText("Detenido por el usuario.")
        elif "proceso finalizado" in lower_payload or "[ok]" in lower_payload:
            self.progress_label.setText("Proceso finalizado.")

    def start_script(self, module: str, args: Optional[List[str]]) -> None:
        if self.runner and self.runner.isRunning():
            QMessageBox.warning(self, "Proceso en ejecución", "Ya hay un proceso en marcha. Deténlo antes de iniciar otro.")
            return

        clear_stop_request()
        self.runner = ScriptRunner(module, args)
        self.runner.output.connect(self.append_output)
        self.runner.finished.connect(self.on_script_finished)
        self.scrapers_tab.set_running(True)
        self.stop_button.setEnabled(True)
        status_message = f"Ejecutando {module}…"
        self.statusBar().showMessage(status_message)
        self.progress_label.setText(status_message)
        self.runner.start()

    def stop_current_script(self) -> None:
        if self.runner and self.runner.isRunning():
            self.append_output(
                "Deteniendo proceso en ejecución… se esperará a que finalice el elemento actual."
            )
            self.statusBar().showMessage("Deteniendo ejecución tras completar el elemento actual…")
            self.progress_label.setText("Deteniendo tras completar el elemento actual…")
            self.runner.stop()
        else:
            self.stop_button.setEnabled(False)

    def on_script_finished(self, success: bool) -> None:
        runner = self.runner
        self.scrapers_tab.set_running(False)
        self.stop_button.setEnabled(False)

        was_stop = bool(runner and runner._stop_requested)
        if success:
            self.statusBar().showMessage("Proceso finalizado correctamente.", 5000)
            self.progress_label.setText("Proceso finalizado.")
        elif was_stop:
            self.statusBar().showMessage("Proceso detenido por el usuario.", 5000)
            self.progress_label.setText("Detenido por el usuario.")
        else:
            self.statusBar().showMessage("Proceso finalizado con errores.", 5000)

        clear_stop_request()
        self.runner = None
        self.scrapers_tab.refresh_progress_info()


def run_gui() -> int:
    """Inicializa y ejecuta la interfaz gráfica."""

    app = QApplication(sys.argv)
    app.setApplicationName("HDFull Scraper")
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":  # pragma: no cover - punto de entrada manual
    raise SystemExit(run_gui())
