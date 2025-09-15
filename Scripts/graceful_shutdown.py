import signal
import threading


class GracefulShutdown:
    """Utility to handle graceful shutdown via system signals."""

    def __init__(self):
        self.shutdown_event = threading.Event()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.shutdown_event.set()
