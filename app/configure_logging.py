import logging
import io

class InMemoryLogHandler(logging.Handler):
    """A custom log handler to store log records in memory."""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.log_buffer = io.StringIO()

    def emit(self, record: logging.LogRecord):
        # Format the log message and write to buffer
        msg = self.format(record)
        self.log_buffer.write(msg + "\n")

    def get_logs(self) -> str:
        """Return the entire contents of the log buffer."""
        return self.log_buffer.getvalue()

    def clear_logs(self):
        """Clear the log buffer."""
        self.log_buffer.seek(0)
        self.log_buffer.truncate(0)

# Create a global instance of our in-memory handler
in_memory_log_handler = InMemoryLogHandler()

def configure_logging():
    # Basic config
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    # Attach our in-memory log handler to the root logger
    logger = logging.getLogger()
    logger.addHandler(in_memory_log_handler)

    return logger