# app/configure_logging.py
import logging
import io
import pprint
import json

class InMemoryLogHandler(logging.Handler):
    """A custom log handler to store log records in memory."""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.log_buffer = io.StringIO()
        # Use a more detailed format for debug messages
        self.formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    def emit(self, record: logging.LogRecord):
        try:
            # Special handling for complex objects
            if isinstance(record.msg, (dict, list, tuple)):
                record.msg = '\n' + pprint.pformat(record.msg, indent=4, width=120)
            
            # Format the log message and write to buffer
            msg = self.formatter.format(record)
            self.log_buffer.write(msg + "\n")
        except Exception as e:
            # Fallback error handling
            self.log_buffer.write(f"Error formatting log: {str(e)}\n")

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
    # Configure root logger with detailed format
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Attach our in-memory log handler to the root logger
    logger = logging.getLogger()
    logger.addHandler(in_memory_log_handler)

    return logger

def format_complex_object(obj, max_length=1000):
    """Helper function to format complex objects for logging"""
    try:
        if isinstance(obj, (dict, list, tuple)):
            formatted = pprint.pformat(obj, indent=4, width=120)
            if len(formatted) > max_length:
                return formatted[:max_length] + "..."
            return formatted
        return str(obj)
    except Exception as e:
        return f"Error formatting object: {str(e)}"

# Add format_state helper function that can be imported where needed
def format_state_for_logging(state):
    """Helper function to format state object for logging"""
    if not state:
        return "None"
    
    try:
        state_dict = {
            "values": state.values,
            "next": state.next,
            "config": state.config,
            "metadata": {
                k: format_complex_object(v, max_length=500)
                for k, v in state.metadata.items()
            }
        }
        return json.dumps(state_dict, indent=4)
    except Exception as e:
        return f"Error formatting state: {str(e)}"