# app/configure_logging.py
import logging
import io
import json
from typing import Any, Dict, Optional
import pprint
from datetime import datetime
from langchain_core.messages import BaseMessage
import pandas as pd

class PrettyFormatter(logging.Formatter):
    """A custom formatter that handles complex objects and provides better visual structure"""
    
    def __init__(self):
        super().__init__()
        self.plain_formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    def _format_dataframe(self, df: pd.DataFrame) -> str:
        """Format DataFrame with better readability"""
        return "\n" + df.to_string(index=True, max_cols=None, max_rows=None)

    def _format_langchain_message(self, msg: BaseMessage) -> Dict[str, Any]:
        """Format LangChain messages in a clean way"""
        return {
            "type": msg.__class__.__name__,
            "content": msg.content,
            "id": msg.id
        }

    def _format_complex_value(self, value: Any, indent_level: int = 0) -> str:
        """Format different types of values with appropriate styling"""
        if isinstance(value, pd.DataFrame):
            return self._format_dataframe(value)
        elif isinstance(value, BaseMessage):
            return json.dumps(self._format_langchain_message(value), indent=2)
        elif isinstance(value, (dict, list)):
            return "\n" + json.dumps(value, indent=2, default=str)
        elif isinstance(value, str) and len(value) > 100:
            return f"\n{value}"
        return str(value)

    def _format_section(self, title: str, content: Any) -> str:
        """Format a section with title and content"""
        separator = "─" * 50
        formatted_content = self._format_complex_value(content)
        return f"\n{separator}\n{title}:\n{separator}\n{formatted_content}\n"

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with enhanced readability"""
        # Handle basic message first
        basic_msg = self.plain_formatter.format(record)
        
        # If the message is a dict, format it as sections
        if isinstance(record.msg, dict):
            sections = []
            for key, value in record.msg.items():
                sections.append(self._format_section(key.upper(), value))
            return basic_msg + "\n".join(sections)
            
        # If it's any other complex object, format it appropriately
        if isinstance(record.msg, (list, dict, pd.DataFrame, BaseMessage)):
            return basic_msg + self._format_complex_value(record.msg)
            
        return basic_msg

class EnhancedLogHandler(logging.Handler):
    """Enhanced log handler with better formatting and memory management"""
    
    def __init__(self, level=logging.NOTSET, max_buffer_size: int = 1000000):
        super().__init__(level)
        self.log_buffer = io.StringIO()
        self.formatter = PrettyFormatter()
        self.max_buffer_size = max_buffer_size

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.formatter.format(record)
            
            # Check buffer size and rotate if needed
            if self.log_buffer.tell() > self.max_buffer_size:
                self.rotate_buffer()
                
            self.log_buffer.write(msg + "\n")
            
        except Exception as e:
            fallback_msg = f"Error formatting log: {str(e)}\n"
            self.log_buffer.write(fallback_msg)

    def rotate_buffer(self):
        """Rotate the buffer by keeping only the last 75% of content"""
        content = self.log_buffer.getvalue()
        self.log_buffer.seek(0)
        self.log_buffer.truncate()
        self.log_buffer.write(content[len(content)//4:])

    def get_logs(self) -> str:
        """Get all logs with optional filtering"""
        return self.log_buffer.getvalue()

    def clear_logs(self):
        """Clear all logs from the buffer"""
        self.log_buffer.seek(0)
        self.log_buffer.truncate()

def configure_enhanced_logging():
    """Configure the enhanced logging system"""
    # Create and configure the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add our enhanced buffer handler
    buffer_handler = EnhancedLogHandler()
    logger.addHandler(buffer_handler)
    
    # Add a console handler with the same formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(PrettyFormatter())
    logger.addHandler(console_handler)
    
    return logger

def log_workflow_response(logger: logging.Logger, response: Dict[str, Any]):
    """Helper function to log workflow responses in a structured way"""
    logger.debug({
        "workflow_status": "Response received",
        "messages": response.get('messages', []),
        "extracted_data": {
            k: v for k, v in response.items() 
            if k != 'messages' and not k.startswith('_')
        }
    })