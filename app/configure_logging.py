# configure_logging.py

import logging

def configure_logging():
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Attach your handler to the root logger, or any named logger
    root_logger = logging.getLogger()
    # set formatter
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)
    root_logger.setLevel(logging.INFO)

    # Optionally add more handlers like console or file
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)