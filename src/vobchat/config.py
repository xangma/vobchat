# app/config.py
from sqlalchemy import exc as sa_exc
from configparser import ConfigParser
import os
from langchain_community.utilities import SQLDatabase
from geoalchemy2 import Geometry
import warnings

localdb = True
use_tunnel = False  # New flag for using SSH tunnel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_config(filename=os.path.join(BASE_DIR, "database.ini"), section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
        config['password'] = os.environ.get('VOB_PASS')
    else:
        raise Exception(f'Section {section} not found in {filename}')

    if localdb:
        # Docker environment variables override defaults
        config['host'] = os.environ.get('DB_HOST', 'localhost')
        config['user'] = os.environ.get('DB_USER', 'postgres')
        config['password'] = os.environ.get('DB_PASSWORD', os.environ.get('POSTGRES_PASS'))
        config['dbname'] = os.environ.get('DB_NAME', config.get('dbname', 'vobchat'))
        config['port'] = int(os.environ.get('DB_PORT', '5432'))

    return config


def get_db(config):
    # If using an SSH tunnel
    if use_tunnel and not localdb:
        from sshtunnel import SSHTunnelForwarder
        ssh_config = load_config(
            filename=os.path.join(BASE_DIR, "database.ini"),
            section='sshtunnel'
        )
        tunnel = SSHTunnelForwarder(
            ssh_address_or_host=ssh_config['host'],
            ssh_username=ssh_config['user'],
            allow_agent=True,
            ssh_pkey=os.path.expanduser(ssh_config['pkey']),
            ssh_password=os.environ.get('UOP_PASS'),
            ssh_private_key_password=os.environ.get('UOP_PASS'),
            remote_bind_address=(ssh_config['remote_bind_address'], int(ssh_config['remote_bind_port'])),
            local_bind_address=(ssh_config['local_bind_address'],),
        )
        tunnel.start()
        config['host'] = 'localhost'
        config['port'] = tunnel.local_bind_port
    else:
        config['port'] = 5432

    # Add connection pool settings and timeouts for better reliability
    dburi = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}?connect_timeout=10&application_name=vobchat"

    warnings.filterwarnings("ignore", category=sa_exc.SAWarning)  # Ignore SQLAlchemy warnings
    db = SQLDatabase.from_uri(dburi, schema=config['schema'])

    try:
        db.run("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
        # Set additional session parameters for stability
        db.run("SET statement_timeout = '30s';")  # 30 second query timeout
        db.run("SET idle_in_transaction_session_timeout = '60s';")  # Close idle transactions
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Could not set database session parameters: {e}")

    if use_tunnel and not localdb:
        db.tunnel = tunnel  # Store tunnel on the db object for later teardown

    return db


def close_db(db):
    if hasattr(db, 'tunnel'):
        db.tunnel.stop()
