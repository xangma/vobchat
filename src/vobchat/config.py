"""
Environment-first database configuration with optional database.ini.

This removes the hard requirement on database.ini. Values are read from
environment variables first, falling back to database.ini if present,
and finally to sane defaults for local/dev and Docker setups.
"""

from sqlalchemy import exc as sa_exc
from configparser import ConfigParser
import os
from langchain_community.utilities import SQLDatabase
from geoalchemy2 import Geometry
import warnings

# Feature flags
localdb = True
use_tunnel = False  # New flag for using SSH tunnel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _read_ini_if_present(filename: str, section: str) -> dict:
    """Best-effort read of database.ini; returns {} if missing or invalid."""
    parser = ConfigParser()
    try:
        parser.read(filename)
        if parser.has_section(section):
            return {k: v for k, v in parser.items(section)}
    except Exception:
        pass
    return {}


def load_config(filename=os.path.join(BASE_DIR, "database.ini"), section='postgresql'):
    # Start with values from database.ini if present
    config = _read_ini_if_present(filename, section)

    # Overlay environment variables (env takes precedence)
    config['host'] = os.environ.get('DB_HOST', config.get('host', 'localhost'))
    config['user'] = os.environ.get('DB_USER', config.get('user', 'postgres'))
    config['password'] = (
        os.environ.get('DB_PASSWORD')
        or os.environ.get('POSTGRES_PASS')
        or os.environ.get('VOB_PASS')
        or config.get('password', '')
    )
    config['dbname'] = os.environ.get('DB_NAME', config.get('dbname', 'vobchat'))
    config['port'] = int(os.environ.get('DB_PORT', config.get('port', 5432)))
    config['schema'] = os.environ.get('DB_SCHEMA', config.get('schema', 'hgis'))

    return config


def get_db(config):
    # If using an SSH tunnel
    if use_tunnel and not localdb:
        from sshtunnel import SSHTunnelForwarder
        ssh_config = _read_ini_if_present(
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
        # Respect port from config; default to 5432
        config['port'] = int(config.get('port', 5432))

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


def get_dash_base_paths():
    """
    Compute Dash base paths from environment, following Dash conventions.

    Reads DASH_URL_BASE_PATHNAME (preferred) and normalizes it to start and end
    with '/'. Returns a tuple of (route_prefix, url_base_pathname) where:
      - route_prefix: '' for root, or '/base' (no trailing slash) for building
        Flask route decorators and comparing request paths.
      - url_base_pathname: '/' for root, or '/base/' for Dash's config.

    For backward compatibility, if DASH_URL_BASE_PATHNAME is not set, this will
    fall back to DASH_URL_BASE or DASH_PREFIX if present. These are deprecated.
    """

    base = (
        os.getenv("DASH_URL_BASE_PATHNAME")
        or "/"
    )

    base = (base or "").strip()

    # Normalize to start and end with '/'
    if base in {"", "/"}:
        return "", "/"

    if not base.startswith("/"):
        base = "/" + base
    if not base.endswith("/"):
        base = base + "/"

    url_base_pathname = base
    route_prefix = base.rstrip("/")
    return route_prefix, url_base_pathname
