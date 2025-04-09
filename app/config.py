# app/config.py
from configparser import ConfigParser
import os
from langchain_community.utilities import SQLDatabase

localdb = False
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
        config['host'] = 'localhost'
        config['user'] = 'postgres'
        config['password'] = os.environ.get('POSTGRES_PASS')

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

    dburi = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    db = SQLDatabase.from_uri(dburi, schema=config['schema'])
    db.run("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")

    if use_tunnel and not localdb:
        db.tunnel = tunnel  # Store tunnel on the db object for later teardown

    return db


def close_db(db):
    if hasattr(db, 'tunnel'):
        db.tunnel.stop()