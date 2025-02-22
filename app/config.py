# app/config.py
from configparser import ConfigParser
import os
from langchain_community.utilities import SQLDatabase

localdb = True

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config(filename=os.path.join(BASE_DIR, "database.ini"), section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
        config['password'] = os.environ.get('VOB_PASS')
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    if localdb:
        config['host'] = 'localhost'
        config['user'] = 'postgres'
        config['password'] = os.environ.get('POSTGRES_PASS')
    return config


def get_db(config):
    dburi = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:5432/{config['dbname']}"
    db = SQLDatabase.from_uri(dburi, schema=config['schema'])
    return db

if __name__ == '__main__':
    config = load_config()
    print(config)