import sqlalchemy
import logging
from sqlalchemy_utils import create_database, drop_database

from new_models import Base

GENERATE_DB = True

def metadata_dump(sql, *multiparams, **params):
    logging.info(sql.compile(dialect=engine.dialect))

connection_url = "mysql+pymysql://root:1234@localhost/test"

if GENERATE_DB:
    drop_database(connection_url)
    create_database(connection_url)
    engine = sqlalchemy.engine.create_engine(connection_url)
else:
    logging.basicConfig(filename='new.sql', encoding='utf-8', level=logging.INFO, format='%(message)s')
    engine = sqlalchemy.engine.create_engine(connection_url, strategy='mock', executor=metadata_dump)

Base.metadata.create_all(engine)