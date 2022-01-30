import keys
from loguru import logger as log
from sqlalchemy import create_engine

table = 'positions'

log.debug(f'Starting sqlalchemy postgresql connection')
try:
    db_string = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/personal'
    db = create_engine(db_string)
    log.debug(f'Sqlalchemy postgresql connection established')
except Exception as error:
    log.critical(f'Error connecting to postgresql database: {error}')
    exit(1)


log.info(f'Creating table {table}')
create_script = f""" CREATE TABLE {table.lower()} (
    symbol           VARCHAR(10) PRIMARY KEY NOT NULL UNIQUE,
    name             VARCHAR(30) NOT NULL,
    shares           NUMERIC(24, 8),
    purchase         NUMERIC(24, 8),
    price            NUMERIC(24, 8),
    change_24h       NUMERIC(10, 2),
    change_1w        NUMERIC(10, 2),
    change_1m        NUMERIC(10, 2),
    change_1y        NUMERIC(10, 2),
    cost             MONEY,
    value            MONEY,
    total_gain_pct   NUMERIC(10, 2),
    total_gain_usd   NUMERIC(10, 2),
    volume_24h       NUMERIC(24, 0),
    rank             NUMERIC(5, 0)
)"""
db.execute(create_script)
