from sys import exit

import keys
import psycopg2
from loguru import logger as log


def create_table(sym):

    conn = None
    cur = None
    log.debug(f'Checking table for [{sym}]')

    try:
        conn = psycopg2.connect(host=keys.hostname, dbname=keys.database, user=keys.username, password=keys.password, port=keys.port)
        cur = conn.cursor()

        create_script = f""" CREATE TABLE IF NOT EXISTS {sym} (
            id               int PRIMARY KEY,
            opentime         TIMESTAMP NOT NULL,
            high             NUMERIC(14, 8) NOT NULL,
            low              NUMERIC(14, 8) NOT NULL,
            close            NUMERIC(14, 8) NOT NULL,
            volume           NUMERIC(17, 8) NOT NULL,
            closetime        TIMESTAMP NOT NULL,
            asset_vol        NUMERIC(17, 8) NOT NULL,
            trades           NUMERIC(8, 0) NOT NULL,
            taker_buy_base   NUMERIC(17, 8) NOT NULL,
            taker_buy_quote  NUMERIC(17, 8) NOT NULL
        )"""

        cur.execute(create_script)
        conn.commit()
        log.debug(f'Table creation complete for [{sym}]')
        return True

    except Exception as error:
        log.critical(f'Error creating table [{sym}]: {error}')
        exit(1)

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
