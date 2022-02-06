import keys
import pandas as pd
from loguru import logger as log
from sqlalchemy import create_engine

table = 'history'


def insert_do_nothing_on_conflicts(sqltable, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy import table, column
    columns = []
    for c in keys:
        columns.append(column(c))
    if sqltable.schema:
        table_name = '{}.{}'.format(sqltable.schema, sqltable.name)
    else:
        table_name = sqltable.name
    mytable = table(table_name, *columns)
    insert_stmt = insert(mytable).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['timestamp'])
    conn.execute(do_nothing_stmt)


log.debug(f'Starting sqlalchemy postgresql connection')
try:
    db_string = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/personal'
    db_string2 = f'postgresql://{keys.username}:{keys.password}@{keys.hostname}:{keys.port}/{keys.database}'
    db = create_engine(db_string)
    db_feeder = create_engine(db_string2)
    log.debug(f'Sqlalchemy postgresql connection established')
except Exception as error:
    log.critical(f'Error connecting to postgresql database: {error}')
    exit(1)

log.debug(f'Loading table data from database')
history_table = pd.read_sql_table('history', db)
for index, value in history_table.iterrows():
    if value['processed'] is None or not value['processed']:
        log.info(f'Processing history entry: {value["timestamp"]} | {value["action"]} | {value["security"]}')
        position_table = pd.read_sql_table('positions', db)
        position_table.set_index('symbol', inplace=True)
        if value['action'] == 'Buy':
            if value['security'] not in position_table.index:
                log.info(f'Inserting new security into positions table: [{value["security"]}]')
                insert_string = """INSERT INTO positions (symbol, name, shares, purchase) values (%s, %s, %s, %s)"""
                values = (f"{value['security']}", f"{value['security']}", f"{value['quantity']}", f"{value['price']}")
                db.execute(insert_string, values)
            elif value['security'] in position_table.index:
                log.info(f'Updating [{value["security"]}] security data in positions table')
                update_string = f"""UPDATE positions SET shares=%s, purchase=%s WHERE symbol='{value['security']}'"""
                r = position_table.loc[value["security"]]["shares"] + value['quantity']
                values = (r, value['price'])
                db.execute(update_string, values)
        elif value['action'] == 'Coinbase Earn':
            if value['security'] not in position_table.index:
                log.info(f'Inserting new security into positions table: [{value["security"]}]')
                insert_string = """INSERT INTO positions (symbol, name, shares, purchase) values (%s, %s, %s, %s)"""
                values = (f"{value['security']}", f"{value['security']}", f"{value['quantity']}", f"{value['price']}")
                db.execute(insert_string, values)
            elif value['security'] in position_table.index:
                log.info(f'Updating [{value["security"]}] security data in positions table')
                update_string = f"""UPDATE positions SET shares=%s, purchase=%s WHERE symbol='{value['security']}'"""
                r = position_table.loc[value["security"]]["shares"] + value['quantity']
                values = (r, value['price'])
                db.execute(update_string, values)
        elif value['action'] == 'Convert':
            note = value["note"].split(' ')
            destcoin = note[5]
            destqty = float(note[4].replace(',', ''))
            log.info(f'Updating [{value["security"]}] conversion to [{destcoin}] in positions table')
            update_string = f"""UPDATE positions SET shares=%s WHERE symbol='{value['security']}'"""
            values = (position_table.loc[value["security"]]["shares"] - value['quantity'])
            db.execute(update_string, values)
            if destcoin not in position_table.index:
                insert_string = """INSERT INTO positions (symbol, name, shares) values (%s, %s, %s)"""
                values = (destcoin, destcoin, destqty)
                db.execute(insert_string, values)
            elif destcoin in position_table.index:
                update_string = f"""UPDATE positions SET shares=%s WHERE symbol='{destcoin}'"""
                values = (position_table.loc[destcoin]["shares"] + destqty)
                db.execute(update_string, values)

        upd_string = f"""UPDATE history SET processed = True WHERE timestamp = {value["timestamp"]}"""
        db.execute(upd_string)
    else:
        log.debug(f'Skipping history entry already marked processed: {value["timestamp"]}')

log.info(f'Processing position changes')
position_table = pd.read_sql_table('positions', db)
position_table.set_index('symbol', inplace=True)
for index, value in position_table.iterrows():
    if value['shares'] <= 0:
        log.info(f'Deleting [{index}] as its no longer being held')
        delete_string = f"""DELETE FROM positions WHERE symbol = %s"""
        vals = (index)
        db.execute(delete_string, vals)
        drop_string = f"""DROP TABLE %s"""
        jo = f'{index.lower}usdt'
        vag = (jo)
        db_feeder.execute(drop_string, vag)
