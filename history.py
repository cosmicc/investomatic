from sqlalchemy import create_engine
from loguru import logger as log
import pandas as pd
import keys

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
    db = create_engine(db_string)
    log.debug(f'Sqlalchemy postgresql connection established')
except Exception as error:
    log.critical(f'Error connecting to postgresql database: {error}')
    exit(1)


#log.info(f'Creating table {table}')
create_script = f""" CREATE TABLE IF NOT EXISTS {table.lower()} (
    timestamp        TIMESTAMP PRIMARY KEY NOT NULL UNIQUE,
    security         VARCHAR(10) NOT NULL,
    action           VARCHAR(15) NOT NULL,
    quantity         NUMERIC(18, 8) NOT NULL,
    price            NUMERIC(18, 8) NOT NULL,
    subtotal         MONEY,
    total            MONEY,
    fee              MONEY,
    note             VARCHAR(100),
    processed        BOOL
)"""
db.execute(create_script)

df = pd.read_csv('coinbase.csv')

df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

columns = ['timestamp', 'action', 'security', 'quantity', 'price', 'subtotal', 'total', 'fee', 'note']

df_data = df[columns]
print(df_data)

df_data.to_sql(table, db, index=False, index_label='timestamp', if_exists='append', method=insert_do_nothing_on_conflicts)
#df_data.set_index('timestamp', inplace=True)
