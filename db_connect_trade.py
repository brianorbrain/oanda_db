'''File to save DB metadata and connections'''

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, func, text
from sqlalchemy.sql import bindparam, exists, select
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR, CHAR, DECIMAL, BOOLEAN, INTEGER, FLOAT, BLOB, insert, SMALLINT, LONGTEXT
import socket
import pandas as pd
import numpy as np
import datetime


metadata = MetaData()
mysql_table = Table('trade_candles', metadata,
                    Column('time', DATETIME(fsp = 6), primary_key = True),
                    Column('symbol', VARCHAR(length = 50), primary_key = True),
                    Column('timeframe', CHAR(length = 3), primary_key = True),
                    Column('o', DECIMAL(precision = 9, scale = 5)),
                    Column('h', DECIMAL(precision = 9, scale = 5)),
                    Column('l', DECIMAL(precision = 9, scale = 5)),
                    Column('c', DECIMAL(precision = 9, scale = 5)),
                    Column('complete', BOOLEAN),
                    Column('volume', INTEGER))

pairs = ['USD_CHF', 'NZD_JPY', 'CAD_JPY', 'GBP_USD', 'NZD_USD',
         'USD_CAD', 'USD_JPY', 'AUD_JPY', 'AUD_USD', 'CHF_JPY',
         'EUR_AUD', 'EUR_CAD', 'EUR_CHF', 'EUR_GBP', 'EUR_JPY',
         'EUR_NZD', 'EUR_USD', 'GBP_AUD', 'GBP_CAD', 'GBP_CHF',
         'GBP_JPY', 'AUD_NZD', 'AUD_CAD']

def db_connection():
    statement = '''mysql://user:pass@10.4.1.11/finance'''
    mysql_engine = create_engine(statement, echo = False)
    return mysql_engine.connect()

def most_recent(symbol, timeframe, conn):
    sql_statement = text("SELECT DATE_FORMAT(time, '%Y-%m-%dT%H:%i:%S.%f000Z') FROM trade_candles "
                         "WHERE (symbol = :symbol AND timeframe = :timeframe AND complete = 1) "
                         "ORDER BY time DESC "
                         "LIMIT 1")
    result = conn.execute(sql_statement, {'symbol':symbol, 'timeframe':timeframe}).fetchone()
    return result


'''Calculate elapsed rows for the calculation of trends.'''
'''Needs minus 1 because of inclusive between'''
def candles_between(conn, pair, timeframe, start, end):
    stmt = text('''SELECT COUNT(*) FROM forex '''
                '''WHERE symbol = :symbol AND timeframe = :timeframe '''
                '''AND time BETWEEN :start AND :end''')
    results = conn.execute(stmt, {'symbol':pair, 'timeframe':timeframe, 'start':start, 'end':end})
    return results.fetchone()[0]-1

'''Insert a list of candles'''
def candle_insert(conn, candles):
    stmt = text("""INSERT INTO trade_candles (time, symbol, timeframe, o, h, l, c, complete, volume)
                VALUES( STR_TO_DATE(:time, '%Y-%m-%dT%H:%i:%S.%f000Z'), :symbol, :timeframe, :o, :h, :l, :c, :complete, :volume)
                ON DUPLICATE KEY UPDATE time= STR_TO_DATE(:time, '%Y-%m-%dT%H:%i:%S.%f000Z'), symbol = :symbol, 
                timeframe = :timeframe, o = :o, h = :h, l = :l, c = :c, complete = :complete, volume = :volume""")
    conn.execute(stmt, candles)

def candle_insert_trade(conn, candles):
    stmt = text("""INSERT INTO trade_candles (time, symbol, timeframe, o, h, l, c, complete, volume)
                VALUES( STR_TO_DATE(:time, '%Y-%m-%dT%H:%i:%S.%f000Z'), :symbol, :timeframe, :o, :h, :l, :c, :complete, :volume)
                ON DUPLICATE KEY UPDATE time= STR_TO_DATE(:time, '%Y-%m-%dT%H:%i:%S.%f000Z'), symbol = :symbol, 
                timeframe = :timeframe, o = :o, h = :h, l = :l, c = :c, complete = :complete, volume = :volume""")
    conn.execute(stmt, candles)

def candle_list_insert(conn, candles):
    conn.execute(mysql_table.insert(), candles)

def select_dataframe(conn, pair, timeframe, start, end):
    stmt = (sqlalchemy.sql.select([mysql_table.c.time, mysql_table.c.symbol, mysql_table.c.timeframe,
                                   mysql_table.c.o, mysql_table.c.h, mysql_table.c.l, mysql_table.c.c])
                      .where(mysql_table.c.symbol == pair)
                      .where(mysql_table.c.timeframe == timeframe)
                      .where(mysql_table.c.time >= start)
                      .where(mysql_table.c.time <= end)
                      .order_by(mysql_table.c.time))
    ##print(stmt)
    candle_dataframe = pd.read_sql_query(sql = stmt,
                                         con = conn,
                                         index_col = 'time',
                                         params = {'pair':pair, 'timeframe':timeframe, 'start':start, 'end':end})
    return candle_dataframe
    


##I need to consolidate these 2 using kwargs or something

def select_recent_candles(conn, pair, timeframe):
    stmt = text('''SELECT * FROM forex WHERE (symbol = :symbol AND timeframe = :timeframe) ORDER BY time DESC LIMIT 201''')
    data = conn.execute(stmt, {'symbol':pair, 'timeframe':timeframe})
    return data.fetchall()

def select_candles(conn, pair, timeframe):
    stmt = text('''SELECT * FROM forex WHERE (symbol = :symbol AND timeframe = :timeframe AND lr_results IS NOT NULL) ORDER BY time ASC''')
    data = conn.execution_options(stream_results=True).execute(stmt, {'symbol':pair, 'timeframe':timeframe})
    return data

def select_candles_slow(conn, pair, timeframe):
    stmt = (select([mysql_table])
                          .where(mysql_table.c.symbol == pair)
                          .where(mysql_table.c.timeframe == timeframe)
                          .order_by(mysql_table.c.time))
    return conn.execute(stmt)

def select_candle_window(conn, pair, timeframe, start, end):
    stmt = (sqlalchemy.sql.select([mysql_table])
                          .where(mysql_table.c.symbol == pair)
                          .where(mysql_table.c.timeframe == timeframe)
                          .where(mysql_table.c.time >= start)
                          .where(mysql_table.c.time <= end)
                          .where(mysql_table.c.lr_results != None)
                          .where(mysql_table.c.complete == 1)
                          .order_by(mysql_table.c.time))
    result = conn.execute(stmt)
    return result.fetchall()
    
def offset_candles(conn, pair, timeframe, search_time, offset):
    if offset <= 0 :
        stmt =text('''SELECT time FROM trade_candles WHERE symbol = :symbol '''
                   '''AND timeframe = :timeframe AND time < :search_time '''
                   '''ORDER BY time DESC LIMIT :offset''')
    else:
        stmt =text('''SELECT time FROM trade_candles WHERE symbol = :symbol '''
                   '''AND timeframe = :timeframe AND time > :search_time '''
                   '''LIMIT :offset''')

    results = conn.execute(stmt, {'symbol':pair, 'timeframe':timeframe, 'search_time':search_time, 'offset':abs(offset)})
    return results.fetchall()[-1][0]

def select_x_closed(conn, pair, timeframe, timestamp, x):
    stmt = (sqlalchemy.sql.select([mysql_table.c.o, mysql_table.c.c])
                          .where(mysql_table.c.symbol == pair)
                          .where(mysql_table.c.timeframe == timeframe)
                          .where(mysql_table.c.time <= timestamp)
                          .order_by(mysql_table.c.time.desc())
                          .limit(x))
    return conn.execute(stmt).fetchall()

def latest_close(conn, pair):
    stmt = (sqlalchemy.sql.select([mysql_table.c.c])
                          .where(mysql_table.c.symbol == pair)
                          .order_by(mysql_table.c.time.desc())
                          .limit(1))
    return conn.execute(stmt).fetchone()

def update_pip_value(conn, pair, value):
    stmt = pip_value.update().where(pip_value.c.symbol == pair).values(pip_value = value)
    conn.execute(stmt)
    

def main():
    conn = db_connection(remote = True)
    timestamp = most_recent('EUR_USD', 'M15', conn)
    print(timestamp)
    candles = select_x_closed(conn, 'EUR_USD', 'M15', datetime.datetime.utcnow(), 30)
    print(candles)


if __name__=='__main__':
    main()
