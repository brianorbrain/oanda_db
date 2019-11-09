import requests
import db_connect_trade
import db_connect
from datetime import datetime


timeframes= ['M1', 'M5', 'M15', 'H1']

apiGateway = 'https://api-fxtrade.oanda.com'
apiToken = {'Authorization' : 'Bearer '}
accountNumber = ''

def get_candles(pair, timeframe, connection, params):
    url = apiGateway + '/v3/instruments/' + pair + '/candles'
    params['dailyAlignment'] = 0
    params['alignmentTimezone'] = 'UTC'
    candle = connection.get(url, headers = apiToken, params = params).json()
    for row in candle['candles']:
        row.update(row.pop('mid'))
        row.update({'symbol':pair, 'timeframe':params['granularity']})
    return candle['candles']


def update_candles(conn, web_connect):
    candle_list = []
    for pair in db_connect.pairs:
        for timeframe in timeframes:
            ##print(pair, timeframe)
            while True:
                last_day = db_connect_trade.most_recent(pair, timeframe, conn)
                ##print(last_day)
                if last_day == None:
                    params = {'granularity':timeframe, 'count': '250'}
                else: params = {'from':last_day[0], 'granularity':timeframe, 'count': '250'}
                candles = get_candles(pair, timeframe, web_connect, params)
                db_connect_trade.candle_insert(conn, candles)
                if len(candles) < 250:
                    break
##            candle_count = db_connect_trade.select_count_candles(conn, pair, timeframe)
##            delete_amount = candle_count[0] - 250
##            db_connect_trade.delete_candle_count(conn, pair, timeframe, delete_amount)


    
def main():
    
    web_connect = requests.Session()
    conn = db_connect_trade.db_connection(remote = False)
    start = datetime.now()
    update_candles(conn, web_connect)
    ##print(datetime.now()-start)
    


if __name__ == '__main__':
    main()
