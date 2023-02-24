import json
import websocket
import requests
import time
import datetime
import psycopg2
import pika
import os
from dotenv import load_dotenv

### Load From Env File If Exist ###
load_dotenv()

### RabbitMQ Connection ###
credentials = pika.PlainCredentials(os.environ['MQ_USERNAME'], os.environ['MQ_PASSWORD'])
MQconnection = pika.BlockingConnection(pika.ConnectionParameters(
    host = os.environ['MQ_HOST'],
    port = os.environ['MQ_PORT'],
    virtual_host = os.environ['MQ_VHOST'],
    credentials = credentials))

channel = MQconnection.channel()

channel.queue_declare(queue='candle')

def sendMessageToQueue(data):
    channel.basic_publish(
    exchange= '',
    routing_key= 'candle',
    body= data
)

baseUrl = 'https://api.kucoin.com'
data = []
minutesProcessed = {}
minuteCandlestick = []
currentTicker = None
previousTicker = None


#### Connecting To DB ###
connection = psycopg2.connect(
    host = os.environ['DB_HOST'],
    database = os.environ['DB_NAME'],
    user = os.environ['DB_USERNAME'],
    password = os.environ['DB_PASSWORD'],
    port = os.environ['DB_PORT']
)
### Create vessel for connecting python and DB
pointer = connection.cursor()

### For first time to create Table ###
tableOHLCQuery = '''
    CREATE TABLE IF NOT EXISTS OHLC(
        ID BIGSERIAL PRIMARY KEY NOT NULL,
        DATE TIMESTAMP NOT NULL,
        OPEN FLOAT8 NOT NULL,
        HIGH FLOAT8 NOT NULL,
        LOW FLOAT8 NOT NULL,
        CLOSE FLOAT8 NOT NULL
    )
'''
pointer.execute(tableOHLCQuery)
connection.commit()


def applyConnectToken(baseUrl):
    endPoint = '/api/v1/bullet-public'
    url = '%s%s' % (baseUrl, endPoint)
    response = requests.post(url).json()['data']

    return response['token'], response['instanceServers'][0]['endpoint'], response['instanceServers'][0]['pingInterval'], response['instanceServers'][0]['pingTimeout']

def createConnection(endPoint, token):
    now = int(time.time() * 1000)
    socket = f'{endPoint}?token={token}&[connectId={now}]'
    
    return socket    

def onOpen(ws):
    print('<===> CONNECTION IS ESTABLISHED <===>')
    
    now = int(time.time() * 1000)
    subscribeMessage = {
        'id' : now,
        'type' : 'subscribe',
        'topic' : '/market/candles:BTC-USDT_1min',
        'privateChannel' : False,
        'response' : True
    }
    ws.send(json.dumps(subscribeMessage))

def onMessage(ws, message):
    candlestickProcess(message)

def onPing(ws):
    now = int(time.time() * 1000)
    pingMessage = {
        'id' : now,
        'type' : 'ping'
    }
    ws.send(json.dumps(pingMessage))

def onClose(ws):
    connection.close()

def candlestickProcess(message):
    global currentTicker, previousTicker

    previousTicker = currentTicker
    currentTicker = json.loads(message)
    timeStamp = datetime.datetime.fromtimestamp(int(currentTicker['data']['candles'][0]))

    if not timeStamp in minutesProcessed:
        minutesProcessed[timeStamp] = True

        if len(minuteCandlestick) > 0:
            minuteCandlestick[-1]['close'] = previousTicker['data']['candles'][2]

        minuteCandlestick.append({
            'minute' : timeStamp,
            'open' : currentTicker['data']['candles'][2],
            'high' : currentTicker['data']['candles'][2],
            'low' : currentTicker['data']['candles'][2],
        })

        #### Inserting To POSTGRESSQL ####
        insertToDataBase(minuteCandlestick[:-1])

    if len(minuteCandlestick) > 0:
        currentCandlestick = minuteCandlestick[-1]
        if currentTicker['data']['candles'][2] > currentCandlestick['high']:
            currentCandlestick['high'] = currentTicker['data']['candles'][2]
        if currentTicker['data']['candles'][2] < currentCandlestick['low']:
            currentCandlestick['low'] = currentTicker['data']['candles'][2]
    
def insertToDataBase(candlestick):   
    MQcandle = {
        'date' : str(candlestick[-1]['minute']),
        'open' : candlestick[-1]['open'],
        'high' : candlestick[-1]['high'],
        'low' : candlestick[-1]['low'],
        'close' : candlestick[-1]['close'],
    }
    sendMessageToQueue(str(MQcandle))

    pointer.execute('INSERT INTO OHLC (DATE, OPEN, HIGH, LOW, CLOSE) VALUES (%s, %s, %s, %s, %s)',
            (candlestick[-1]['minute'], candlestick[-1]['open'], candlestick[-1]['high'], candlestick[-1]['low'], candlestick[-1]['close']))
    connection.commit()
    print('Candle time:', candlestick[-1]['minute'], 'inserted!')
    

token, endPoint, ping, pingTimeOut = applyConnectToken(baseUrl)

ping = int(ping / 1000) - 2
pingTimeOut = int(pingTimeOut / 1000) - 2

socket = createConnection(endPoint, token)

ws = websocket.WebSocketApp(socket,
                            on_open = onOpen,
                            on_message = onMessage,
                            on_ping = onPing,
                            on_close= onClose)

ws.run_forever(ping_interval = ping, ping_timeout = pingTimeOut)

    