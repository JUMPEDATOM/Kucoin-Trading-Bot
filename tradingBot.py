import datetime
import psycopg2
import pandas as pd
import pika
import requests
import time
import hashlib
import hmac
import base64
import json
import os
from dotenv import load_dotenv

### Load From Env File If Exist ###
load_dotenv()

baseUrl = 'https://openapi-sandbox.kucoin.com'
inPosition = False

### Open Kucoin Api Key ###
kucoinApiKey = os.environ['KUCOIN_APIKEY']
kucoinApiSecret = os.environ['KUCOIN_API_SECRET']
kucoinApiPasspharse = os.environ['KUCOIN_API_PASSPHARSE']

### RabbitMQ Connection ###
connectionParameters = pika.ConnectionParameters('localhost')
MQconnection = pika.BlockingConnection(connectionParameters)
channel = MQconnection.channel()
channel.queue_declare(queue= 'strategy')

### load from DataBase ###
connection = psycopg2.connect(
    host = os.environ['DB_HOST'],
    database = os.environ['DB_NAME'],
    user = os.environ['DB_USERNAME'],
    password = os.environ['DB_PASSWORD'],
    port = os.environ['DB_PORT']
)
pointer = connection.cursor()


def authentication(endPoint, method, bodyStr = ''):
    now = int(time.time() * 1000)
    strToSign = str(now) + method.upper() + endPoint + bodyStr
    signature = base64.b64encode(
        hmac.new(kucoinApiSecret.encode('utf-8'), strToSign.encode('utf-8'), hashlib.sha256).digest())
    passphrase = base64.b64encode(hmac.new(kucoinApiSecret.encode('utf-8'), kucoinApiPasspharse.encode('utf-8'), hashlib.sha256).digest())
    headers = {
        "KC-API-SIGN": signature,
        "KC-API-TIMESTAMP": str(now),
        "KC-API-KEY": kucoinApiKey,
        "KC-API-PASSPHRASE": passphrase,
        "KC-API-KEY-VERSION": "2",
        "Content-Type": "application/json"
    }
    return headers

def privatePostingApi(endPoint, body):
    bodyStr = json.dumps(body) #convert to json format
    headers = authentication(endPoint, 'post', bodyStr)
    url = '%s%s' % (baseUrl, endPoint)
    response = requests.post(url, headers = headers, data = bodyStr).json()
    return response 

def privateGettingApi(orderId):
    endPoint = '/api/v1/orders/' + f'{orderId}'
    headers = authentication(endPoint, 'get')
    url = '%s%s' % (baseUrl, endPoint)
    response = requests.get(url, headers = headers)
    quantity = response.json()['data']['dealSize']

    return quantity 

def pullingDataFromDB():
    pointer.execute('SELECT * FROM STRATEGY1 OFFSET ((SELECT COUNT(*) FROM STRATEGY1)-1)')
    lastRow = pointer.fetchall()
    
    return lastRow         

def makeOrder(transaction, quantity):
    endPoint = '/api/v1/orders'
    body = {
        'clientOid' : str(int(time.time() * 1000)),
        'side' : transaction,
        'symbol' : 'BTC-USDT',
        'type' : 'market',
        'size' : quantity
    }
    result = privatePostingApi(endPoint, body)

    return result

def traderBot(record):
    global inPosition, orderQuantity

    df = pd.DataFrame(record, columns= ['High-Sma', 'Low-Sma', 'Tsi', 'Tsi-Signal'])
    df['Low-Sma'] = df['Low-Sma'].astype(float)
    df['High-Sma'] = df['High-Sma'].astype(float)

    print(
            'Order Instruction:', '\n'
            'time:', datetime.datetime.now(), '\n'
            'Low Sma:', df['Low-Sma'].to_string(index= False), '\n'
            'High Sma:',  df['High-Sma'].to_string(index= False)
        )

    if not inPosition and df['Low-Sma'].values[0] > df['High-Sma'].values[0]:
        if df['Tsi'].values[0] > df['Tsi-Signal'].values[0]:
            order = makeOrder('buy', 1000)
            orderQuantity = privateGettingApi(order['data']['orderId'])
            inPosition = True

    if inPosition and df['Low-Sma'].values[0] < df['High-Sma'].values[0]:
        if df['Tsi'].values[0] < df['Tsi-Signal'].values[0]:
            order = makeOrder('sell', orderQuantity)
            inPosition = False

    else:
        print('The Situation Is Not Suitable Right Now!')
    
    print()


def onMessageReceived(ch, method, properties, body):
    record = pullingDataFromDB()
    traderBot(record)


channel.basic_consume(queue= 'strategy', auto_ack= True, on_message_callback= onMessageReceived)
print('<===> CONSUMING STARTED <===>')
channel.start_consuming()