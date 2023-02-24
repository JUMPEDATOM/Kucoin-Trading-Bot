import time
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import ta
import pika
import json
import os
from dotenv import load_dotenv

### Load From Env File If Exist ###
load_dotenv()

candleDataFrame = pd.DataFrame(columns= ['date','open','high','low','close'])

### RabbitMQ Connection ###
connectionParameters = pika.ConnectionParameters('localhost')
MQconnection = pika.BlockingConnection(connectionParameters)
channel = MQconnection.channel()
channel.queue_declare(queue= 'candle')
channel.queue_declare(queue= 'strategy')

def sendMessageToQueue(data):
    channel.basic_publish(
    exchange= '',
    routing_key= 'strategy',
    body= data,
    properties = pika.BasicProperties(
        expiration= os.environ['MQ_EXPIRATION_TIMEOUT'],
        )
    )

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

def candleTimeFrameConverter(record, timeFrame):
    candleData = json.loads(record)
    candleDataFrame.loc[len(candleDataFrame)] = candleData
    candleDataFrame['date'] = pd.to_datetime(candleDataFrame['date'], errors='coerce')
    candleDataFrame.set_index('date', inplace = True)
    ### Resampling... ###
    resamplecandle = candleDataFrame.resample(f'{timeFrame}').agg({
        'open' : 'first',
        'high' : 'max',
        'low' : 'min',
        'close' : 'last'
    })
    candleDataFrame.reset_index(inplace = True)

    return resamplecandle

def strategyOne(df):
    df.reset_index(inplace = True)
    df['close'] = df['close'].astype(float)
    ### Calculate SSL CHANNLE Andicator & TRUE STRENGTH INDEX Andicator ###
    andicatorDataFrame = pd.DataFrame()
    andicatorDataFrame['High_SMA'] = ta.trend.sma_indicator(df.high, window = 10)
    andicatorDataFrame['Low_SMA'] = ta.trend.sma_indicator(df.low, window = 10)
    andicatorDataFrame['TSI'] = ta.momentum.tsi(df.close, window_slow = 25, window_fast = 13)
    andicatorDataFrame['TSI_Signal'] = ta.trend.ema_indicator(andicatorDataFrame.TSI, window = 7)
    andicatorDataFrame = andicatorDataFrame.fillna(0)
    
    return andicatorDataFrame

def insertToDatabase(df):
    ###===== For first time to create Table ====###
    tableOHLCQuery = '''
        CREATE TABLE IF NOT EXISTS STRATEGY1(
        HIGH_SMA FLOAT8 NOT NULL,
        LOW_SMA FLOAT8 NOT NULL,
        TSI FLOAT8 NOT NULL,
        TSI_SIGNAL FLOAT8 NOT NULL
        )
    '''
    pointer.execute(tableOHLCQuery)
    connection.commit()

    pointer.execute('INSERT INTO strategy1 (High_SMA, Low_SMA, TSI, TSI_Signal) VALUES (%s, %s, %s, %s)',
            (df['High_SMA'].iloc[-1], df['Low_SMA'].iloc[-1], df['TSI'].iloc[-1], df['TSI_Signal'].iloc[-1]))
    connection.commit()

    print('Dataframe:', candleDataFrame['date'].iloc[-1], 'inserted!')


def onMessageReceived(ch, method, properties, body):
    # Decoding The Message...
    resampleData = body.decode().replace('\'', '"')
    #Push To DataFrame!
    timeFrameConvertedData = candleTimeFrameConverter(resampleData, '2 min')
    andicatorDataFrame = strategyOne(timeFrameConvertedData)

    insertToDatabase(andicatorDataFrame)
    sendMessageToQueue(str(time.time()))

channel.basic_consume(queue= 'candle', auto_ack= True, on_message_callback= onMessageReceived)
print('<===> CONSUMING STARTED <===>')
channel.start_consuming()