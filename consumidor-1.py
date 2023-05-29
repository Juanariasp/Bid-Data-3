import boto3
import json
import numpy as np
from collections import deque

region_name = 'us-east-1'
stream_name = 'parcial3'
window_size = 20  # Tamaño de la ventana
num_std_dev = 1  # Número de desviaciones estándar

kinesis = boto3.client('kinesis', region_name=region_name)

price_window = {}

def consume_bollinger():
    shard_iterator = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId='shardId-000000000001',
        ShardIteratorType='LATEST'
    )['ShardIterator']
    
    while True:
        response = kinesis.get_records(
            ShardIterator=shard_iterator,
            Limit=100
        )
        
        for record in response['Records']:
            action_data = json.loads(record['Data'])
            stock = action_data["stock"]
            price = action_data['price']

            if stock not in price_window.keys():
                price_window[stock] = deque(maxlen=window_size)
            
            price_window[stock].append(price)
            
            ans = check_bollinger(price_window[stock], price = price, stock = stock)
            print(ans) if ans != None else ans
            
        shard_iterator = response['NextShardIterator']

def check_bollinger(price_window, price, stock):
    if len(price_window) == window_size:
        # Calculo de la franja inferior
        prices = np.array(price_window)
        sma = np.mean(prices)
        std = np.std(prices)
        bollinger_lower = sma - num_std_dev * std
        if price < bollinger_lower:
                    print(f'ALERTA: Precio de acción {stock} está por debajo de la franja inferior de Bollinger (${round(bollinger_lower,2)} USD) con ${price}')
    return None

# Ejecución del consumidor
if __name__ == '__main__':
    consume_bollinger()
