import boto3
import json
import numpy as np
from collections import deque

num_std_dev = 1 
window_size = 20  
region_name = 'us-east-1'
stream_name = 'kinesis-Tercer-Parcial'

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
        prices = np.array(price_window)
        sma = np.mean(prices)
        std = np.std(prices)
        bollinger_lower = sma - num_std_dev * std
        if price < bollinger_lower:
                    print(f'!!! ATENCION !!! {stock} esta por debajo la franja inferior (${round(bollinger_upper,2)} USD) con ${price}')
    return None

if __name__ == '__main__':
    consume_bollinger()
