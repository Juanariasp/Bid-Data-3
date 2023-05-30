import boto3
import json
import numpy as np
from collections import deque

region_name = 'us-east-1'
stream_name = 'kinesis-Tercer-Parcial'
window_size = 20  
num_std_dev = 1  

kinesis = boto3.client('kinesis', region_name=region_name)

price_window = {}

bollinger_upper = None
bollinger_lower = None

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
            
            if len(price_window[stock]) == window_size:
                prices = np.array(price_window[stock])
                sma = np.mean(prices)
                std = np.std(prices)
                bollinger_upper = sma + num_std_dev * std
                if price > bollinger_upper:
                    print(f'!!! ATENCION !!! {stock} superó la franja superior (${round(bollinger_upper,2)} USD) con ${price}')
            
        shard_iterator = response['NextShardIterator']

if __name__ == '__main__':
    consume_bollinger()