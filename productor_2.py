import datetime
import json
import random
import boto3

STREAM_NAME = "kinesis-Tercer-Parcial"

def get_data():
    return {
        'event_time': datetime.datetime.now().isoformat(),
        'stock': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': random.choices([round(random.random() * 100, 2),round(random.random() * 2, 2),round(random.uniform(10, 200), 2)], weights=[30,30,70])[0]}

def generate(stream_name, kinesis_client):
    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="stock_market2")

if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))
