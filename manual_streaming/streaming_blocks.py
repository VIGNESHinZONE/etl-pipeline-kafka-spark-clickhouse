from kafka import KafkaProducer
import pandas as pd
import time
from json import dumps
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
blocks = pd.read_parquet("data/user_files/blocks.parquet")
blocks['timestamp'] = blocks['timestamp'].apply(int)
blocks['difficulty'] = blocks['difficulty'].apply(int)
blocks['total_difficulty'] = blocks['total_difficulty'].apply(int)

blocks = blocks[['timestamp', 'number', 'transaction_count', 'gas_used']]
blocks = blocks.to_dict('records')

for i, block in enumerate(blocks):
    producer.send('blocks_kafka', block)
    time.sleep(15)
    if i%100 == 0:
        print(f"Finished {i / len(blocks)} % percent of data")