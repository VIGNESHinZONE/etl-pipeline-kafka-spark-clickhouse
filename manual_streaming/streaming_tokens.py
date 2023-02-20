from kafka import KafkaProducer
import pandas as pd
import time
from json import dumps
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
tokens = pd.read_parquet("data/user_files/token_tranfers.parquet")
tokens['value'] = tokens['value'].apply(int)
tokens['block_timestamp'] = tokens['block_timestamp'].apply(int)

tokens = tokens[['from_address', 'to_address', 'block_timestamp', 'transaction_hash', 'log_index']]
tokens = tokens.to_dict('records')

for i, token in enumerate(tokens):
    producer.send('tokens_kafka', token)
    time.sleep(15)
    if i%100 == 0:
        print(f"Finished {i / len(token)} % percent of data")