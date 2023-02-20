import pandas as pd
import numpy as np


#### Blocks csv to parquet ########
block = pd.read_csv("data/raw_csv/blocks.csv")
# block['timestamp'] = pd.to_datetime(block['timestamp'], unit='s')
string_keys = [
    'hash', 'parent_hash', 
    'nonce', 'sha3_uncles', 'logs_bloom', 
    'transactions_root', 'state_root', 
    'receipts_root', 'miner', 'extra_data'
]
for k in string_keys:
    if block[k].dtype == object:
        block[k] = block[k].astype("string")
keys = [
    'timestamp', 'number', 'hash', 'parent_hash', 
    'nonce', 'sha3_uncles', 'logs_bloom', 
    'transactions_root', 'state_root', 
    'receipts_root', 'miner', 'difficulty', 
    'total_difficulty', 'size', 'extra_data', 
    'gas_limit', 'gas_used', 'transaction_count', 
    'base_fee_per_gas'
]

block = block[keys]
block.to_parquet("data/user_files/blocks.parquet", index=False)



#### Token Transfer csv to parquet ########
tok = pd.read_csv("data/raw_csv/token_transfers.csv")
block_temp = block[['timestamp', 'number', 'hash']]
block_temp = block_temp.rename(
    columns={
        "number": "block_number", 
        "timestamp": "block_timestamp", 
        "hash": "block_hash"
    }
)
tok = pd.merge(
    tok,
    block_temp,
    on=['block_number']
)
string_keys = [
    'token_address', 'from_address', 'to_address',
    'transaction_hash', 'block_hash'
]
for k in string_keys:
    if tok[k].dtype == object:
        tok[k] = tok[k].astype("string")


tok.to_parquet("data/user_files/token_tranfers.parquet", index=False)

