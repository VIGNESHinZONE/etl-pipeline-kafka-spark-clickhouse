from sqlalchemy import create_engine, Column, MetaData
from clickhouse_sqlalchemy import (
    Table, make_session, get_declarative_base, types, engines
)
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import func
import datetime as dt
from dateutil import tz


uri = 'clickhouse+native://0.0.0.0:9000'

engine = create_engine(uri)
session = make_session(engine)
metadata = MetaData(bind=engine)

Base = get_declarative_base(metadata=metadata)


class Blocks(Base):
    __tablename__ = 'blocks'
    timestamp = Column(types.Int128)
    number = Column(types.Int128)
    hash = Column(types.String, primary_key=True)
    parent_hash = Column(types.String)
    nonce = Column(types.String)
    sha3_uncles = Column(types.String)
    logs_bloom = Column(types.String)
    transactions_root = Column(types.String)
    state_root = Column(types.String)
    receipts_root = Column(types.String)
    miner = Column(types.String)
    difficulty = Column(types.Int128)
    total_difficulty = Column(types.Int128)
    size = Column(types.Int128)
    extra_data = Column(types.String)
    gas_limit = Column(types.Int128)
    gas_used = Column(types.Int128)
    transaction_count = Column(types.Int128)
    base_fee_per_gas = Column(types.Int128)
    __table_args__ = (
        engines.Memory(),
    )


class TokenTransfer(Base):
    __tablename__ = 'token_transfer'

    token_address = Column(types.String)
    from_address = Column(types.String)
    to_address = Column(types.String)
    value = Column(types.UInt128)
    transaction_hash = Column(types.String, primary_key=True)
    log_index = Column(types.Int128, primary_key=True)
    block_timestamp = Column(types.Int128)
    block_number = Column(types.Int128)
    block_hash = Column(types.String)
    __table_args__ = (
        engines.Memory(),
    )



Blocks.__table__.create()
data = pd.read_parquet('data/user_files/blocks.parquet')
data['difficulty'] = data['difficulty'].apply(int)
data['total_difficulty'] = data['total_difficulty'].apply(int)
data['timestamp'] = data['timestamp'].apply(int)

data = data.to_dict('records')
session.execute(Blocks.__table__.insert(), data)


TokenTransfer.__table__.create()
data = pd.read_parquet('data/user_files/token_tranfers.parquet')
data['value'] = data['value'].apply(int)
data['block_timestamp'] = data['block_timestamp'].apply(int)

data = data.to_dict('records')
session.execute(TokenTransfer.__table__.insert(), data)

