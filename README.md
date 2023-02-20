# etl-pipeline-kafka-spark-clickhouse

[Video Demonstration](https://drive.google.com/file/d/1Cx4BZh_Ee3KwHMerFtC1N8Mzqq4kO8Hh/view?usp=sharing)

Follow the steps to create the initial environment - 

1. Run this command to start the clickhouse DB
```
docker run --name arda_clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest
```

2. run this command to start a python virtual environment - 
```
python -m venv etl-venv
source etl-venv/bin/activate
pip install -r requirements.txt
```

3. Run this command, to convert raw csv files from `ethereum-etl` to optimized parquet file formats
```
python utils/raw_csv_converter.py
```

4. Load the block and token data into clickhouse DB
```
python db.py
```

5. Open http://localhost:8123/play on browser as columnar store for adhoc analysis
Tables - `blocks` and `token_transfer` are available for exploration



# Ad-hoc analysis

5. Follow the commands for adhoc-analysis

    Q1. How many ERC-20 token contracts were found? 

        ```
        select count(distinct(token_address)) from token_transfer;
        ```

    Q2. Current balance for any token address?

        ```
        with t as (
            select 
                from_address as address, 
                -sum(value) as total_value 
            from token_transfer 
            group by from_address
            UNION ALL
            select 
                to_address as address, 
                sum(value) as total_value 
            from token_transfer 
            group by to_address)

        select 
            address, sum(total_value) as value 
        from t group by address
        ```
    
    Q3. Highest transaction in a block

        ```
        select block_number, max(value) from token_transfer group by block_number
        ```


# Streaming Pipeline


Start a kafka broker and create two topics - `blocks_kafka` and `tokens_kafka`. Follow [kafka-setup.md](./kafka-setup.md)

1. Lets do a manual Streaming of `blocks` and `tokens` by running the two commands in seperate terminal console - 

    Terminal 1 - 
    `python manual_streaming/streaming_blocks.py`

    Terminal 2 - 
    `python manual_streaming/streaming_tokens.py`

2. Follow the commands to run the spark-jobs to analyse the real-time stream.

    Q1. The moving average of the number of transactions in a block 

        Output will shown on the console directly - 
        ```
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark-jobs/mean_transaction.py
        ```

    
    Q2. Total value of gas every hour

        Output will shown on the console directly - 
        ```
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark-jobs/total_gas.py
        ```


    Q3. Running count of number of transfers sent and received by addresses
        
        Since the size is a bit huge, complete outputs will be streamed to a folder - `streaming_output/`  

        Run this to get count of total addresses sent, output in `streaming_output/sent_count`  
        ```
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark-jobs/from_address_running_count.py
        ```

        Run this to get count of total addresses recieved, output in `streaming_output/recieved_count`  
        ```
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark-jobs/to_address_running_count.py
        ```







