Start Zookeper - 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka - 
```
bin/kafka-server-start.sh config/server.properties
```

Create Topics - 

```
bin/kafka-topics.sh --create --topic blocks_kafka --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

Delete Topics - 
```
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic blocks_kafka
```

Consume console - 

```
bin/kafka-console-consumer.sh --topic blocks_kafka --bootstrap-server localhost:9092
```


Spark-submit command for streaming - 

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 read_kafka.py
```