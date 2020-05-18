# ClickStreamWebAnalyticsConsumer

Run consumer script which can get the data based on the topic and save data as parquet format unsing parquet sink.
###### spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 click_stream_consumer.py 

To generate website screenshot use the webpage_to_image.py selenium script.
The file is available under visualization directory.

Once the data is available we can use any viz tool, I have used Plotly visualization. 
Jupyter notebook file is available under visualization directory. -> click_events_viz.ipynb 

Sample Output : 
![](/visualization/click_events_output.png)

Start Zookeeper Service: <br/>
zookeeper-server-start conf/zoo.cfg 

Start Kafka Server: <br/>
kafka-server-start config/server.properties 

kafka-console-consumer --bootstrap-server localhost:9092 --topic click_events --from-beginning

Start Kafka Consumer: <br/>
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 click_stream_consumer.py 

Start Cassandra Service: <br/>
bin/cassandra <br/>
bin/nodetool status <br/>
bin/cqlsh <br/>

CREATE KEYSPACE webanalytics WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

DESC KEYSPACES;

USE webanalytics;

DESC TABLES;

CREATE TABLE clickevents (
  datetime timestamp,
  value text,
  PRIMARY KEY (datetime)
  
CREATE TABLE webanalytics.clickevents (
    datetime timestamp,
    value text,
    PRIMARY KEY (datetime, value)
) WITH CLUSTERING ORDER BY (value ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';  

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 click_stream_cassandra_consumer.py 

visualization: <br/>
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 click_events_viz.py <br/>
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 click_events_viz_cassandra.py 


