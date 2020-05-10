# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 click_stream_cassandra_consumer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object, current_timestamp

spark = SparkSession. \
    builder. \
    appName("WebAalyticsApp"). \
    config("spark.ui.port", "0"). \
    config('spark.cassandra.connection.host', '127.0.0.1'). \
    getOrCreate()

clicks_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "click_events") \
    .option("startingOffsets", "latest") \
    .option("inferSchema", "true") \
    .load() \
    .selectExpr("CAST(value AS STRING)").alias("json_data")  # can remove alias


# query = clicks_df. \
#     writeStream. \
#     trigger(processingTime='10 seconds'). \
#     format("console"). \
#     start()

# query = clicks_df. \
#     writeStream. \
#     queryName("clickevents"). \
#     format("memory"). \
#     outputMode("append"). \
#     start()


# query = clicks_df. \
#     writeStream. \
#     queryName("clickeventscasssandra"). \
#     foreach(CassandraSinkForeach.ForEachWriter()). \
#     start()


def writeToCassandra(df, epochId):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table = "clickevents", keyspace = "webanalytics") \
        .save()


spark.conf.set("spark.sql.shuffle.partitions", "1")

query = (
    clicks_df \
        .withColumn("datetime", current_timestamp())
        .writeStream
        .foreachBatch(writeToCassandra)
        .outputMode("append")
        .start()
    )

# query_cassandra = clicks_df.writeStream\
#                       .format("org.apache.spark.sql.cassandra")\
#                       .option("keyspace", "webanalytics")\
#                       .option("table", "clickevents")\
#                       .start()

# query_cassandra = clicks_df.writeStream\
#                       .cassandraFormat("clickevents", "webanalytics")\
#                       .start()

# query = clicks_df. \
#         writeStream. \
#         queryName("clickevents"). \
#         format("parquet"). \
#         option("path", "/Users/akashpatel/Documents/Clairvoyant/dummy/data"). \
#         option("checkpointLocation", "/Users/akashpatel/Documents/Clairvoyant/dummy/cp"). \
#         start()


# query = clicks_df. \
#         writeStream. \
#         queryName("clickevents"). \
#         format("jdbc"). \
#         start("jdbc:mysql//localhost:3306/WebAnalytics","jdbc")

query.awaitTermination()
