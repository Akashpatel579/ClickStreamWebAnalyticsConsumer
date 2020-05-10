from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    appName("WebAalyticsApp"). \
    config("spark.ui.port", "0"). \
    config('spark.cassandra.connection.host', '127.0.0.1'). \
    getOrCreate()

df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="clickevents", keyspace="webanalytics").load()

df.show()



# def writeToCassandra(df, epochId):
#   df.write \
#     .format("org.apache.spark.sql.cassandra") \
#     .options(table="clickevents", keyspace="webanalytics") \
#     .save()
#
# spark.conf.set("spark.sql.shuffle.partitions", "1")
#
# query = (
#   clicks_df \
#     .writeStream
#     .foreachBatch(writeToCassandra)
#     .outputMode("append")
#     .start()
#     )