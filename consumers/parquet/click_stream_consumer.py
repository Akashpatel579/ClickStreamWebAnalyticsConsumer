from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object

spark = SparkSession. \
    builder. \
    appName("WebAalyticsApp"). \
    config("spark.ui.port", "0"). \
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

query = clicks_df. \
        writeStream. \
        queryName("clickevents"). \
        format("parquet"). \
        option("path", "/Users/akashpatel/Documents/Clairvoyant/dummy/data"). \
        option("checkpointLocation", "/Users/akashpatel/Documents/Clairvoyant/dummy/cp"). \
        start()


# query = clicks_df. \
#         writeStream. \
#         queryName("clickevents"). \
#         format("jdbc"). \
#         start("jdbc:mysql//localhost:3306/WebAnalytics","jdbc")

query.awaitTermination()