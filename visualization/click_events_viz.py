#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    appName("WebAalyticsApp"). \
    config("spark.ui.port", "0"). \
    getOrCreate()

clicks_df = spark. \
    read. \
    parquet("/Users/akashpatel/Documents/Clairvoyant/dummy/data/")

clicks_df.show(truncate=False)

from pyspark.sql.functions import col, get_json_object

clicks_df = clicks_df. \
    withColumn("x", get_json_object(col("value"), "$.x").cast("long")). \
    withColumn("y", get_json_object(col("value"), "$.y").cast("long")). \
    drop("value")

clicks_df.printSchema()

pandas_df = clicks_df.toPandas()

import plotly.express as px

fig = px.scatter( x=pandas_df["x"], y=pandas_df["y"])

fig.update_yaxes(autorange="reversed")

from PIL import Image
img = Image.open('/Users/akashpatel/Documents/Clairvoyant/ClickStreamWebAnalyticsConsumer/visualization/nice.png')

fig.add_layout_image(
        dict(
            source=img,
            xref="x",
            yref="y",
            x=0,
            y=8,
            sizex=1680,
            sizey=713.94,
            sizing="stretch",
            opacity=0.6,
            layer="below"))


fig.update_layout(template="plotly_white")

fig.show()

spark.stop()