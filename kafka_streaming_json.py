#kafka_streaming_json_demo.py
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_streaming_json.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "data-topic"
Bootstrap_server_name = 'localhost:9092'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from data-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Bootstrap_server_name) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    #orders_df3.show()

    print("Printing Schema of message/event: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")
    print("printing schema df1")
    orders_df1.printSchema()   # |-- value: string (nullable = true)
                               # |-- timestamp: timestamp (nullable = true)


    #Define a schema for the orders data
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("costomer_id", StringType()) \
        .add("costomer_names", StringType()) \
        .add("product_id", StringType()) \
        .add("product_name", StringType()) \
        .add("product_category", StringType()) \
        .add("payment_type", StringType()) \
        .add("quantity", StringType()) \
        .add("price", StringType()) \
        .add("datetime", StringType()) \
        .add("order_country_name", StringType()) \
        .add("order_city_name", StringType()) \
        .add("payment_txn_id", StringType()) \
        .add("payment_txn_success", StringType()) \
        .add("order_ecommerce_website_name", StringType())

    # Message:  {'order_id': 19, 'costomer_id': 102, 'costomer_names': 'William', 'product_id': 218, 
    # 'product_name': 'Sonata', 'product_category': 'Electrical', 'payment_type': 'UPI', 'quantity': 2.27, 
    # 'price': 339.27, 'datetime': '2021-09-10 23:36:00', 'order_country_name': 'India', 'order_city_name': 'Chennai', 
    # 'order_ecommerce_website_name': 'www.snapdeal.com', 'payment_txn_id': 73635, 'payment_txn_success': 'failed'}
    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema)\
        .alias("orders"), "timestamp")
    # printing schema of message
    print("df2 schema")
    orders_df2.printSchema()

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    # aggregate - finding total_order_amount by grouping country, city
    orders_df4 = orders_df3.withWatermark("timestamp", "2 seconds") \
        .groupBy("order_country_name", "order_city_name", window("timestamp","2 seconds")) \
        .agg(sum("price").alias("total_sum"),count("order_id").alias("order_count")) \
        .select("order_country_name", "order_city_name","order_count","total_sum") \

    # orders=orders_df3.select("costomer_names","order_city_name","price","order_ecommerce_website_name","timestamp")
    # name_stream=orders \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false") \
    #     .format("console") \
    #     .start()
    # name_stream.awaitTermination()

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()

    result=orders_df4.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path","/home/ankit/folder/parquet") \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "/home/ankit/folder/parquet") \
        .start()
    #result.awaitTermination()

    # Writing final result to console
    orders_agg_write_stream = orders_df4 \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()
        # .writeStream \
        # .format("kafka") \
        # .option("kafka.bootstrap.servers", Bootstrap_server_name) \
        # .option("topic", "output") \
        # .trigger(processingTime='1 seconds') \
        # .outputMode("update") \
        # .option("checkpointLocation","/home/ankit/folder") \
        # .start()

    orders_agg_write_stream.awaitTermination()

    print("processing done closing spark application.")
	
