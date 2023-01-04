import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
from pyspark.sql.functions import rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# This function will select a random row from the DataFrame df using PySpark's rand() and limit functions, convert the row to a dictionary using toPandas() and to_dict(), and then send the dictionary to the Kafka producer using the send() function.
def send_random_stock_info(df):
  random_stock_info = df.orderBy(rand()).limit(1).toPandas().to_dict(orient="records")[0]
  producer.send('store_sales', value=random_stock_info)

# Set the schema for the store_sales table
store_sales_schema = StructType([
  StructField("ss_sold_date_sk",IntegerType(), True),
  StructField("ss_sold_time_sk", IntegerType(), True),
  StructField("ss_item_sk", IntegerType(), True),
  StructField("ss_customer_sk", IntegerType(), True),
  StructField("ss_cdemo_sk", IntegerType(), True),
  StructField("ss_hdemo_sk", IntegerType(), True),
  StructField("ss_addr_sk", IntegerType(), True),
  StructField("ss_store_sk", IntegerType(), True),
  StructField("ss_promo_sk", IntegerType(), True),
  StructField("ss_ticket_number", IntegerType(), True),
  StructField("ss_quantity", IntegerType(), True),
  StructField("ss_wholesale_cost", DoubleType(), True),
  StructField("ss_list_price", DoubleType(), True),
  StructField("ss_sales_price", DoubleType(), True),
  StructField("ss_ext_discount_amt", DoubleType(), True),
  StructField("ss_ext_sales_price", DoubleType(), True),
  StructField("ss_ext_wholesale_cost", DoubleType(), True),
  StructField("ss_ext_list_price", DoubleType(), True),
  StructField("ss_ext_tax", DoubleType(), True),
  StructField("ss_coupon_amt", DoubleType(), True),
  StructField("ss_net_paid", DoubleType(), True),
  StructField("ss_net_paid_inc_tax", DoubleType(), True),
  StructField("ss_net_profit", DoubleType(), True)
])

# Read the store_sales.dat file into a DataFrame
store_sales_path = '/path/to/streaming_data_after_2002/store_sales_2003-01-02.dat'
store_sales_df = spark.read.csv(store_sales_path, header=False, schema=store_sales_schema, sep='|')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

#The loop will run indefinitely, sending a new random stock info every second.
while True:
  send_random_stock_info(store_sales_df)
  sleep(1)