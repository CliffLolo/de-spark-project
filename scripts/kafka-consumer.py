import json
from kafka import KafkaConsumer, KafkaProducer
from s3fs import S3FileSystem
s3 = S3FileSystem()

consumer = KafkaConsumer(
    'store_sales',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for count, i in enumerate(consumer):
    with s3.open("s3://clifflolo-bigspark-store-sales-useast1-dev/store_sales_{}.json".format(count), 'w') as file:
        json.dump(i.value, file)