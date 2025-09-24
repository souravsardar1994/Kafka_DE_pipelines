from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from PropertyConstants import PropertyConstants as pc
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("ED_read_events_from_kafka")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()

)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", pc.BOOTSTRAP_SERVER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            f"{pc.JAAS_MODULE} required username='{pc.CLUSTER_API_KEY}' password='{pc.CLUSTER_API_SECRET}';")
    .option("subscribe", "device-data")
    .option("maxOffsetsPerTrigger", 10)
    .option("startingoffsets", "earliest")
    .load()
)

kafka_df = kafka_df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value")
)

json_schema = (
    StructType(
        [StructField('customerId', StringType(), True),
         StructField('data', StructType(
             [StructField('devices',
                          ArrayType(StructType([
                              StructField('deviceId', StringType(), True),
                              StructField('measure', StringType(), True),
                              StructField('status', StringType(), True),
                              StructField('temperature', LongType(), True)
                          ]), True), True)
              ]), True),
         StructField('eventId', StringType(), True),
         StructField('eventOffset', LongType(), True),
         StructField('eventPublisher', StringType(), True),
         StructField('eventTime', StringType(), True)
         ])
)

kafka_df = (kafka_df.withColumn("value_json",
                                from_json(col('value'), schema=json_schema))

            ).selectExpr("value_json.*")

exploded_df = kafka_df.withColumn("data_devices", explode("data.devices"))
flattened_df = (exploded_df.drop("data")
                .withColumn("deviceId", col("data_devices.deviceId"))
                .withColumn("measure", col("data_devices.measure"))
                .withColumn("status", col("data_devices.status"))
                .withColumn("temperature", col("data_devices.temperature"))
                .drop("data_devices")
                )

(flattened_df
 .writeStream
 .format("memory")
 .queryName("kafka_table")
 .trigger(once=True)
 .outputMode("append")
 .option("checkpointLocation", "output/streaming/checkpoint_kafka_3")
 .start()
 .awaitTermination()
 )

spark.sql("select * from kafka_table").show()
