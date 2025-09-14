from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession
    .builder
    .appName("Streaming Process Files")
    .config("spark.streaming.stopGracefullyOnShutdown", "True")
    .master("local[*]")
    .getOrCreate()
)

spark.conf.set("spark.sql.streaming.schemaInference", True)

streaming_df = (
    spark
    .readStream
    .option("cleanSource", "archive")
    .option("sourceArchiveDir", "archive_dir")
    .option("maxFilesPerTrigger", 1)
    .format("json")
    .load("devices_data/")
)

# streaming_df.show(truncate=False)

exploded_df = streaming_df.withColumn("data_devices", explode("data.devices")).drop("data")
# exploded_df.show(truncate=False)

flattened_df = (exploded_df.withColumn("deviceId", col("data_devices.deviceId"))
                .withColumn("measure", col("data_devices.measure"))
                .withColumn("status", col("data_devices.status"))
                .withColumn("temperature", col("data_devices.temperature"))
                ).drop("data_devices")

# flattened_df.show(truncate=False)

# streaming_df.show(truncate=False)
flattened_df.printSchema()

(flattened_df
 .writeStream
 .format("csv")
 .outputMode("append")
 .option("path", "output/device_data.csv")
 .option("checkpointLocation", "checkpoint_dir")
 .start()
 .awaitTermination()
 )
