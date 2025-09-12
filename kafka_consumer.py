from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("spark_kafka_consumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
)

BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
CLUSTER_API_KEY = "W3DX2NIFXG4EP32Q"
CLUSTER_API_SECRET = "cfltjd7K8aJ0VIo17pPo4OTk6zqctLkObl3LV75atvpiM/jCfxEuv5HLfvm5vZNA"

df = (
    spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f"{JAAS_MODULE} required username='{CLUSTER_API_KEY}' password='{CLUSTER_API_SECRET}';"
        )
        .option("subscribe", "invoices")
        .load()
)

df.show()

# Decode key/value as strings before printing
# from pyspark.sql.functions import col, expr
#
# messages = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
# query = (
#     messages.writeStream
#         .format("console")
#         .outputMode("append")
#         .start()
# )
#
# query.awaitTermination()
