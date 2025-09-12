from pyspark.sql import SparkSession
from PropertyConstants import PropertyConstants

spark = (
    SparkSession.builder
        .appName("spark_kafka_consumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
)

BOOTSTRAP_SERVER = PropertyConstants.BOOTSTRAP_SERVER
JAAS_MODULE = PropertyConstants.JAAS_MODULE
CLUSTER_API_KEY = PropertyConstants.CLUSTER_API_KEY
CLUSTER_API_SECRET = PropertyConstants.CLUSTER_API_SECRET

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
