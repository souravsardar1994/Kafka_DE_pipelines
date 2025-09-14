from PropertyConstants import PropertyConstants
from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql.functions import cast, from_json


class Ingestor():
    def __init__(self, spark):
        self.base_data_dir = PropertyConstants.base_data_dir
        self.BOOTSTRAP_SERVER = PropertyConstants.BOOTSTRAP_SERVER
        self.JAAS_MODULE = PropertyConstants.JAAS_MODULE
        self.CLUSTER_API_KEY = PropertyConstants.CLUSTER_API_KEY
        self.CLUSTER_API_SECRET = PropertyConstants.CLUSTER_API_SECRET
        self.spark = spark

    def getSchema(self):
        return PropertyConstants.schema

    def ingestFromKafka(self, spark , startingTime):
        df = (

            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config",
                    f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';")
            .option("subscribe", "invoices")
            .option("maxOffsetsPerTrigger", 10)
            .option("startingTimestamp", startingTime)
            .load()
        )

        return df

    def getInvoices(self, kafka_df: DataFrame):
        return kafka_df.select(
            kafka_df.key.cast("string").alias("key"),
            from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"), "topic", "timestamp"
        )

    def process(self, startingTime=1):
        print("starting bronze stream....", end='')
        raw_df = self.ingestFromKafka(self.spark, startingTime)
        kafka_df = self.getInvoices(raw_df)
        sQuery = (
            kafka_df.writeStream
            .queryName("bronze-ingestion")
            .option("checkpointLocation", PropertyConstants.check_point_path)
            .outputMode("append")
            .toTable("invoices_final")
        )

        sQuery.awaitTermination()
        print("write completed")


if __name__ == '__main__':
    spark = (
        SparkSession.builder
        .appName("spark_kafka_consumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )
    ing = Ingestor(spark)
    ing.process()
