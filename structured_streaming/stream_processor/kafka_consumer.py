

from pyspark.sql import SparkSession, DataFrame


class KafkaConsumer:

    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def processor(self, brokers: str, topic: str) -> DataFrame:
        return self.spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "100000") \
            .option("subscribe", topic) \
            .load()


