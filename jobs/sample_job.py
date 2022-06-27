import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from structured_streaming.data_transformation.tranformations import with_year_month_day_and_hour
from structured_streaming.storage.write_storage import WriteStorage
from structured_streaming.stream_processor.kafka_consumer import KafkaConsumer

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logging.info("Args:", sys.argv)

    if len(sys.argv) != 6:
        raise Exception(
            'Exactly 5 arguments are required: <BROKERS> <TOPIC> <OUTPUT_PATH> <CHECKPOINT_PATH> <TRIGGER_TIME>. eg: '
            '127.0.0.1:9092 foo gs://path/to/destination/dir gs://path/to/checkpoint/dir \'2 seconds\'')

    spark = (SparkSession.builder
             .appName("sample-job")
             .getOrCreate())

    stream_df = KafkaConsumer(spark) \
        .processor(sys.argv[1], sys.argv[2])

    schema = StructType([
        StructField("username", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])

    lines = stream_df.select(from_json(
        col("value").cast("string"), schema).alias("parsed_value")
    )

    df = lines.select('*')

    target_df = lines.select(
        col("parsed_value.username"),
        col("parsed_value.currency"),
        col("parsed_value.amount")
    )

    transformed_df = with_year_month_day_and_hour(target_df)

    WriteStorage() \
        .write_storage(
        df=transformed_df,
        output_path=sys.argv[3],
        checkpoint_path=sys.argv[4],
        trigger_time=sys.argv[5]
    )

