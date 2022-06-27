from pyspark.sql import DataFrame


class WriteStorage:

    @staticmethod
    def write_storage(df: DataFrame, output_path: str, checkpoint_path: str, trigger_time: str):
        df.writeStream \
            .format("parquet") \
            .trigger(processingTime=trigger_time) \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("year", "month", "day", "hour") \
            .start() \
            .awaitTermination()


