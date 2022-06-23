from pyspark.sql import DataFrame


class WriteStorage:

    @staticmethod
    def write_storage(dataframe: DataFrame, out_put_path: str, check_point_path: str, trigger_time: str):
        dataframe.writeStream \
            .format("json") \
            .trigger(processingTime=trigger_time) \
            .option("path", out_put_path) \
            .option("checkpointLocation", check_point_path) \
            .start() \
            .awaitTermination()


