from datetime import datetime

from pyspark.sql.types import StringType, StructType, StructField, TimestampType, DoubleType, IntegerType

from structured_streaming.data_transformation.tranformations import with_year_month_day_and_hour
from tests import spark_session


class TestTransformation(object):

    def test_with_timestamp(self):
        schema = StructType([
            StructField("username", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])

        source_df = spark_session.createDataFrame(
            [("jonesdarren", "LYD", 98.15),
             ("hudsonjon", "AED", 1502.05)],
            schema=schema
        )

        result_df = with_year_month_day_and_hour(source_df)

        expected_schema = StructType([
            StructField("username", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("hour", IntegerType(), True),
        ])

        dt = datetime.now()

        expected_df = spark_session.createDataFrame(
            [
                (
                    "jonesdarren", "LYD"
                    , 98.15
                    , dt
                    , int(dt.strftime("%Y"))
                    , int(dt.strftime("%m"))
                    , int(dt.strftime("%d"))
                    , int(dt.strftime("%H"))
                ),
                (
                    "hudsonjon"
                    , "AED"
                    , 1502.05
                    , dt
                    , int(dt.strftime("%Y"))
                    , int(dt.strftime("%m"))
                    , int(dt.strftime("%d"))
                    , int(dt.strftime("%H"))
                )
            ],
            schema=expected_schema
        )

        assert (expected_df.collect() == result_df.collect())
