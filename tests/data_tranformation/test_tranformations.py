from pyspark.sql.types import StringType, StructType, StructField

from structured_streaming.data_transformation.tranformations import with_clean_user_name
from tests import spark_session


class TestTransformation(object):

    def test_with_clean_user_name(self):
        schema = StructType([
            StructField("user_name", StringType(), True),
            StructField("letter", StringType(), True)
        ])

        source_df = spark_session.createDataFrame(
            [("fons&&eca", "a"), ("<##>jose", "b"), ("!!samuel**", "c")],
            schema=schema
        )
        result_df = with_clean_user_name(source_df)

        expected_schema = StructType([
            StructField("user_name", StringType(), True),
            StructField("letter", StringType(), True),
            StructField("clean_user_name", StringType(), True),
        ])
        expected_df = spark_session.createDataFrame(
            [("fons&&eca", "a", "fonseca"), ("<##>jose", "b", "jose"), ("!!samuel**", "c", "samuel")],
            schema=expected_schema
        )

        assert (expected_df.collect() == result_df.collect())
