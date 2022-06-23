import pyspark.sql.functions as F
import quinn


def with_clean_user_name(df):
    return df.withColumn(
        "clean_username",
        quinn.remove_non_word_characters(F.col("username"))
    )
