import pyspark.sql.functions as F


def with_year_month_day_and_hour(df):
    return df.withColumn("timestamp", F.current_timestamp()) \
        .withColumn("year", F.year("timestamp")) \
        .withColumn("month", F.month("timestamp")) \
        .withColumn("day", F.dayofmonth("timestamp")) \
        .withColumn("hour", F.hour("timestamp"))
