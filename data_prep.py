from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def prepare_trips_data(spark: SparkSession) -> DataFrame:
    pu_loc_to_change = [
        236, 132, 161, 186, 142, 141, 48, 239, 170, 162, 230, 163, 79, 234, 263, 140, 238, 107, 68, 138, 229, 249,
        237, 164, 90, 43, 100, 246, 231, 262, 113, 233, 143, 137, 114, 264, 148, 151
    ]

    res_df = spark.read\
        .parquet("data/trips/*.parquet")\
        .withColumn(
            "PULocationID",
            F.when(F.col("PULocationID").isin(pu_loc_to_change), F.lit(237))
            .otherwise(F.col("PULocationID"))
        )
    return res_df


