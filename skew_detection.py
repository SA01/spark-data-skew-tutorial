import time

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from common import create_spark_session_with_aqe_disabled, create_spark_session_with_aqe_skew_join_enabled, \
    create_spark_session_with_aqe_enabled
from data_prep import prepare_trips_data


def trips_data_exploration(spark: SparkSession):
    trips_data = prepare_trips_data(spark=spark)
    trips_data.show(truncate=False)

    trips_data\
        .groupBy("PULocationID")\
        .agg(F.count(F.lit(1)).alias("num_rows")).sort(F.col("num_rows").desc())\
        .show(truncate=False, n=100)

    location_details_data = spark.read.option("header", True).csv("data/taxi+_zone_lookup.csv")
    location_details_data.show(truncate=False)


def join_on_skewed_data(spark: SparkSession):
    trips_data = prepare_trips_data(spark=spark)
    location_details_data = spark.read.option("header", True).csv("data/taxi+_zone_lookup.csv")

    trips_with_pickup_location_details = trips_data\
        .join(location_details_data, F.col("PULocationID") == F.col("LocationID"), "inner")

    trips_with_pickup_location_details \
        .groupBy("Zone") \
        .agg(F.avg("trip_distance").alias("avg_trip_distance")) \
        .sort(F.col("avg_trip_distance").desc()) \
        .show(truncate=False, n=1000)

    trips_with_pickup_location_details \
        .groupBy("Borough") \
        .agg(F.avg("trip_distance").alias("avg_trip_distance")) \
        .sort(F.col("avg_trip_distance").desc()) \
        .show(truncate=False, n=1000)


def join_on_skewed_data_with_subsplit(spark: SparkSession):
    subsplit_partitions = 25
    trips_data = prepare_trips_data(spark=spark)\
        .withColumn("dom_mod", F.dayofyear(F.col("tpep_pickup_datetime"))).withColumn("dom_mod", F.col("dom_mod") % subsplit_partitions)

    location_details_data = spark\
        .read\
        .option("header", True)\
        .csv("data/taxi+_zone_lookup.csv")\
        .withColumn("location_id_alt", F.array([F.lit(num) for num in range(0,subsplit_partitions)])) \
        .withColumn("location_id_alt", F.explode(F.col("location_id_alt")))

    trips_with_pickup_location_details = trips_data\
        .join(
            location_details_data,
            (F.col("PULocationID") == F.col("LocationID")) & (F.col("location_id_alt") == F.col("dom_mod")),
            "inner"
        )

    trips_with_pickup_location_details \
        .groupBy("Zone") \
        .agg(F.avg("trip_distance").alias("avg_trip_distance")) \
        .sort(F.col("avg_trip_distance").desc()) \
        .show(truncate=False, n=1000)

    trips_with_pickup_location_details \
        .groupBy("Borough") \
        .agg(F.avg("trip_distance").alias("avg_trip_distance")) \
        .sort(F.col("avg_trip_distance").desc()) \
        .show(truncate=False, n=1000)


if __name__ == '__main__':
    start_time = time.time()
    spark = create_spark_session_with_aqe_disabled()
    # spark = create_spark_session_with_aqe_skew_join_enabled()
    # spark = create_spark_session_with_aqe_enabled()

    trips_data_exploration(spark=spark)

    # join_on_skewed_data(spark=spark)
    # join_on_skewed_data_with_subsplit(spark=spark)

    print(f"Elapsed_time: {(time.time() - start_time)} seconds")
    time.sleep(10000)
