import sys
import math
from pyspark.sql.types import DoubleType, DateType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, mean, col, date_format

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate haversine distance between two geographical point
    :param lat1: Double
    :param lon1: Double
    :param lat2: Double
    :param lon2: Double
    :return:
    distance in meters
    """
    R = 6372800  # Earth radius in meters

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def compute_speed(lat1, lon1, lat2, lon2, duration):
    distance = haversine(lat1, lon1, lat2, lon2) / 1000 # km
    duration = duration / 3600 # hour

    return distance / duration


class SparkComputation:

    def __init__(self):
        spark = SparkSession.builder.getOrCreate()
        self.df = spark.read.csv('train.csv', inferSchema=True, header=True)

    def get_avg_speed(self):
        compute_speed_udf = udf(compute_speed, DoubleType())
        return self.df.withColumn('avg_speed',
                      compute_speed_udf('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude',
                                        'trip_duration')) \
            .select(mean(col("avg_speed")).alias('avg_speed')) \
            .collect()

    def get_ride_by_day_of_week(self):
        return self.df.withColumn('week_day', date_format(col("pickup_datetime"), "E"))\
            .groupby('week_day')\
            .count()\
            .collect()

    def get_ride_by_hour_of_day(self):
        hour_of_day = udf(lambda x: int(x/4), IntegerType())
        return self.df.withColumn('hour', hour_of_day(date_format(col("pickup_datetime"), "H").cast(IntegerType())))\
            .groupby('hour')\
            .count()\
            .collect()

    def get_km_by_day_of_week(self):
        compute_distance = udf(haversine, DoubleType())
        return self.df.withColumn('distance', compute_distance('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude'))\
            .withColumn('week_day', date_format(col("pickup_datetime"), "E")) \
            .groupby('week_day')\
            .sum('distance')\
            .collect()
