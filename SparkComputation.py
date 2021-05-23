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
    :return: Double
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
    """
    Calculate speed between two geographical point based on haversine distance
    :param lat1: Double
    :param lon1: Double
    :param lat2: Double
    :param lon2: Double
    :return: Double
    Speed in km/h
    """
    distance = haversine(lat1, lon1, lat2, lon2) / 1000 # km
    duration = duration / 3600 # hour

    return distance / duration


class SparkComputation:

    def __init__(self):
        """
        Initialize & clean Spark Dataframe that will be used for transformations
        """
        spark = SparkSession.builder.getOrCreate()
        self.df = spark.read.csv('train.csv', inferSchema=True, header=True)\
            .dropna()\
            .dropDuplicates()

    def get_avg_speed(self):
        """
        Calculate average speed
        :return: Array
        """
        compute_speed_udf = udf(compute_speed, DoubleType())
        return self.df.withColumn('avg_speed',
                      compute_speed_udf('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude',
                                        'trip_duration')) \
            .select(mean(col("avg_speed")).alias('avg_speed')) \
            .collect()

    def get_ride_by_day_of_week(self):
        """
        Count number of ride by day of the week
        :return: Array
        """
        return self.df.withColumn('week_day', date_format(col("pickup_datetime"), "E"))\
            .groupby('week_day')\
            .count()\
            .collect()

    def get_ride_by_hour_of_day(self):
        """
        Count number of ride by hour of the day
        :return: Array
        """
        hour_of_day = udf(lambda x: int(x/4), IntegerType())
        return self.df.withColumn('hour', hour_of_day(date_format(col("pickup_datetime"), "H").cast(IntegerType())))\
            .groupby('hour')\
            .count()\
            .collect()

    def get_km_by_day_of_week(self):
        """
        Calculate sum of km by day of the week
        :return: Array
        """
        compute_distance = udf(haversine, DoubleType())
        return self.df.withColumn('distance', compute_distance('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude'))\
            .withColumn('week_day', date_format(col("pickup_datetime"), "E")) \
            .groupby('week_day')\
            .sum('distance')\
            .collect()
