import sys
import math
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, mean, col, date_format

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate haversine distance
    :param lat1: Double
    :param lon1: Double
    :param lat2: Double
    :param lon2: Double
    :return: distance
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


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('train.csv', inferSchema=True, header=True)

    if sys.argv[1] == 'avg_speed':
        compute_speed_udf = udf(compute_speed, DoubleType())
        avg_speed = df.withColumn('avg_speed', compute_speed_udf('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude', 'trip_duration'))\
            .select(mean(col("avg_speed")).alias('avg_speed'))\
            .collect()
        print(avg_speed[0]['avg_speed'])

    elif sys.argv[1] == 'ride_by_day_of_week':
        ride_by_day_of_week = df.withColumn('week_day', date_format(col("pickup_datetime"), "E"))\
            .groupby('week_day')\
            .count()\
            .collect()

        for day in ride_by_day_of_week:
            print(day['week_day'], day['count'])

    elif sys.argv[1] == 'ride_by_hour_of_day':

    else:
        print('Unkown command')

# df.show()
# df.printSchema()