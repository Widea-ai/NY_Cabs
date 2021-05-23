from flask import Flask
from SparkComputation import SparkComputation

app = Flask(__name__)

sc = SparkComputation()

@app.route("/avg_speed")
def avg_speed():
    result = sc.get_avg_speed()
    return {'avg_speed': result[0]['avg_speed']}

@app.route("/ride_by_day_of_week")
def ride_by_day_of_week():
    result = {}
    for row in sc.get_ride_by_day_of_week():
        result[row['week_day']] = row['count']

    return result

@app.route("/ride_by_hour_of_day")
def ride_by_hour_of_day():
    result = {}
    for row in sc.get_ride_by_hour_of_day():
        result[row['hour']] = row['count']

    return result

@app.route("/km_by_day_of_week")
def km_by_day_of_week():
    result = {}
    for row in sc.get_km_by_day_of_week():
        result[row['week_day']] = row['sum(distance)']

    return result
