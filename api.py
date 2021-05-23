from flask import Flask, abort
from SparkComputation import SparkComputation

app = Flask(__name__)

sc = SparkComputation()

@app.route("/avg_speed")
def avg_speed():
    result = sc.get_avg_speed()

    if len(result) < 1 or 'avg_speed' not in result[0]:
        abort(500, 'No results, something wrong append')
    elif not isinstance(result[0]['avg_speed'], (float)):
        abort(500, 'Wrong response type')

    return {'avg_speed': result[0]['avg_speed']}

@app.route("/ride_by_day_of_week")
def ride_by_day_of_week():
    rows = sc.get_ride_by_day_of_week()

    if len(rows) < 1:
        abort(500, 'No results, something wrong append')

    result = {}
    for row in rows:
        if not isinstance(row['count'], (int)):
            abort(500, 'Wrong response type')
        result[row['week_day']] = row['count']

    return result

@app.route("/ride_by_hour_of_day")
def ride_by_hour_of_day():
    rows = sc.get_ride_by_hour_of_day()

    if len(rows) < 1:
        abort(500, 'No results, something wrong append')

    result = {}
    for row in rows:
        if not isinstance(row['count'], (int)):
            abort(500, 'Wrong response type')
        result[row['hour']] = row['count']

    return result

@app.route("/km_by_day_of_week")
def km_by_day_of_week():
    rows = sc.get_km_by_day_of_week()

    if len(rows) < 1:
        abort(500, 'No results, something wrong append')

    result = {}
    for row in rows:
        if not isinstance(row['sum(distance)'], (float)):
            abort(500, 'Wrong response type')
        result[row['week_day']] = row['sum(distance)']

    return result
