# NY_Cabs
python :  3.8  
Dev env : Debian 10  

## Project Set Up
### Install dependencies :
If you wish, you can set up a venv, then run : 
```shell
pip install -r requirements.txt
```

### To run the project :
```shell
export FLASK_APP=api
flask run
```

## How to use it
After running the project, you can access different parts of the test on a browser. 

### Average speed of each ride
Instead of returning speed of 1.500.000 rides, i've made the average of all rides. 
```
http://localhost:5000/avg_speed
```

### Number of rides by day of the week
```
http://localhost:5000/ride_by_day_of_week
```

### Number of rides by hour of the day
```
http://localhost:5000/ride_by_hour_of_day
```

### Number of km by day of the week
```
http://localhost:5000/km_by_day_of_week
```