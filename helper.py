#The following file details functions to retrieve and process the data from the ergastF1 database run on your localhost

import mysql.connector
from mysql.connector.plugins import caching_sha2_password
import pandas as pd

def sql_return(request, values):
    try:
        ergast = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="888000Suva",
            database="ergast_f1_copy"
        )
        mycursor = ergast.cursor()
        mycursor.execute(request, values)
        result = mycursor.fetchall()
        mycursor.close()
    except Exception as e:
        result = f"Error connecting to database due to {e}"
    return result

drivers = ["Lewis Hamilton"]
year = 2019
round = 1

for driver in drivers:
    results =  pd.DataFrame(sql_return(("""
                SELECT CONCAT(drivers.forename, " ", drivers.surname) AS Driver, laptimes.lap, laptimes.position, laptimes.time, 
                    races.year,races.name, drivers.code
                FROM laptimes 
                JOIN drivers
                    ON laptimes.driverId = drivers.driverId AND drivers.forename = %s AND drivers.surname = %s
                JOIN races
                    ON races.raceId = laptimes.raceId AND races.year = %s AND races.round = %s
                    ORDER BY laptimes.lap"""),
                    (driver.split(" ")[0], driver.split(" ")[1], year ,round)),
                    columns = ["Driver", "Lap", "Position", "Time", "Year", "Race", "Code"])

print(results)