###EDEM MDA Data Project 2 Group 3
#Generating data for streaming

#Import libraries
import pandas as pd
import uuid
import time
import json

#Create dataframe from csv-file
df = pd.read_csv("dataset.csv")

while True:
    #Iterate through rows in dataframe
    for index, row in df.iterrows():
        #Save required data as variable (dict)
        sensor_data = {"id": str(uuid.uuid1()), 
                        "time": row["FECHA"],
                        "motor_power": row["Par agitador"],
                        "pressure": row["P abs SW mb"], 
                        "temperature": row["Tª SW"]}
        #Save variable as json-file that will be generated and re-written every second 
        with open("sensor_data.json", "w") as jsonFile:
            json.dump(sensor_data, jsonFile)
        print(sensor_data)  
        time.sleep(1)