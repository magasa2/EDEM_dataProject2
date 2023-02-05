import pandas as pd
import uuid
import time
import json


def iterate_rows():
    # Crear dataframe
    df = pd.read_csv("dataset.csv")
    # Simular una API indefinida
    while True:
        # Iterar por cada fila del dataframe
        for index, row in df.iterrows():
            # guardar datos necesarios en una variable del formato json
            sensor_data = {"id": str(uuid.uuid1()), 
                            "time": row["FECHA"],
                            "motor_power": row["Par agitador"],
                            "pressure": row["P abs SW mb"], 
                            "temperature": row["Tª SW"]}
            # guardar la variable en un archivo .json, que se sobreescribe cada segundo 
            with open("sensor_data.json", "w") as jsonFile:
                json.dump(sensor_data, jsonFile)
            time.sleep(1)
            print(sensor_data)
        return sensor_data