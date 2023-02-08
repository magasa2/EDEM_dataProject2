# Serverless_Data_Processing_GCP
# EDEM_Master_Data_Analytics

""" Data Stream Generator:
The generator will publish a new record simulating a new transaction in our site.""" 

#Import libraries
import json
import time
import uuid
import random
import logging
import argparse
import google.auth
import pandas as pd
from faker import Faker
from datetime import datetime
from google.cloud import pubsub_v1


#Input arguments
parser = argparse.ArgumentParser(description=('Aixigo Contracts Dataflow pipeline.'))
parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A New transaction has been registered. Id: %s", message['Transaction_Id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
            

#Generator Code
def generateMockData():
    # Crear dataframe
    df = pd.read_csv("dataset.csv")
    # Simular una API indefinida
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

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message = generateMockData()
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 5 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)


