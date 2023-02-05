""" Serverless Data Processing with Dataflow
    Master Data Analytics EDEM
    Academic Year 2022-2023"""

""" Import libraries """

#Import beam libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools

#Import Common Libraries
from datetime import datetime
import argparse
import json
import logging
import requests

""" Helpful functions """
def ParsePubSubMessage(message):
    #Decode PubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')
    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)
    #Logging
    logging.info("Receiving message from PubSub:%s", pubsubmessage)
    #Return function
    return row

""" Dataflow Process """
def run():

    """ Input Arguments"""
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                    '--project_id',
                    required=True,
                    help='GCP cloud project name')
    parser.add_argument(
                    '--input_subscription',
                    required=True,
                    help='PubSub Subscription which will be the source of data.')
    parser.add_argument(
                    '--output_bigquery',
                    required=True,
                    help='Table where data will be stored in BigQuery. Format: <dataset>.<table>.')
    parser.add_argument(
                    '--bigquery_schema_path',
                    required=False,
                    default='./schema/bq_schema.json',
                    help='BigQuery Schema Path within the repository.')

                    
    args, pipeline_opts = parser.parse_known_args()

    """ BigQuery Table Schema """

    #Load schema from /schema folder
    with open(args.bigquery_schema_path) as file:
        input_schema = json.load(file)

    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(input_schema))

    """ Apache Beam Pipeline """
    #Pipeline Options
    options = PipelineOptions(pipeline_opts, save_main_session=True, streaming=True, project=args.project_id)

    #Pipeline
    with beam.Pipeline(argv=pipeline_opts,options=options) as p:
        data = (
            p 
                | "Read From PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/{args.project_id}/subscriptions/{args.input_subscription}", with_attributes=True)
                # Parse JSON messages with Map Function
                | "Parse JSON messages" >> beam.Map(ParsePubSubMessage) 
              
        )

        """ Part 02: Writing data to BigQuery"""
        (
            data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{args.project_id}:{args.output_bigquery}",
                schema = schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        

if __name__ == '__main__':
    #Add Logs
    logging.getLogger().setLevel(logging.INFO)
    #Run process
    run()
