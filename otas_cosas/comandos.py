Project_ID= edemproject2
Bucket_name = edemproject2

#Generar la imagen de docker
gcloud builds submit --tag 'gcr.io/edemproject2/dataflow/edem:latest' .


gcloud dataflow flex-template build "gs://edemproject2/dataflowtemplate.json" \
  --image "gcr.io/edemproject2/dataflow/edem:latest" \
  --sdk-language "PYTHON"



 #Para iniciar DataFlow
python edem_dataflow_streaming.py \
    --project_id edemproject2 \
    --input_subscription iotToBigQuery-sub \
    --output_topic iotToBigQuery \
    --output_bigquery edemDataset.table1 \
    --runner DataflowRunner \
    --job_name  dataflow-job \
    --region europe-west1 \
    --temp_location gs://edemproject2/tmp \
    --staging_location gs://edemproject2/stg

#Para inciciar generador de datos
cd /02_Code/00_Generator
python generator.py \
    --project_id edemproject2 \
    --topic_name iotToBigQuery

