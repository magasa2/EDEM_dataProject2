Project_ID= edemproject2
Bucket_name = edemproject2

#Generar la imagen de docker
gcloud builds submit --tag 'gcr.io/edemproject2/dataflow/edem:latest' .
gcloud builds submit --tag 'gcr.io/edemproject2/generador/edem:latest' .
#Create Dataflow Flex Template from Image(si lo haces con la imagen de docker) :
gcloud dataflow flex-template build "gs://edemproject2/dataflowtemplate.json" \
    --image 'gcr.io/edemproject2/dataflow/edem:latest' \
    --sdk-language "PYTHON" \
    --metadata-file "schemas/metadata.json"

#Run Dataflow Flex Template (sin levantar la imagen docker):
gcloud dataflow flex-template run edem-dataflow-job-docker \
    --template-file-gcs-location "gs://edemproject2/dataflowtemplate.json" \
    --parameters project_id= "edemproject2" \
    --parameters input_subscription= "iotToBigQuery-sub" \
    --parameters output_topic= "iotToCloudFunctions" \
    --parameters output_bigquery= "edemDataset.table5" \
    --region "europe-west1"

 #Para iniciar DataFlow
python dataflow.py \
    --project_id edemproject2 \
    --input_subscription iotToBigQuery-sub \
    --output_topic iotToCloudFunctions \
    --output_bigquery edemDataset.table5 \
    --runner DataflowRunner \
    --job_name  dataflow-job \
    --region europe-west1 \
    --temp_location gs://edemproject2/tmp \
    --staging_location gs://edemproject2/stg

#Para inciciar generador de datos

python generator_publisher.py \
    --project_id edemproject2 \
    --topic_name iotToBigQuery

gcloud builds submit --tag gcr.io/edemproject2/generador

gcloud builds submit --tag gcr.io/edemproject2/generador


gcloud builds submit --tag gcr.io/edemproject2/generador --file Dockerfile .


docker push gcr.io/edemproject2/my-image


python main.py -t 10 -e 2 -i my-image \
    --project_id edemproject2 \
    --topic_name iotToBigQuery

python main.py -t 10 -e 2 -i my-image --project=edemproject2 --topic=iotToBigQuery

python main.py -t 10 -e 2 -i my-image \
    -p edemproject2 \
    -tp iotToBigQuery

python main.py -t 10 -e 2 -i my-image -p edemproject2 -tp iotToBigQuery


levantar imagen y arrancar el docker
docker buil -t myimage .

docker run myimage --edemproject2 --iotToBigQuery

docker run -e myimage --project_id edemproject2 --topic_name iotToBigQuery

docker run -i myimage -e project_id=edemproject2 -e topic_name=iotToBigQuery 


docker run -e myimage -e --project_id=edemproject2 -e --topic_id=iotToBigQuery
