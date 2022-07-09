# Some Sample Structured Streaming

## Run the job (local)
### Build
```bash
poetry build
```

### Running
```bash
poetry run spark-submit \
  --master local \
  --packages 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3'\
  --py-files dist/structured_streaming-*.whl jobs/sample_job.py \
  <IP_KAFKA_BROKER>:9092 com.google.sample.purchases.2 ./tmp/output ./tmp/checkpoint '60 seconds'
```

### Submit to Dataproc
#### Preparing dependencies
```bash
poetry export -f requirements.txt --output requirements.txt
```
#### Copy initialization script GCS
```bash
gsutil cp requirements.txt gs://andresousa-experimental-scripts
```
#### Create Dataproc cluster
Create the cluster with [python dependencies](./scripts/initialize-cluster.sh) and submit the job

```bash
export REGION=us-central1;
gcloud dataproc clusters create cluster-sample \
--region=${REGION} \
--image-version 2.0-debian10 \
--initialization-actions=gs://andresousa-experimental-scripts/initialize-cluster.sh 
```


#### Submit job
```bash
gcloud dataproc jobs submit pyspark \
  --cluster=cluster-sample \
  --region=us-central1 \
  --properties=^#^spark.jars.packages='org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3' \
  --py-files dist/structured_streaming-*.whl \
  jobs/sample_job.py \
  -- <IP_KAFKA_BROKER>:9092 com.google.sample.purchases.2 gs://andresousa-experimental-streaming-test/output gs://andresousa-experimental-checkpoints/checkpoint '60 seconds'
```

## Debugging

### See query streaming in console
```python
query = df \
    .writeStream \
    .outputMode('Append') \
    .format('console') \
    .start()

query.awaitTermination()
```
