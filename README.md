# Structured Streaming

## Run the job (local)
### Build
```bash
poetry build
```

### Running
```bash
poetry run spark-submit \
  --master local \
  --packages 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'\
  --py-files dist/structured_streaming-*.whl jobs/sample_job.py \
  <SERVER>:9092 com.google.sample.purchases.1 ./tmp/output ./tmp/checkpoint '2 seconds'
```

### Submit to Dataproc
#### Create Dataproc cluster
Create the cluster with [python dependencies](./scripts/initialize-cluster.sh) and submit the job

```bash
export REGION=us-central1;
gcloud dataproc clusters create cluster-sample \
--region=${REGION} \
--initialization-actions=gs://andresousa-experimental-scripts/initialize-cluster.sh
```
### Preparing dependencies
```bash
poetry export -f requirements.txt --output requirements.txt
```

#### Submit job
```bash
gcloud dataproc jobs submit pyspark \
  --cluster=cluster-sample \
  --region=us-central1 \
  --py-files dist/structured_streaming-*.whl jobs/sample_job.py \
  -- <SERVER>:9092 test-topic gs://andresousa-experimental-streaming-test gs://andresousa-experimental-checkpoints '2 seconds'
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