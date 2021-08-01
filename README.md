# pubsub-dataflow-bigquery

A sample code of Google Cloud Dataflow for 'Exactly-Once' streaming insertion, from Google Cloud Pub/Sub to Google BigQuery.

```sh
bash ./setup.sh

cd ./dataflow
docker build -t pubsub-dataflow-bigquery .

docker run --rm -v ~/.config:/root/.config -e GCP_PROJECT=your-project-id pubsub-dataflow-bigquery bash /tmp/start_streaming.sh
```
