# pubsub-dataflow-bigquery

```
cd ./gcp/
docker build -t pubsub-dataflow-bigquery .

docker run --rm -v ~/.config:/root/.config -e GCP_PROJECT=your-project-id pubsub-dataflow-bigquery bash /tmp/start_streaming.sh
```
