# pubsub-dataflow-bigquery

```sh
cd ./gcp
bash ./setup.sh

cd ./dataflow
docker build -t pubsub-dataflow-bigquery .

docker run --rm -v ~/.config:/root/.config -e GCP_PROJECT=your-project-id pubsub-dataflow-bigquery bash /tmp/start_streaming.sh
```
