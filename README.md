Generating requirements file

```
pipenv update
pipenv requirements > requirements.txt
```

Build docker image for x86-64 and push to GCR

```
docker buildx build --platform linux/amd64 -f Dockerfile -t gcr.io/ae32-vpcservice-datawarehouse/airflow:latest --push .
```

Access Airflow cluster deployed to GKE

```
# Tunnel
export HTTPS_PROXY=http://127.0.0.1:8888
gcloud compute ssh ae32-bastion-host --project ae32-vpc-host --zone europe-west2-a -- -L8888:127.0.0.1:8888

# Restart Airflow deployment
kubectl rollout restart deployment.apps/airflow -n airflow

# Access Airflow Web UI on localhost:8080
kubectl port-forward service/airflow-svc 8080:8080 -n airflow
```
