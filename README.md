## Local Development

Run the following command to spin up a local docker setup of Airflow
```
make local
```
You should be able to access the airflow dashboard on `http://localhost:8080/`

---

## Production Deployment (WIP)

To gain access to self-managed Airflow GKE cluster
```
make prod_login
```

We are currently missing CI/CD for Airflow development, to deploy changes to GKE:

Build docker image for x86-64 and push to GCR
```
docker buildx build --platform linux/amd64 -f Dockerfile -t gcr.io/ae32-vpcservice-datawarehouse/airflow:latest --push .
```

Initiate Tunnel to Bastion Host
```
export HTTPS_PROXY=http://127.0.0.1:8888
gcloud compute ssh ae32-bastion-host --project ae32-vpc-host --zone europe-west2-a -- -L8888:127.0.0.1:8888
```

To pull the latest image and restart the Airflow deployment
```
make k8s_rollout
```

To access Airflow's Web UI on localhost:8080
```
make web_forward
```

Note that some pipelines may require access to certain databases and some variables may need to be imported  