## Local Development

Run the following command to spin up a local docker setup of Airflow
```
make local
```
You should be able to access the airflow dashboard on `http://localhost:8080/`

To build a docker image for x86-64 and push to GCR
```
docker buildx build --platform linux/amd64 -f Dockerfile -t gcr.io/ae32-vpcservice-datawarehouse/airflow:latest --push .
```

---

## Production

To deploy a particular branch to the Airflow production cluster, create a Github 
release with the appropriate Tag and Github Actions will deploy the changes to the production cluster.

To gain access to self-managed Airflow GKE cluster
```
make prod_login
```

To initiate Tunnel to Bastion Host
```
export HTTPS_PROXY=http://127.0.0.1:8888
gcloud compute ssh ae32-bastion-host --project ae32-vpc-host --zone europe-west2-a -- -L8888:127.0.0.1:8888
```

To access Airflow's Web UI on localhost:8080
```
make web_forward
```

## FAQ

Q: I am seeing DAG Import Errors, how do I fix them?

A: If you see the following errors when spinning up Airflow for the first time:
```
Broken DAG: [/opt/airflow/dags/napo_benefit_code_weekly.py] Traceback (most recent call last):
  File "/opt/airflow/dags/napo_benefit_code_weekly.py", line 14, in <module>
    GCP_CREDENTIALS = Variable.get("GCP_CREDENTIALS")
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/variable.py", line 141, in get
    raise KeyError(f'Variable {key} does not exist')
KeyError: 'Variable GCP_CREDENTIALS does not exist'
```
You will need to import the appropriate variables into Airflow via `Admin -> Variables` page.
You can find the variables for local development on 1Password.

Q: I am seeing the GCP connection ID erors, how do I fix them?

A: If you see the following errors when spinning up Airflow for the first time:
```
File "/usr/local/lib/python3.9/site-packages/airflow/models/connection.py", line 430, in get_connection_from_secrets
    raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")
airflow.exceptions.AirflowNotFoundException: The conn_id `google_cloud_default` isn't defined
```
You will need to setup the corresponding connection in `Admin -> Connections`. 
For Google Cloud connections, you will need to provide a local path to a service account Keyfile.