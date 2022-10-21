FROM apache/airflow:2.3.4

USER root

RUN apt-get update && apt-get install -y libpq-dev build-essential

WORKDIR "/opt/airflow/"

USER airflow

COPY requirements.txt .

RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

COPY airflow-configs/webserver_config.py webserver_config.py
COPY airflow-configs/airflow.cfg airflow.cfg
COPY dags dags