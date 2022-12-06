FROM apache/airflow:2.3.4 as build

USER root

RUN apt-get update && apt-get install -y libpq-dev build-essential

WORKDIR "/opt/airflow/"

# Install gcloud cli
RUN sudo apt-get install apt-transport-https ca-certificates gnupg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN sudo apt-get update && sudo apt-get install google-cloud-cli

USER airflow

COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install \
    --no-cache-dir \
    -r requirements.txt

COPY airflow-configs/webserver_config.py webserver_config.py
COPY airflow-configs/airflow.cfg airflow.cfg
COPY setup.py setup.py
COPY dags dags

# Don't buffer output send it straight away so nothing is lost if python
# exits/crashes/etc before the buffer is written.
# https://stackoverflow.com/questions/59812009/what-is-the-use-of-pythonunbuffered-in-docker-file
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /opt/airflow

FROM build as prod
CMD [ "airflow" ]

FROM build as test
COPY requirements-test.txt .
RUN pip install \
    --no-cache-dir \
    -r requirements-test.txt
COPY . .
ENTRYPOINT [ "pytest", "/opt/airflow/tests", "--timeout=600" ]
