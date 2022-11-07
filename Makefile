BRANCH_NAME?=$(shell git rev-parse --abbrev-ref HEAD)
BRANCH_TAG=$(subst /,_,$(BRANCH_NAME))

DOCKER_REPO?=repo_not_set
DOCKER_NAME?=napo-airflow
DOCKER_IMAGE=${DOCKER_NAME}:${GIT_COMMIT}

export DOCKER_BRANCH_IMAGE=${DOCKER_NAME}:${BRANCH_TAG}
export AIRFLOW_UID?=$(shell id -u ${USER})
export AIRFLOW_GID?=0
export PIPENV_VENV_IN_PROJECT=1

.DEFAULT_GOAL := all

.PHONY: all
all:
	echo "Please choose a make target to run."

.PHONY: local
local:
	pip3 install --upgrade pip pipenv
	pipenv lock
	pipenv requirements > requirements.txt
	docker build -t airflow:local .
	docker-compose up -d

.PHONY: clean
clean:
	find . -iname '*.pyc' -exec rm {} \; -print

.PHONY: docker_build
docker_build: clean
	docker build -t ${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE} .

.PHONY: docker_push
docker_push:
	docker push ${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE}

.PHONY: prod_login
prod_login:
	gcloud container clusters get-credentials production \
		--region europe-west2 \
		--project ae32-vpcservice-datawarehouse

.PHONY: web_forward
web_forward:
	kubectl port-forward service/airflow-svc 8080:8080 -n airflow

.PHONY: up
up:
	docker-compose --project-name ${DOCKER_NAME} up --remove-orphans

.PHONY: launch
launch:
	docker-compose --project-name ${DOCKER_NAME} up --remove-orphans -d

.PHONY: ps
ps:
	docker-compose --project-name ${DOCKER_NAME} ps

.PHONY: down
down:
	docker-compose --project-name ${DOCKER_NAME} logs -t
	docker-compose --project-name ${DOCKER_NAME} down --remove-orphans
	rm -r logs

.PHONY: k8s_rollout
k8s_rollout:
	kubectl rollout restart deployment.apps/airflow -n airflow

.PHONY: lint
lint:
	flake8 dags/*

