GIT_COMMIT?=$(shell git rev-parse HEAD)
BRANCH_NAME?=$(shell git rev-parse --abbrev-ref HEAD)
BRANCH_TAG=$(subst /,_,$(BRANCH_NAME))

DOCKER_REPO?=repo_not_set
DOCKER_NAME?=napo-airflow
DOCKER_TAG?=${BRANCH_TAG}
DOCKER_IMAGE=${DOCKER_NAME}:${GIT_COMMIT}
DOCKER_BRANCH_IMAGE=${DOCKER_NAME}:${DOCKER_TAG}

export AIRFLOW_UID?=$(shell id -u ${USER})
export AIRFLOW_GID?=0
export PIPENV_VENV_IN_PROJECT=1
export K8S_NAMESPACE=airflow
define PATCH_TEMPLATE
{
  "spec":{
    "template":{
      "spec":{
        "containers":[
          {
            "name":"airflow-scheduler",
            "image":"${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE}"
          },
          {
            "name":"airflow-webserver",
            "image":"${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE}"
          }
        ]
      }
    }
  }
}
endef
export PATCH_TEMPLATE


.DEFAULT_GOAL := all

.PHONY: all
all:
	echo "Please choose a make target to run."

.PHONY: build
build:
	pip3 install --upgrade pip pipenv
	pipenv lock
	pipenv requirements > requirements.txt

.PHONY: local
local:
	docker build --target prod -t ${DOCKER_NAME}:local .
	docker-compose up -d

.PHONY: clean
clean:
	find . -iname '*.pyc' -exec rm {} \; -print

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

.PHONY: lint
lint:
	flake8 dags/*

.PHONY: test
test: lint
	pytest --timeout=600

.PHONY: docker_build
docker_build: clean
	docker build \
		-t ${DOCKER_IMAGE} \
		-t ${DOCKER_BRANCH_IMAGE} \
		-t ${DOCKER_REPO}/${DOCKER_IMAGE} \
		-t ${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE} \
		--target prod .
	docker build \
		-t ${DOCKER_IMAGE}-test \
		-t ${DOCKER_BRANCH_IMAGE}-test \
		-t ${DOCKER_REPO}/${DOCKER_IMAGE}-test \
		-t ${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE}-test \
		--target test .

.PHONY: docker_test
docker_test:
	docker run --rm ${DOCKER_IMAGE}-test

.PHONY: docker_release
docker_release:
	docker push ${DOCKER_REPO}/${DOCKER_IMAGE}
	docker push ${DOCKER_REPO}/${DOCKER_BRANCH_IMAGE}

.PHONY: kubectl_patch
kubectl_patch:
	kubectl patch deployment airflow -p "$${PATCH_TEMPLATE}" -n ${K8S_NAMESPACE}
