
DOCKER_REG=581191604223.dkr.ecr.us-east-1.amazonaws.com
REPO=${DOCKER_REG}/hvairflow

DOCKER_IP=$(shell docker-machine ip hv)

build:
	docker build -t ${REPO}:${USER} .
	docker tag ${REPO}:${USER} ${REPO}:latest

push:
	docker push ${REPO}:${USER}
	docker push ${REPO}:latest

jenkins-build:
	docker build -f Dockerfile --no-cache=true -t ${REPO}:${TAG_TO_DEPLOY} .
	docker tag ${REPO}:${TAG_TO_DEPLOY} ${REPO}:latest

jenkins-push:
	docker push ${REPO}:${TAG_TO_DEPLOY}
	docker push ${REPO}:latest

run:
	docker pull ${REPO}:latest

	docker run -d \
	-e FERNET_KEY=eUpx4FQTDCp13ZCSk3PGAGZG1R3bzq3sfyesMmzrR40= \
	-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
	-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
	-e DOCKER_IP=${DOCKER_IP} \
	-v ${PWD}/dags:/usr/local/airflow/dags \
	-v ${PWD}/../spark:/usr/local/airflow/spark \
	-p 8080:8080 \
	--name airflow \
	${REPO}:latest

test:
	docker exec -it airflow /bin/bash -c 'cd $$AIRFLOW_HOME && cd dags && nosetests -w test/ -v'

shell:
	docker exec -it airflow /bin/bash

HUP:
	docker exec -t airflow kill -HUP 1

deploy-dev:
	make test &&										 \
		cd dags && scp -r . airflow@airflow-dev.awsdev.healthverity.com:airflow/dags &&	 \
		cd ../../spark && scp -r . airflow@airflow-dev.awsdev.healthverity.com:spark/

deploy:
	make test &&										 \
		cd dags && scp -r . airflow@airflow-prod.aws.healthverity.com:airflow/dags &&	 \
		cd ../../spark && scp -r . airflow@airflow-prod.aws.healthverity.com:spark/
