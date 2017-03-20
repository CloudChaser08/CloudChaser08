
DOCKER_REG=581191604223.dkr.ecr.us-east-1.amazonaws.com
REPO=${DOCKER_REG}/hvairflow

DOCKER_IP=$(shell docker-machine ip hv)

define RSYNC_CMD
rsync -rav --delete -e 'ssh -o StrictHostKeyChecking=no'
endef

clean:
	find . -name "*.pyc" -delete

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

pull:
	docker pull ${REPO}:latest

runprod:
	docker run -d \
	-e FERNET_KEY=ydunNyBBPZOm3atVj6njjhHASM3HfIwW/HDQu93TmPc= \
	-e BASE_URL='https://airflow-prod.aws.healthverity.com'    \
	-e SQL_CONN='postgresql+psycopg2:///airflow?host=/var/run/postgresql/' \
	-e EXECUTOR=LocalExecutor                                  \
	-v /home/airflow/airflow/dags:/usr/local/airflow/dags      \
	-v /var/run/postgresql:/var/run/postgresql                 \
	-p 8080:8080                                               \
	--name airflow                                             \
	${REPO}:latest

run: pull
	docker run -d \
	-e FERNET_KEY=eUpx4FQTDCp13ZCSk3PGAGZG1R3bzq3sfyesMmzrR40= \
	-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
	-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
	-e BASE_URL=http://${DOCKER_IP}:8080 \
	-e RESET_DB=1 \
	-v ${PWD}/dags:/usr/local/airflow/dags \
	-v ${PWD}/../spark:/usr/local/airflow/spark \
	-p 8080:8080 \
	--name airflow \
	${REPO}:latest

test: pull clean
	docker run --rm \
	-e FERNET_KEY=eUpx4FQTDCp13ZCSk3PGAGZG1R3bzq3sfyesMmzrR40= \
	-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
	-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
	-e PYTHONDONTWRITEBYTECODE=1 \
	-v ${PWD}/dags:/usr/local/airflow/dags \
	-v ${PWD}/../spark:/usr/local/airflow/spark \
	--name airflow-test \
	${REPO}:latest runtests


shell:
	docker exec -it airflow /bin/bash

HUP:
	docker exec -t airflow kill -HUP 1

# JENKINS RELATED

jenkins-test: pull clean
	docker run --rm \
	-e FERNET_KEY=eUpx4FQTDCp13ZCSk3PGAGZG1R3bzq3sfyesMmzrR40= \
	-e PYTHONDONTWRITEBYTECODE=1 \
	-v "${PWD}/dags":/usr/local/airflow/dags \
	-v "${PWD}/../spark":/usr/local/airflow/spark \
	--name airflow-test \
	${REPO}:latest runtests

jenkins-deploy-dev: jenkins-test clean
	${RSYNC_CMD} dags/     airflow@airflow-dev.awsdev.healthverity.com:airflow/dags
	${RSYNC_CMD} ../spark/ airflow@airflow-dev.awsdev.healthverity.com:spark

jenkins-deploy: jenkins-test clean
	${RSYNC_CMD} dags/     airflow@airflow-prod.awsdev.healthverity.com:airflow/dags
	${RSYNC_CMD} ../spark/ airflow@airflow-prod.awsdev.healthverity.com:spark

