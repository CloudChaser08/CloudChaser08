

build:
	docker build -t hvairflow .

run:
	docker run -d \
	-e FERNET_KEY=eUpx4FQTDCp13ZCSk3PGAGZG1R3bzq3sfyesMmzrR40= \
	-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
	-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
	-v ${PWD}/../dags:/usr/local/airflow/dags \
	-p 8080:8080 \
	--name airflow \
	hvairflow


shell:
	docker exec -it airflow /bin/bash


HUP:
	docker exec -t airflow kill -HUP 1
