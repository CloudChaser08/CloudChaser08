
NAME=dewey
DOCKER_REG=581191604223.dkr.ecr.us-east-1.amazonaws.com

build:
	docker build -t ${NAME} .
	docker tag ${NAME} ${DOCKER_REG}/${NAME}
	docker tag ${NAME} ${DOCKER_REG}/${NAME}:latest

push:
	docker push ${DOCKER_REG}/${NAME}
	docker push ${DOCKER_REG}/${NAME}:latest

package-spark:
	cd spark && make package && cd ..
