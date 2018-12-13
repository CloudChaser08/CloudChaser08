
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


pylint-score:
	echo "pylint.score.dewey:`find . -name '*.py' | xargs pylint | grep 'Your code' | sed -e 's/Your\ code\ has\ been\ rated\ at\ //' -e 's/\/.*//'`|g"| nc -v -q2 -u localhost 8125
