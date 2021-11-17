APP_NAME=dewey
CONTAINER_NAME=dewey-spark-tester-3
NOW=$(shell date '+%F_%H_%M_%S')
KEY_FILE?=~/.ssh/emr_deployer

analytics-deploy:
	scp -i ${KEY_FILE} target/dewey.zip hadoop@analytics.aws.healthverity.com:/tmp/
	ssh -i ${KEY_FILE} hadoop@analytics.aws.healthverity.com "sudo cp /usr/lib/spark/python/lib/dewey.zip /usr/lib/spark/python/lib/dewey.zip.$(date +%Y%m%d%H%M)"
	ssh -i ${KEY_FILE} hadoop@analytics.aws.healthverity.com "sudo mv /tmp/dewey.zip /usr/lib/spark/python/lib/dewey.zip"
	ssh -i ${KEY_FILE} hadoop@analytics.aws.healthverity.com "sudo find /usr/lib/spark/python/lib/dewey.zip.* -mtime +7 -delete"
	ssh -i ${KEY_FILE} hadoop@analytics.aws.healthverity.com "sudo -u hadoop hdfs dfs -put -f /usr/lib/spark/python/lib/dewey.zip /user/spark/lib/"
	ssh -i ${KEY_FILE} hadoop@analytics.aws.healthverity.com "sudo -u hadoop hdfs dfs -chgrp hadoop /user/spark/lib/dewey.zip"

clean-build:
	rm -f target/${APP_NAME}.zip

clean-test:
	rm -rf spark-warehouse
	rm -f derby.log
	rm -rf metastore_db
	find . -name '*.pyc' -delete

build-notest: clean-build
	mkdir -p target
	cd .. && zip -r spark/target/${APP_NAME}.zip $(shell cd .. && find spark -name "*.py" -print) --exclude spark/target && cd spark
	echo "Created target/${APP_NAME}.zip"

spark-test: clean-test build-notest
	pytest -v test/ || (rm target/${APP_NAME}.zip && exit 1)
	@$(MAKE) clean-test

# excludes external table loader tests
# excludes nextgen tests while they depend on external tables
spark-test-jenkins: clean-test build-notest
	pytest -v -x --ignore=test/helpers/external_table_loader_test.py \
                  --ignore=test/providers/nextgen test/ || (rm target/${APP_NAME}.zip && exit 1)

# excludes external table loader tests
# excludes nextgen tests while they depend on external tables
spark-test-jenkins-python3: clean-test build-notest
	PYSPARK_PYTHON=python3 python3 -m pytest -v -x \
		  --ignore=test/helpers/external_table_loader_test.py \
                  --ignore=test/providers/nextgen test/ || (rm target/${APP_NAME}.zip && exit 1)

package: clean-test build-notest
	cd .. && tar -czf dewey_spark.tar.gz --exclude spark/test spark
	echo "Created dewey_spark.tar.gz"

build: spark-test

build-container:
	docker build -t dewey_spark:latest .

test-python2: build-container clean-test build-notest
	@if [ -n "${AWS_ACCESS_KEY_ID}" ]; \
	then \
		$(eval ID_OPT=-e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}") \
		$(eval SECRET_OPT=-e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}") \
		:; \
	fi
	@docker run --rm --name dewey-spark-tester-2 -v "${PWD}:/dewey/spark" \
		$(ID_OPT) \
		${SECRET_OPT} \
		-e "AWS_DEFAULT_REGION=us-east-1" \
		-w "/dewey/spark" \
		--entrypoint python dewey_spark:latest -m pytest -v \
		-x --ignore=/dewey/spark/test/helpers/external_table_loader_test.py \
		-x --ignore=/dewey/spark/test/providers/nextgen \
		/dewey/spark/test

test-python3: build-container clean-test build-notest
	@if [ -n "${AWS_ACCESS_KEY_ID}" ]; \
	then \
		$(eval ID_OPT=-e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}") \
		$(eval SECRET_OPT=-e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}") \
		:; \
	fi
	@docker run --rm --name ${CONTAINER_NAME}-${NOW}-${BUILD_NUMBER} -v "${PWD}:/dewey/spark" \
		$(ID_OPT) \
		${SECRET_OPT} \
		-e "AWS_DEFAULT_REGION=us-east-1" \
		-e "PYSPARK_PYTHON=python3" \
		-w "/dewey/spark" \
		--entrypoint python3 dewey_spark:latest -m pytest -v \
		-x --ignore=/dewey/spark/test/helpers/external_table_loader_test.py \
		-x --ignore=/dewey/spark/test/providers/nextgen \
		/dewey/spark/test

lint-python: build-container
	@docker run --rm -v "${PWD}:/dewey/spark" \
		-e "AWS_DEFAULT_REGION=us-east-1" \
		-e "PYSPARK_PYTHON=python3" \
		-w "/dewey/spark" \
		--entrypoint python3 dewey_spark:latest /root/spark/lint_all_packages.py \
		--threshold ${PYLINT_THRESHOLD}

dewey-spark-shell: build-container clean-test build-notest
	@if [ -n "${AWS_ACCESS_KEY_ID}" ]; \
	then \
		$(eval ID_OPT=-e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}") \
		$(eval SECRET_OPT=-e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}") \
		:; \
	fi
	@docker run --rm --name ${CONTAINER_NAME} -v "${PWD}:/dewey/spark" \
		$(ID_OPT) \
		${SECRET_OPT} \
		-e "AWS_DEFAULT_REGION=us-east-1" \
		-e "PYSPARK_PYTHON=python3" \
		-w "/dewey/spark" \
		-it dewey_spark:latest
