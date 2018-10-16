APP_NAME=dewey

analytics-deploy:
	scp -i ~/.ssh/emr_deployer spark/target/dewey.zip hadoop@analytics.aws.healthverity.com:/tmp/
	ssh -i ~/.ssh/emr_deployer hadoop@analytics.aws.healthverity.com 'sudo cp /usr/lib/spark/python/lib/dewey.zip /usr/lib/spark/python/lib/dewey.zip.$(date +%Y%m%d%H%M)'
	ssh -i ~/.ssh/emr_deployer hadoop@analytics.aws.healthverity.com "sudo mv /tmp/dewey.zip /usr/lib/spark/python/lib/dewey.zip"
	ssh -i ~/.ssh/emr_deployer hadoop@analytics.aws.healthverity.com "sudo find /usr/lib/spark/python/lib/dewey.zip.* -mtime +7 -delete"

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

package: clean-test build-notest
	cd .. && tar -czf dewey_spark.tar.gz --exclude spark/test spark
	echo "Created dewey_spark.tar.gz"

build: spark-test
