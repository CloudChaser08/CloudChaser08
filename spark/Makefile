APP_NAME=dewey

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
	pytest -v || (rm target/${APP_NAME}.zip && exit 1)
	@$(MAKE) clean-test

# excludes external table loader tests
# excludes nextgen tests while they depend on external tables
spark-test-jenkins: clean-test build-notest
	pytest -v -x --ignore=test/helpers/external_table_loader_test.py \
                  --ignore=test/providers/nextgen || (rm target/${APP_NAME}.zip && exit 1)

build: spark-test
