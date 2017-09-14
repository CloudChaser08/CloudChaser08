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

spark-test: build-notest clean-test
	pytest -v
	clean-test

build: spark-test
