APP_NAME=dewey

clean:
	find . -name '*.pyc' -delete

build-notest:
	mkdir -p target
	cd .. && zip -r spark/target/${APP_NAME}.zip $(shell cd .. && find spark -name "*.py" -print) --exclude spark/target && cd spark
	echo "Created target/${APP_NAME}.zip"

build: spark-test build-notest

spark-test: clean
	pytest -v

