APP_NAME=dewey

build-notest:
	mkdir -p target
	cd .. && zip -r spark/target/${APP_NAME}.zip $(shell cd .. && find spark -name "*.py" -print) --exclude spark/target && cd spark
	echo "Created target/${APP_NAME}.zip"

clean:
	find . -name '*.pyc' -delete
	rm -f target/dewey.zip

spark-test: clean build-notest
	pytest -v

build: spark-test
