APP_NAME=dewey

clean:
	find . -name '*.pyc' -delete
	rm -f target/${APP_NAME}.zip

build-notest: clean
	mkdir -p target
	cd .. && zip -r spark/target/${APP_NAME}.zip $(shell cd .. && find spark \( -name "*.py" -o -name "*.sql" \) -print) --exclude spark/target && cd spark
	echo "Created target/${APP_NAME}.zip"

spark-test: build-notest
	pytest -v

build: spark-test
