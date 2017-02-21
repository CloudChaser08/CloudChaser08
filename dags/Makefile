

build-test:
	mkdir tmp > /dev/null 2>&1; export AIRFLOW_HOME=tmp

	# install required packages
	pip install -r requirements.txt

	# initialize airflow at the new AIRFLOW_HOME
	airflow initdb

run-test:
	python `which nosetests` -w test/ -v
