

build-test:
	mkdir tmp > /dev/null 2>&1; export AIRFLOW_HOME=tmp

	# required airflow packages
	python `which pip` install airflow
	python `which pip` install "airflow[slack]"
	python `which pip` install "airflow[hive]"

	# used to run tests
	python `which pip` install nose
	python `which pip` install mock

	# required airflow packages
	python `which pip` install boto

	# initialize airflow at the new AIRFLOW_HOME
	airflow initdb

run-test:
	python `which nosetests` -w test/
