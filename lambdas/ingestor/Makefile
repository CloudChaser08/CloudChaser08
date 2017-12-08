build: requirements.txt ingestor.py clean
	mkdir package
	cp ingestor.py package/
	pip install -r requirements.txt -t ./package/
	cd package && zip -r ../ingestor.zip ./

clean :
	rm -fr package
	rm -f ingestor.zip
	find . -name '*.pyc' -exec rm -f {} +
