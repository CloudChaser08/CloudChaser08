objects = ingestor.py
build: requirements.txt clean $(objects)
	mkdir package
	cp $(objects) package/
	mkdir -p package/hvpipeline/framework/
	cp -r ../../framework package/hvpipeline/
	pip install -r ../../framework/requirements.txt -t ./package/
	pip install -r requirements.txt -t ./package/
	cd package && zip -r ../ingestor.zip ./

build-quiet: requirements.txt clean $(objects)
	mkdir package
	cp $(objects) package/
	mkdir -p package/hvpipeline/framework/
	cp -r ../../framework package/hvpipeline/
	pip install -r ../../framework/requirements.txt -t ./package/
	pip install -q -r requirements.txt -t ./package/
	cd package && zip -q -r ../ingestor.zip ./

deploy: build-quiet
	aws s3 cp ingestor.zip s3://healthveritydev/ifishbein/lambdas/ingestor/

clean:
	rm -fr package
	rm -f ingestor.zip
	find . -name '*.pyc' -exec rm -f {} +
