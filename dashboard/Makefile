build: test
	rm -rf dashboard.zip
	cd src; zip -r ../dashboard.zip *

upload-prod: build
	aws lambda update-function-code --function-name provider-status-dash --zip-file 'fileb://dashboard.zip'

upload-dev: build
	aws lambda update-function-code --function-name provider-status-dash-dev --region us-east-1 --zip-file 'fileb://dashboard.zip' 

deploy-dev: upload-dev
	aws lambda invoke --function-name provider-status-dash-dev --region us-east-1 --log-type Tail --payload '{"dev": true}' invoke_output.txt

test: install-deps
	mocha './src/**/*_test.js'

install-deps:
	cd src && npm install && cd ..

teardown-prod:
	aws lambda delete-function --function-name provider-status-dash

teardown-dev:
	aws lambda delete-function --function-name provider-status-dash-dev