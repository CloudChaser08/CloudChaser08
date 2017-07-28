build: test
	rm -rf dashboard.zip
	cd src; zip -r ../dashboard.zip *

deploy: build
	aws lambda update-function-code --function-name provider-status-dash --zip-file 'fileb://dashboard.zip'

test:
	mocha './src/**/*_test.js'
