APP_NAME=dewey

build:
	mkdir -p target
	cd .. && zip -r spark/target/${APP_NAME}.zip spark/* --exclude spark/target && cd spark
	echo "Created target/${APP_NAME}.zip"
