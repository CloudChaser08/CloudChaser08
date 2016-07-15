
AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

DAY=$(shell date +'%Y-%m-%d')
SRC_FILE='http://www.accessdata.fda.gov/cder/ndctext.zip'

all: prep drop create import clean

drop:
	psql -f drop_table.sql

create:
	psql -f create_table.sql

import:
	psql \
	  --set AWS_CREDENTIALS=${AWS_CREDENTIALS} \
	  --set SOURCE="s3://salusv/reference/ndc/${DAY}/package.txt" \
	  -f copy_package.sql
	
	psql \
	  --set AWS_CREDENTIALS=${AWS_CREDENTIALS} \
	  --set SOURCE="s3://salusv/reference/ndc/${DAY}/product.txt" \
	  -f copy_product.sql
	
	psql \
	  -f transform_ndc.sql

prep: fetch push

fetch:
	mkdir -p data
	cd data && wget ${SRC_FILE}
	cd data && 7z -y x ndctext.zip

push:
	aws s3 cp --sse AES256 data/product.txt s3://salusv/reference/ndc/${DAY}/product.txt
	aws s3 cp --sse AES256 data/package.txt s3://salusv/reference/ndc/${DAY}/package.txt

clean:
	rm -rf data
