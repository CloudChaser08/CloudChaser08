
AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

DAY=$(shell date +'%Y-%m-%d')
YESTERDAY=$(shell date --date=yesterday +'%Y-%m-%d')

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

prep: fetch ingest-new push 

ingest-new:
	bin/prepare_ndc_package.py -o data/yesterday_package.txt -n data/package.txt -u data/package_updated.txt
	bin/prepare_ndc_product.py -o data/yesterday_product.txt -n data/product.txt -u data/product_updated.txt

fetch:
	mkdir -p data
	/usr/local/bin/aws s3 cp --sse AES256 s3://salusv/reference/ndc/${YESTERDAY}/product.txt data/yesterday_product.txt 
	/usr/local/bin/aws s3 cp --sse AES256 s3://salusv/reference/ndc/${YESTERDAY}/package.txt data/yesterday_package.txt 
	
	cd data && wget ${SRC_FILE}
	cd data && 7z -y x ndctext.zip

push:
	/usr/local/bin/aws s3 cp --sse AES256 data/product_updated.txt s3://salusv/reference/ndc/${DAY}/product.txt
	/usr/local/bin/aws s3 cp --sse AES256 data/package_updated.txt s3://salusv/reference/ndc/${DAY}/package.txt

clean:
	rm -rf data
