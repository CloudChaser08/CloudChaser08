
DAY=$(shell date +'%Y-%m-%d')
DX_FILE='https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2015-code-descriptions.zip'
PCS_FILE='https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2015-PCS-long-and-abbreviated-titles.zip'

all: prep drop create import clean

drop: 
	psql -f drop_table.sql

create:
	psql -f create_table.sql

import:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  --set SOURCE="s3://salusv/reference/icd10/${DAY}/dx.tsv" \
	  -f copy_dx.sql
	
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  --set SOURCE="s3://salusv/reference/icd10/${DAY}/pcs.tsv" \
	  -f copy_pcs.sql

prep: fetch convert push

fetch:
	mkdir -p data
	cd data && wget ${DX_FILE}
	cd data && wget ${PCS_FILE}
	cd data && 7z x -y 2015-code-descriptions.zip
	cd data && 7z x -y 2015-PCS-long-and-abbreviated-titles.zip

convert:
	./convert.sh data/icd10cm_order_2015.txt
	./convert.sh data/icd10pcs_order_2015.txt

push:
	aws s3 cp --sse AES256 data/icd10cm_order_2015.txt.tsv s3://salusv/reference/icd10/${DAY}/dx.tsv
	aws s3 cp --sse AES256 data/icd10pcs_order_2015.txt.tsv s3://salusv/reference/icd10/${DAY}/pcs.tsv

clean:
	rm -rf data
