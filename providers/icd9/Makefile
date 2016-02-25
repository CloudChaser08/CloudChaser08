
DAY=$(shell date +'%Y-%m-%d')
SRC_FILE='https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/ICD-9-CM-v32-master-descriptions.zip'

all: drop create prep import

drop: 
	psql -f drop_table.sql

create:
	psql -f create_table.sql

import:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  --set SOURCE="s3://salusv/reference/icd9/${DAY}/dx_long_short.csv" \
	  -f copy_dx.sql
	
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  --set SOURCE="s3://salusv/reference/icd9/${DAY}/sg_long_short.csv" \
	  -f copy_sg.sql

prep: fetch clean push

fetch:
	mkdir -p data
	cd data && wget ${SRC_FILE}
	cd data && 7z x ICD-9-CM-v32-master-descriptions.zip

clean:
	xls2csv.py data/CMS32_DESC_LONG_SHORT_DX.xlsx
	xls2csv.py data/CMS32_DESC_LONG_SHORT_SG.xlsx

push:
	aws s3 cp --sse AES256 data/CMS32_DESC_LONG_SHORT_DX.xlsx.csv s3://salusv/reference/icd9/${DAY}/dx_long_short.csv
	aws s3 cp --sse AES256 data/CMS32_DESC_LONG_SHORT_SG.xlsx.csv s3://salusv/reference/icd9/${DAY}/sg_long_short.csv

