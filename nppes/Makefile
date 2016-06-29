
export PGHOST=${RS_HOST}
export PGUSER=${RS_USER}
export PGPORT=5439
export PGDATABASE=dev
AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

export DATE=$(shell date '+%B_%Y')
DAY=$(shell date +'%Y-%m-%d')
SRC_FILE="http://download.cms.gov/nppes/NPPES_Data_Dissemination_${DATE}.zip"

all: prep drop create import clean

drop: 
	psql -f drop_table.sql

create:
	psql -f create_table.sql

import:
	psql \
	  --set AWS_CREDENTIALS=${AWS_CREDENTIALS} \
	  --set SOURCE="s3://salusv/reference/nppes/${DAY}/" \
	  -f copy.sql

prep: fetch split push

fetch:
	mkdir -p data
	cd data && wget ${SRC_FILE}
	cd data && 7z -y x NPPES_Data_Dissemination_${DATE}.zip

split:
	cd data && split -d -C 500M npidata_20050523-20160612.csv npi_part-
	cd data && sed -i'' -e '1d' npi_part-00
	cd data && gzip npi_part-*

push:
	aws s3 sync --sse AES256 --exclude="*" --include="npi_part-*" data/ s3://salusv/reference/nppes/${DAY}/

clean:
	rm -rf data
