
all: drop create import

drop: 
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f drop_table.sql

create:
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f create_table.sql

import:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy.sql

fetch:
	mkdir -p nppes_data
	cd nppes_data && wget http://download.cms.gov/nppes/NPPES_Data_Dissemination_Aug_2015.zip
	cd nppes_data && 7za x NPPES_Data_Dissemination_Aug_2015.zip

split:
	cd nppes_data && split -d -C 500M npidata_20050523-20150809.csv npi_part-
	cd nppes_data && sed -i'' '1d' npi_part-00
	cd nppes_data && gzip npi_part-*

push:
	cd nppes_data && for i in $(ls npi_part*); do; s3 cp $i s3://healthverity/incoming/nppes/20050523-20150809/; done;
