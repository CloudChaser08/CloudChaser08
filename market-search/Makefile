all: drop create claims service

create:
	psql -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER} -f create_table.sql

drop:
	psql -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER} -f drop_table.sql

store-s3:
	aws s3 cp patient-jsonpaths.json s3://healthveritydev/jsnavely/marketplace-search/
	aws s3 cp marketplace-jsonpaths.json s3://healthveritydev/jsnavely/marketplace-search/

patients:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER}\
	  -f copy_patients.sql

market:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER}\
	  -f copy_market_search.sql


