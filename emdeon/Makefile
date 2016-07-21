
create_norm:
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f create_normalized_table.sql

create:
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f create_table.sql

createrx:
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f create_rx_table.sql


all: drop create claims service

drop: 
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f drop_table.sql

claims:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_claims.sql

claims_norm:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  --set S3_INPUT_PATH="s3://salusv/provider/linked/marketplace/v1/medical/normalized_csv/20160710-to-20160716/" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_claims_norm.sql

rx_norm:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  --set S3_INPUT_PATH="s3://salusv/provider/linked/emdeon/v1/pharmacy/2016/07/18/2016-07-18:10:58:40/rx.csv/" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} \
	  -f copy_rx_norm.sql

service:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_service.sql

diag:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_diag.sql
rx:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_rx.sql

