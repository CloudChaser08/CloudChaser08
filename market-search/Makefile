
create:
	psql -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER} -f create_table.sql

createrx:
	psql -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER} -f create_rx_table.sql


all: drop create claims service

drop: 
	psql -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER} -f drop_table.sql

patients:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_MKT_HOST} -p 5439 -d dev -U ${RS_MKT_USER}\
	  -f copy_patients.sql


