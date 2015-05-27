
drop: 
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f drop_table.sql

create:
	psql -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER} -f create_table.sql

import:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy.sql

member:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_member.sql

enrollment:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_enrollment.sql

provider:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_provider.sql

rxclaim:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_rxclaim.sql

labclaim:
	psql \
	  --set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
	  -h ${RS_HOST} -p 5439 -d dev -U ${RS_USER}\
	  -f copy_labclaim.sql


all: drop create import
