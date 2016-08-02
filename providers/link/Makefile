
export PGHOST=${RS_HOST}
export PGUSER=${RS_USER}
export PGPORT=5439
export PGDATABASE=dev
AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}"

S3SRC='s3://salusv/matching/prodgz/payload/86396771-0345-4d67-83b3-7e22fded9e1d/20160724-to-20160730/'

create:
	psql -f create_table.sql

drop:
	psql -f drop_table.sql

import: jsonpaths
	psql \
	  --set AWS_CREDENTIALS=${AWS_CREDENTIALS} \
	  --set SOURCE=${S3SRC} \
	  -f copy.sql

link:
	psql \
	  --set AWS_CREDENTIALS=${AWS_CREDENTIALS} \
	  -f link.sql

unload:
	psql \
	  --set AWS_CREDENTIALS=${AWS_CREDENTIALS} \
	  --set DEST="s3://salusv/link_dump2/" \
	  -f unload.sql

jsonpaths:
	aws s3 cp --sse AES256 link.jsonpaths s3://salusv/redshift/link.jsonpaths

all: drop create import
