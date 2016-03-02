
	exit


create:
	psql -f create_table.sql

all: drop create claims service

drop:
	psql -f drop_table.sql

import: jsonpaths
ifdef PROVIDER
ifdef SETID
	psql \
		-e \
		--set AWS_CREDENTIALS="aws_access_key_id=${AWS_ACCESS_KEY_ID};aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}" \
		--set PROVIDER="$(PROVIDER)"  \
		--set SOURCE="s3://salusv/provider/$(PROVIDER)/$(PROVIDER)_$(SETID).json.gz" \
		-f copy.sql
else
	echo "ERROR: No Set ID"
endif
else
	echo "ERROR: No Provider ID"
endif

jsonpaths:
	aws s3 cp --sse AES256 jsonpaths s3://salusv/redshift/link.jsonpaths
