# Importing weather locatation data into redshift

create table has schema

jsonpaths is necessary to use COPY from JSON into redshift

COPY weather_pings FROM 's3://bucketpath/data'
with credentials 'aws_access_key_id=XXX;aws_secret_access_key=XXX'
JSON as 's3://bucketpath/jsonpaths';
