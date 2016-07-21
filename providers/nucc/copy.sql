
COPY nucc_taxonomy FROM 's3://healthverity/incoming/nucc/nucc_taxonomy_151.csv'
WITH CREDENTIALS :'AWS_CREDENTIALS'
TRUNCATECOLUMNS
CSV
COMPUPDATE ON;
