
COPY docgraph_60day
FROM 's3://salusv/provider/docgraph/60days/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
COMPUPDATE ON
GZIP


COPY docgraph_procedure
FROM 's3://salusv/provider/docgraph/Procedures/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DELIMITER '	'
COMPUPDATE ON
GZIP
IGNOREHEADER 1
