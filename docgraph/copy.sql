
COPY docgraph_60day
FROM 's3://salusv/provider/docgraph/60days/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
COMPUPDATE ON
GZIP
