
COPY walgreens_stores 
FROM 's3://healthverity/incoming/walgreens' 
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV IGNOREHEADER 1
