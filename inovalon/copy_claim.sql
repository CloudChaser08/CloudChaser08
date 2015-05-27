
COPY inovalon_claim_raw
FROM 's3://salusv/incoming/inovalon/Claim/2012/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP;

COPY inovalon_claim_code_raw
FROM 's3://salusv/incoming/inovalon/ClaimCode/2012/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP;
