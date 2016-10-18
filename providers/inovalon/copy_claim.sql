
COPY inovalon_claim_raw
FROM 's3://salusv/incoming/inovalon/Claim/2014/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
COMPUPDATE ON
GZIP;

COPY inovalon_claim_code_raw
FROM 's3://salusv/incoming/inovalon/ClaimCode/2014/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
COMPUPDATE ON
GZIP;