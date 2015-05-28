
COPY inovalon_claim_raw
FROM 's3://salusv/incoming/inovalon/Claim/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
COMPUPDATE ON
GZIP;

COPY inovalon_claim_code_raw
FROM 's3://salusv/incoming/inovalon/ClaimCode/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
COMPUPDATE ON
GZIP;
