
COPY inovalon_rxclaim
FROM 's3://salusv/incoming/inovalon/RxClaim/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP;
