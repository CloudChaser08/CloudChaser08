
COPY inovalon_labclaim
FROM 's3://salusv/incoming/inovalon/LabClaim/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP;
