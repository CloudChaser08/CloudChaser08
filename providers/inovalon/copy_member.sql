
COPY inovalon_member
FROM 's3://salusv/incoming/inovalon/Member/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP;

