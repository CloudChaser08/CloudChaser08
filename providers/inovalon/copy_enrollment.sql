
COPY inovalon_enrollment
FROM 's3://salusv/incoming/inovalon/Enrollment/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP;

