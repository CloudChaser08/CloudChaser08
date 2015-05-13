
COPY pitney_bowes_locations 
FROM 's3://healthverity/incoming/pitneybowes' 
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV IGNOREHEADER 1
QUOTE '%'
DELIMITER '|' 
GZIP
