
COPY inovalon_provider
FROM 's3://salusv/incoming/inovalon/Provider/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP
QUOTE '}'
COMPUPDATE ON;

COPY inovalon_provider_supplemental
FROM 's3://salusv/incoming/inovalon/ProviderSupplemental/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV 
DATEFORMAT 'YYYY-MM-DD'
DELIMITER '	' 
GZIP
QUOTE '}'
COMPUPDATE ON;
