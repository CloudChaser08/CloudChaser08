
COPY nppes FROM 's3://healthverity/incoming/nppes/20050523-20150809/npi_part'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV
DATEFORMAT 'MM/DD/YYYY'
GZIP
COMPUPDATE ON;
