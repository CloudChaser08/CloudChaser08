
COPY nppes
FROM :'SOURCE'
WITH CREDENTIALS :'AWS_CREDENTIALS'
CSV
DATEFORMAT 'MM/DD/YYYY'
GZIP
COMPUPDATE ON;