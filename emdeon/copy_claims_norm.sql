COPY emdeon_claim_norm FROM 's3://salusv/provider/linked/marketplace/v1/medical/2016-03-10:10:00:18/normalizedClaims.avro'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
MAXERROR 50
COMPUPDATE ON
format as avro 'auto';
