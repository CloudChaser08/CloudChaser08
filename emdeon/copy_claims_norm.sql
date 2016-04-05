COPY emdeon_claim_norm FROM 's3://salusv/provider/linked/marketplace/v1/medical/2016-04-04:11:50:02/emdeon_professional.avro/part-r-00000-6fc1937b-8f23-4a4e-8bd3-a82696276b18.avro'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
MAXERROR 100000
COMPUPDATE ON
format as avro 'auto';
