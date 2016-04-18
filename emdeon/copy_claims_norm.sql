COPY emdeon_claim_norm FROM 's3://salusv/provider/linked/marketplace/v1/medical/2016-04-15:17:15:45/unrelated/emdeon_professional.avro/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
ACCEPTANYDATE
BLANKSASNULL
EMPTYASNULL
MAXERROR 100000
COMPUPDATE ON
format as avro 'auto';
