COPY emdeon_claim_norm FROM 's3://salusv/provider/linked/marketplace/v1/medical/2016-04-14:18:56:40/related/emdeon_professional.avro/part-r-00000-7520b479-699c-47a9-b6e3-3c2e1894d65a.avro'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
ACCEPTANYDATE
BLANKSASNULL
EMPTYASNULL
MAXERROR 100000
COMPUPDATE ON
format as avro 'auto';
