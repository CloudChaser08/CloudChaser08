
COPY emdeon_claim FROM 's3://healthveritydev/emdeonTransactions7/claims.out'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
DELIMITER '|'
COMPUPDATE ON;
