
COPY emdeon_service FROM 's3://healthveritydev/emdeonTransactions8/service.out'
WITH CREDENTIALS :'AWS_CREDENTIALS'
DATEFORMAT 'YYYYMMDD'
ACCEPTANYDATE
DELIMITER '|'
MAXERROR 50
COMPUPDATE ON;