COPY patients FROM 's3://healthveritydev/jsnavely/unique_emdeon_claims'
WITH CREDENTIALS :'AWS_CREDENTIALS'
format as json 's3://healthveritydev/jsnavely/marketplace-search/patient-jsonpaths.json'
MAXERROR 50
COMPUPDATE ON;
