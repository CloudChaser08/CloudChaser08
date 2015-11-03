COPY market_search FROM 's3://salusv/provider/linked/marketplace/v1/medical/2015-11-02:19:47:25/patients.json'
WITH CREDENTIALS :'AWS_CREDENTIALS'
format as json 's3://healthveritydev/jsnavely/marketplace-search/marketplace-jsonpaths.json'
MAXERROR 50
COMPUPDATE ON;
