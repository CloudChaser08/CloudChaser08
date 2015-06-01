
COPY weather_pings FROM 's3://salusv/provider/weatherchannel/'
WITH CREDENTIALS :'AWS_CREDENTIALS'
JSON as 's3://vhxwdata/jsonpaths'
TIMEFORMAT AS 'epochmillisecs'
ROUNDEC
COMPUPDATE ON
MAXERROR 20
GZIP;
