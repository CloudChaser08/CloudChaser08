
COPY weather_pings FROM 's3://salusv/provider/weatherchannel/2015/03'
WITH CREDENTIALS :'AWS_CREDENTIALS'
JSON as 's3://vhxwdata/jsonpaths'
TIMEFORMAT AS 'epochmillisecs'
ROUNDEC
COMPUPDATE ON
GZIP;
