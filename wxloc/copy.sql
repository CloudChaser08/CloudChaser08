
COPY weather_pings FROM 's3://healthverity/incoming/weatherchannel' 
WITH CREDENTIALS :'AWS_CREDENTIALS'
JSON as 's3://vhxwdata/jsonpaths'
TIMEFORMAT AS 'epochmillisecs'
ROUNDEC;
