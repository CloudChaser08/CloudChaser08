CREATE TABLE tmobile_pings (
  created               TIMESTAMP,
  msisdn                CHAR(64) distkey,
  lat                   DECIMAL(14,10),
  lon                   DECIMAL(14,10),
  cellname              VARCHAR(32)
)
SORTKEY(created, lat, lon);
