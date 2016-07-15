CREATE TABLE tmobile_pings (
  created               TIMESTAMP encode lzo,
  msisdn                CHAR(64) distkey encode lzo,
  lat                   DECIMAL(14,10) encode lzo,
  lon                   DECIMAL(14,10) encode lzo,
  cellname              VARCHAR(32) encode lzo
)
SORTKEY(created, lat, lon);
