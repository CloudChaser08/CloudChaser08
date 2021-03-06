
CREATE TABLE weather_pings (
  dev_model             VARCHAR(64),
  dev_appver            VARCHAR(8),
  dev_os                VARCHAR(100),
  dev_sdk               VARCHAR(8),
  dev_brand             VARCHAR(32),
  dev_formfactor        VARCHAR(8),
  dev_mfg               VARCHAR(64),
  dev_product           VARCHAR(64),
  factualid             VARCHAR(64),
  wxdid                 VARCHAR(42),
  pressure              VARCHAR(32),
  adid                  VARCHAR(40),
  timestamp             TIMESTAMP,
  tags                  VARCHAR(40),
  accuracy              DECIMAL(18,4),
  tz                    INT2,
  fixtype               VARCHAR(28),
  dma                   INT2,
  version               INT2,
  postalcode            CHAR(8),
  profileid             VARCHAR(64),
  lon                   DECIMAL(14,10),
  platform              VARCHAR(12),
  lat                   DECIMAL(14,10),
  eventid               VARCHAR(36)
)
SORTKEY(timestamp, lat, lon);
