


CREATE TABLE weather_pings (
  dev_model             VARCHAR(128),
  dev_appver            VARCHAR(12),
  dev_os                VARCHAR(64),
  dev_sdk               VARCHAR(32),
  dev_brand             VARCHAR(64),
  dev_formfactor        VARCHAR(32),
  dev_mfg               VARCHAR(64),
  dev_product           VARCHAR(64),

  factualid             VARCHAR(64),
  wxdid                 VARCHAR(64),
  pressure              VARCHAR(64),
  adid                  VARCHAR(64),
  timestamp             BIGINT,
  tags                  VARCHAR(64),
  accuracy              DECIMAL(37,32),
  tz                    INT2,
  fixtype               VARCHAR(32),
  dma                   INTEGER,
  version               INT2,
  postalcode            CHAR(2),
  profileid             VARCHAR(64),
  lon                   DECIMAL(37,32),
  platform              VARCHAR(32),
  lat                   DECIMAL(37,32),
  eventid               VARCHAR(48)
);
