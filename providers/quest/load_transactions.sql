DROP TABLE IF EXISTS transactional_raw;
CREATE TABLE transactional_raw (
        accn_id              text ENCODE lzo,
        dosid                text ENCODE lzo,
        local_order_code     text ENCODE lzo,
        standard_order_code  text ENCODE lzo,
        order_name           text ENCODE lzo,
        loinc_code           text ENCODE lzo,
        local_result_code    text ENCODE lzo,
        result_name          text ENCODE lzo,
        date_of_service      text ENCODE lzo,
        date_collected       text ENCODE lzo,
        diagnosis_code       text ENCODE lzo,
        icd_codeset_ind      text ENCODE lzo
        ) DISTKEY(accn_id) SORTKEY(accn_id);

COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2015/08' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2015/09' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2015/10' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2015/11' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2015/12' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/01' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/02' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/03' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/04' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/05' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/06' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/07' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
COPY transactional_raw FROM 's3://healthverity/incoming/quest/merged/2016/08' CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
