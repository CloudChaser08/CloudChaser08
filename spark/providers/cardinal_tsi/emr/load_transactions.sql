DROP TABLE IF EXISTS transactions_diagnosis;
CREATE EXTERNAL TABLE transactions_diagnosis (
        source             string,
        uniq_id            string,
        person_id          string,
        diagnosis_code_id  string,
        description        string,
        enc_timestamp      string
        )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES (
        'mapping.source' = 'source',
        'mapping.uniq_id' = 'uniq_id',
        'mapping.person_id' = 'person_id',
        'mapping.diagnosis_code_id' = 'diagnosis_code_id',
        'mapping.description' = 'description',
        'mapping.enc_timestamp' = 'enc_timestamp'
        )
    STORED AS TEXTFILE
    LOCATION {diagnosis_input_path}
    ;

DROP TABLE IF EXISTS transactions_medication;
CREATE EXTERNAL TABLE transactions_medication (
        source              string,
        uniq_id             string,
        person_id           string,
        ndc_id              string,
        start_date          string,
        date_stopped        string,
        rx_quantity         string,
        rx_refills          string,
        generic_ok_ind      string,
        date_last_refilled  string,
        sig_desc            string,
        rx_units            string
        )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES (
        'mapping.source' = 'source',
        'mapping.uniq_id' = 'uniq_id',
        'mapping.person_id' = 'person_id',
        'mapping.ndc_id' = 'ndc_id',
        'mapping.start_date' = 'start_date',
        'mapping.date_stopped' = 'date_stopped',
        'mapping.rx_quantity' = 'rx_quantity',
        'mapping.rx_refills' = 'rx_refills',
        'mapping.generic_ok_ind' = 'generic_ok_ind',
        'mapping.date_last_refilled' = 'date_last_refilled',
        'mapping.sig_desc' = 'sig_desc',
        'mapping.rx_units' = 'rx_units'
        )
    STORED AS TEXTFILE
    LOCATION {medication_input_path}
    ;
