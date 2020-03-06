DROP TABLE IF EXISTS transactions;
CREATE EXTERNAL TABLE transactions
(
    unique_patient_id               STRING,
    medical_claim_id                STRING,
    client_med_availability_col     STRING,
    record_type                     STRING,
    claim_record_seq_num            STRING,
    medical_code                    STRING,
    medical_qualifier_code          STRING,
    place_of_service_code           STRING,
    cpt_modifier_code_01            STRING,
    cpt_modifier_code_02            STRING,
    cpt_modifier_code_03            STRING,
    cpt_modifier_code_04            STRING,
    patient_status_code             STRING,
    provider_first_name             STRING,
    provider_middle_name            STRING,
    provider_last_name              STRING,
    provider_name_suffix_text       STRING,
    professional_title_text         STRING,
    provider_npi_number             STRING,
    provider_street_addr_line_1     STRING,
    provider_street_addr_line_2     STRING,
    provider_city_name              STRING,
    provider_state_or_province_code STRING,
    provider_postal_code            STRING,
    provider_fax_number             STRING,
    provider_phone_number           STRING,
    first_serviced_date             STRING,
    last_serviced_date              STRING,
    transaction_code                STRING,
    hvjoinkey                       STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES
    (
        "input.regex" = "^(.{{15}})(.{{23}})(.{{72}})(.{{3}})(.{{4}})(.{{15}})(.{{2}})(.{{3}})(.{{2}})(.{{2}})(.{{2}})(.{{2}})(.{{2}})(.{{25}})(.{{25}})(.{{35}})(.{{10}})(.{{15}})(.{{10}})(.{{40}})(.{{40}})(.{{30}})(.{{2}})(.{{15}})(.{{35}})(.{{15}})(.{{8}})(.{{8}})(.{{1}})(.*)"
    )
STORED AS TEXTFILE
LOCATION {input_path}
