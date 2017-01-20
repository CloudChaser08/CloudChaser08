-- barebones EMR model that can only be used for making marketplace
-- searchable

-- TODO: Finalize EMR model and migrate existing EMR providers to a
-- better model
DROP TABLE IF EXISTS emr_common_model;
CREATE TABLE emr_common_model (
    record_id       bigint IDENTITY(0,1),
    hvid            text ENCODE lzo,
    patient_gender  text ENCODE lzo,
    patient_state   text ENCODE lzo,
    patient_age     text ENCODE lzo,
    drug            text ENCODE lzo,
    diagnosis       text ENCODE lzo,
    lab             text ENCODE lzo,
    procedure       text ENCODE lzo
    )
