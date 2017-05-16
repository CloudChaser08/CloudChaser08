DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        record_id               bigint,
        hvid                    string,
        created                 date,
        model_version           string,
        data_set                string,
        data_feed               string,
        data_vendor             string,
        source_version          string,
        patient_age             string,
        patient_year_of_birth   string,
        patient_zip             string,
        patient_state           string,
        patient_gender          string,
        source_record_id        string,
        source_record_qual      string,
        source_record_date      date,
        date_start              date,
        date_end                date,
        benefit_type            string
        )
    {properties}
;
