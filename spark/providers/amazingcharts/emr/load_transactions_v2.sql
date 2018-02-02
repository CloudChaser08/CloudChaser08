DROP TABLE IF EXISTS d_costar;
CREATE EXTERNAL TABLE d_costar (
        costar_key                        string,
        full_code                         string,
        base_code                         string,
        code_to_store                     string,
        concept_name                      string,
        semantic_type                     string,
        base_code_onto_by_icd_9cm         string,
        representative_icd_9cm_code       string,
        icd_9cm_set                       string,
        representative_icd_10cm_code      string,
        representative_icd_10cm_branch    string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_costar'
    ;

DROP TABLE IF EXISTS d_cpt;
CREATE EXTERNAL TABLE d_cpt (
        cpt_key                           string,
        cpt_id                            string,
        cpt_code                          string,
        cpt_description                   string,
        cpt_short_description             string,
        cpt_common                        string,
        fee                               string,
        rvu                               string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_cpt'
    ;

DROP TABLE IF EXISTS d_date;
CREATE EXTERNAL TABLE d_date (
        date_key                          string,
        date_time                         string,
        date_yyyy                         string,
        date_mm                           string,
        date_month                        string,
        date_ww                           string,
        date_dd                           string,
        date_day_of_week                  string,
        date_quarter                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_date'
    ;

DROP TABLE IF EXISTS d_drug;
CREATE EXTERNAL TABLE d_drug (
        drug_key                          string,
        drug_id                           string,
        drug_sub_id_1                     string,
        drug_name                         string,
        drug_route_id                     string,
        drug_dosage_form_id               string,
        strength                          string,
        strength_uom                      string,
        route                             string,
        dosage_form                       string,
        generic_name                      string,
        dea_generic_named_code            string,
        dea_legend_code                   string,
        theraputic_category               string,
        drug_info                         string,
        generic_drug_name_override        string,
        manufacturer                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_drug'
    ;

DROP TABLE IF EXISTS d_icd10;
CREATE EXTERNAL TABLE d_icd10 (
        icd10_key                         string,
    order_number                          string,
        icd_10_code                       string,
        is_complete                       string,
        short_description                 string,
        long_description                  string,
        is_active                         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_icd10'
    ;

DROP TABLE IF EXISTS d_icd9;
CREATE EXTERNAL TABLE d_icd9 (
        icd9_key                          string,
        id                                string,
        code                              string,
        short_description                 string,
        shorter_description               string,
        description                       string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_icd9'
    ;

DROP TABLE IF EXISTS d_lab_directory;
CREATE EXTERNAL TABLE d_lab_directory (
        practice_key                      string,
        lab_directory_key                 string,
        test_code                         string,
        lab_company                       string,
        test_name                         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}lab_d_lab_directory'
    ;

DROP TABLE IF EXISTS d_patient;
CREATE EXTERNAL TABLE d_patient (
        patient_key                       string,
        practice_key                      string,
        gender                            string,
        birth_year                        string,
        state                             string,
        zip                               string,
        inactive                          string,
        reason_inactive                   string,
        marital_status                    string,
        takes_no_meds                     string,
        patient_race                      string,
        language_preference               string,
        ethnicity_id                      string,
        date_of_death                     string,
        state_code                        string,
        ethnicity_description             string,
        race_description                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_patient'
    ;

DROP TABLE IF EXISTS d_provider;
CREATE EXTERNAL TABLE d_provider (
        provider_key                      string,
        practice_key                      string,
        degree                            string,
        state                             string,
        specialty                         string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_provider'
    ;

DROP TABLE IF EXISTS d_time;
CREATE EXTERNAL TABLE d_time (
        time_key string,
        hh24mi string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}d_time'
    ;

DROP TABLE IF EXISTS d_vaccine_cpt;
CREATE EXTERNAL TABLE d_vaccine_cpt (
        vaccine_cpt_key                   string,
        vaccine_cpt_id                    string,
        vaccine_name                      string,
        vaccine_sub_component             string,
        cpt_code                          string,
        cvx_code                          string,
        country                           string,
        unique_id                         string,
        date_last_touched                 string,
        is_generic                        string,
        cvx_code_unspecified_formulation  string,
        immunity_code                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}vaccine_d_vaccine_cpt'
    ;

DROP TABLE IF EXISTS f_diagnosis;
CREATE EXTERNAL TABLE f_diagnosis (
        date_key                          string,
        time_key                          string,
        record_type                       string,
        practice_key                      string,
        patient_key                       string,
        problem_icd                       string,
        icd_type                          string,
        date_active                       string,
        date_inactive                     string,
        provider_key                      string,
        date_row_added                    string,
        date_last_activated               string,
        date_resolved                     string,
        costar_key                        string,
        snomed                            string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}f_diagnosis'
    ;

DROP TABLE IF EXISTS f_encounter;
CREATE EXTERNAL TABLE f_encounter (
        date_key                          string,
        time_key                          string,
        practice_key                      string,
        patient_key                       string,
        encounter_date                    string,
        chief_complaint                   string,
        history_of_present_illness        string,
        review_of_systems                 string,
        past_medical_history              string,
        current_medications               string,
        allergies                         string,
        social_history                    string,
        family_history                    string,
        physical_exam                     string,
        assessment                        string,
        plan                              string,
        blood_pressure                    string,
        temperature                       string,
        respiratory_rate                  string,
        pulse                             string,
        weight                            string,
        height                            string,
        body_mass_index                   string,
        head_circumference                string,
        vital_comments                    string,
        provider_key                      string,
        age_gender_sentence               string,
        callback_comment                  string,
        cpt_code                          string,
        cpt_comments                      string,
        tests                             string,
        date_row_added                    string,
        oxygen_saturation                 string,
        pain_scale                        string,
        pulmonary_function                string,
        other_vitals                      string,
        oxygen_saturation_room_air        string,
        supplemental_O2_amount            string,
        peak_flow_post_bronchodilator     string,
        supplemental_O2_type              string,
        packs_per_day                     string,
        years_smoked                      string,
        years_quit                        string,
        last_menstrual_period             string,
        estimated_delivery_date           string,
        pregnancy_comments                string,
        vision_os                         string,
        vision_od                         string,
        hearing                           string,
        hearing_comments                  string,
        systolic_blood_pressure_supine    string,
        diastolic_blood_pressure_supine   string,
        weight_in_pounds                  string,
        systolic                          string,
        diastolic                         string,
        height_in_inches                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}f_encounter'
    ;

DROP TABLE IF EXISTS f_injection;
CREATE EXTERNAL TABLE f_injection (
        date_key                          string,
        time_key                          string,
        record_type                       string,
        record_name                       string,
        practice_key                      string,
        patient_key                       string,
        vaccine_cpt_key                   string,
        provider_key                      string,
        lot_no                            string,
        date_given                        string,
        volume                            string,
        route                             string,
        site                              string,
        manufacturer                      string,
        expiration                        string,
        sequence                          string,
        type                              string,
        cpt                               string,
        is_given_elsewhere                string,
        patient_refused                   string,
        vis_version                       string,
        vis_date_given                    string,
        deleted                           string,
        date_sent_to_registry             string,
        patient_parent_refused            string,
        patient_had_infection             string,
        how_migrated                      string,
        reaction_date                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}f_injection'
    ;

DROP TABLE IF EXISTS f_lab;
CREATE EXTERNAL TABLE f_lab (
        date_key                          string,
        time_key                          string,
        practice_key                      string,
        lab_test_id                       string,
        patient_key                       string,
        created_date_lt                   string,
        lab_directory_key                 string,
        specimen_nbr_lt                   string,
        specimen_status                   string,
        fasting                           string,
        lab_test_status_lt                string,
        sign_off_id                       string,
        sign_off_date                     string,
        comments                          string,
        lab_result_id                     string,
        accession_nbr_ac                  string,
        ordering_provider_id              string,
        specimen_nbr_lr                   string,
        lab_test_code_lr                  string,
        specimen_volume                   string,
        specimen_collected_dt             string,
        action_code                       string,
        clinical_info                     string,
        specimen_source                   string,
        alternate_id_1                    string,
        alternate_id_2                    string,
        lab_test_status_lr                string,
        parent_for_reflex_obx             string,
        parent_for_reflex_obr             string,
        specimen_condition                string,
        lab_result_detail_id              string,
        inactive_flag                     string,
        corrects_lab_test_id              string,
        corrected_by_lab_test_id          string,
        lab_test_status_lrd               string,
        lab_test_code_lrd                 string,
        loinc_test_code                   string,
        observation_sub_id                string,
        observation_value                 string,
        uom                               string,
        reference_ranges                  string,
        abnormal_flag                     string,
        normal_abnormal_type              string,
        value_type                        string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}f_lab'
    ;

DROP TABLE IF EXISTS f_medication;
CREATE EXTERNAL TABLE f_medication (
        date_key                          string,
        time_key                          string,
        practice_key                      string,
        patient_key                       string,
        provider_key                      string,
        med_name                          string,
        med_sig                           string,
        med_no                            string,
        med_refill                        string,
        med_dns                           string,
        date_initiated                    string,
        date_last_refilled                string,
        med_comments                      string,
        prior_refills                     string,
        refillable                        string,
        inactive                          string,
        drug_key                          string,
        quick_add_reason_prescribed       string,
        deleted                           string,
        date_inactivated                  string,
        date_started                      string,
        dispense_qualifier                string,
        erx_status                        string,
        daw                               string,
        sent_by_sure_scripts              string,
        inactive_reason                   string,
        script_printed                    string,
        script_faxed                      string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}f_medication'
    ;

DROP TABLE IF EXISTS f_procedure;
CREATE EXTERNAL TABLE f_procedure (
        date_key                          string,
        time_key                          string,
        practice_key                      string,
        patient_key                       string,
        provider_key                      string,
        date_of_service                   string,
        cpt_key                           string,
        units                             string,
        price                             string,
        date_performed                    string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{input_path}f_procedure'
    ;

DROP TABLE IF EXISTS d_multum_to_ndc;
CREATE EXTERNAL TABLE d_multum_to_ndc (
        ndc string,
        drug_name string,
        multum_id string,
        active_status string,
        touch_date string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION '{d_multum_to_ndc_path}'
    ;
