DROP TABLE IF EXISTS demographics_transactions;
CREATE TABLE demographics_transactions (
        id                               string,
        import_source_id                 string,
        practice_id                      string,
        system_id                        string,
        patient_id                       string,
        first_name                       string,
        last_name                        string,
        birth_date                       string,
        gender                           string,
        race                             string,
        patient_address_one              string,
        patient_address_two              string,
        patient_address_three            string,
        city                             string,
        state                            string,
        zip_code                         string,
        zip_code_plus_four               string,
        date_of_death                    string,
        middle_initial                   string,
        middle_name                      string,
        ethnicity                        string,
        home_phone                       string,
        home_phone_note                  string,
        marital_status                   string,
        ssn                              string,
        title                            string,
        patient_language                 string,
        mobile_phone                     string,
        mobile_phone_note                string,
        work_phone                       string,
        work_phone_note                  string,
        smoker_status                    string,
        alcohol_use                      string,
        patient_weight                   string,
        patient_height                   string,
        country                          string,
        doctor_name                      string,
        bsa                              string,
        first_appointment_date           string,
        suffix                           string,
        icd_nine                         string,
        icd_ten                          string,
        patient_alive_indicator          string,
        test_patient_indicator           string,
        maiden_name                      string,
        clinical_trial_indicator         string,
        patient_age                      string,
        allergy_indicator                string,
        infection_indicator              string,
        inpatient_indicator              string,
        county                           string,
        faxphone                         string,
        cur_entry                        string,
        email_address                    string,
        occupation                       string,
        educational_level                string,
        present_employer_name            string,
        moms_full_name                   string,
        fathers_full_name                string,
        moms_maiden_name                 string,
        birth_place                      string,
        location_code                    string,
        date_inactivated                 string,
        date_reactivated                 string,
        order_payor_type                 string,
        order_payor_name                 string,
        drivers_license_number           string,
        drivers_license_state            string,
        allergen_description             string,
        plan_one_name                    string,
        primary_coverage_type            string,
        plan_one_pay_method_type_text    string,
        plan_one_priority                string,
        plan_one_start_date              string,
        plan_one_end_date                string,
        plan_one_card_holder_id          string,
        plan_one_group_number            string,
        plan_one_expiration_date         string,
        plan_one_group_code              string,
        plan_two_name                    string,
        secondary_coverage_type          string,
        plan_two_pay_method_type_text    string,
        plan_two_priority                string,
        plan_two_start_date              string,
        plan_two_end_date                string,
        plan_two_card_holder_id          string,
        plan_two_group_number            string,
        plan_two_expiration_date         string,
        plan_two_group_code              string,
        plan_three_name                  string,
        tertiary_coverage_type           string,
        plan_three_pay_method_type_text  string,
        plan_three_priority              string,
        plan_three_start_date            string,
        plan_three_end_date              string,
        plan_three_card_holder_id        string,
        plan_three_group_number          string,
        plan_three_expiration_date       string,
        plan_three_group_code            string,
        hvJoinKey                        string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {demographics_input_path}
    ;

DROP TABLE IF EXISTS demographics_transactions_dedup;
CREATE TABLE demographics_transactions_dedup AS (
    SELECT dem.*
    FROM demographics_transactions dem
        INNER JOIN (
        SELECT patient_id, MAX(hvJoinKey) AS hvJoinKey
        FROM demographics_transactions
        GROUP BY patient_id
            ) upi ON dem.patient_id = upi.patient_id
        AND dem.hvJoinKey = upi.hvJoinKey
    WHERE dem.import_source_id IS NOT NULL
        )
    ;

DROP TABLE IF EXISTS diagnosis_transactions;
CREATE TABLE diagnosis_transactions (
        id                   string,
        import_source_id     string,
        practice_id          string,
        system_id            string,
        patient_id           string,
        diagnosis_id         string,
        icd_cd               string,
        stage_of_disease     string,
        diagnosis_date       string,
        diagnosis_typ        string,
        confirm_diagnosis    string,
        resolution_date      string,
        diagnosis_name       string,
        valid_entry          string,
        diagnosis_desc       string,
        mthd_of_diagnosis    string,
        resolution_desc      string,
        stg_crit_desc        string,
        cur_entry_ind        string,
        clinical_desc        string,
        diagnosis_cmt        string,
        vision_diagnosis_id  string,
        provider_npi         string,
        icd10                string,
        diag_group           string,
        cancer_stage         string,
        cancer_stage_t       string,
        cancer_stage_n       string,
        cancer_stage_m       string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {diagnosis_input_path}
    ;

DROP TABLE IF EXISTS encounter_transactions;
CREATE TABLE encounter_transactions (
        id                          string,
        import_source_id            string,
        practice_id                 string,
        system_id                   string,
        patient_id                  string,
        patient_visit_id            string,
        visit_type                  string,
        visit_date                  string,
        visit_complete              string,
        inst_id                     string,
        userid_link                 string,
        treatment_plan_name         string,
        treatment_plan_vers_no      string,
        date_treatment_plan_init    string,
        cycle_no                    string,
        cycle_day                   string,
        visit_inst_id               string,
        sch_cmt                     string,
        visit_start_tstamp          string,
        visit_end_tstamp            string,
        cancel_reason_type          string,
        patient_instr_cmt           string,
        visit_financial_status_cmt  string,
        treatment_name              string,
        treatment_description       string,
        cpt                         string,
        visit_billed_amount         string,
        visit_unit_of_measure       string,
        claim_id                    string,
        contract_amt                string,
        reimburse_amt               string,
        provider_npi                string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {encounter_input_path}
    ;

DROP TABLE IF EXISTS lab_transactions;
CREATE TABLE lab_transactions (
        id                                string,
        import_source_id                  string,
        practice_id                       string,
        system_id                         string,
        patient_id                        string,
        test_id                           string,
        test_name                         string,
        test_date                         string,
        test_result_group_id              string,
        test_result_id                    string,
        test_type                         string,
        test_name_specific                string,
        test_value                        string,
        test_value_string                 string,
        test_value_txt                    string,
        min_norm                          string,
        max_norm                          string,
        unit_of_measure                   string,
        unit_desc                         string,
        min_reason                        string,
        max_reason                        string,
        valid_entry_ind                   string,
        abnormal_flag_cd                  string,
        notes                             string,
        physician_id                      string,
        cpt                               string,
        delivery_cpt                      string,
        record_source_rto_execution_date  string,
        end_date                          string,
        record_source                     string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {lab_input_path}
    ;

DROP TABLE IF EXISTS dispense_transactions;
CREATE TABLE dispense_transactions (
        id                            string,
        import_source_id              string,
        rto_written_prescription_id   string,
        practice_id                   string,
        system_id                     string,
        patient_id                    string,
        visit_id                      string,
        prescription_number           string,
        item_no                       string,
        agt_seq_no                    string,
        erx_agt_rx_id                 string,
        ordersall_medications_flag    string,
        edw_evaluated_ind             string,
        npi                           string,
        brand_name                    string,
        generic_name                  string,
        duration                      string,
        admin_date                    string,
        num_doses                     string,
        icd_nine                      string,
        icd_ten                       string,
        qty                           string,
        days_supply                   string,
        refills                       string,
        strength                      string,
        agt_rx_strength               string,
        ndc_written                   string,
        ndc_labeler_code              string,
        ndc_drug_and_strength         string,
        ndc_package_size              string,
        quantity_per_day              string,
        quantity_units                string,
        instructions                  string,
        discontinue_date              string,
        type                          string,
        create_date                   string,
        created_by_name               string,
        ordered_by_name               string,
        planned_duration              string,
        planned_unit                  string,
        actual_unit                   string,
        planned_value                 string,
        actual_value                  string,
        route                         string,
        waste                         string,
        cpt_code                      string,
        rx_cmt                        string,
        valid_entry_ind               string,
        treatment_plan_name           string,
        treatment_plan_vers_no        string,
        phase_seq_no                  string,
        cycle_no                      string,
        cycle_day                     string,
        completed_ind                 string,
        dispensed_ind                 string,
        treatment_start_date          string,
        treatment_line                string,
        treatment_intent              string,
        treatment_use                 string,
        rx_calc_audit_desc            string,
        err_rsn_txt                   string,
        dosage_form                   string,
        dose_level                    string,
        rx_total                      string,
        admn_dosage_unit              string,
        admn_route                    string,
        admn_dose_frq_unit            string,
        admn_frq_x                    string,
        admn_frq_unit                 string,
        admn_dur_unit                 string,
        general_orders                string,
        discontinue_reason            string,
        discontinue_effective_date    string,
        rx_dose_range                 string,
        strength_unit                 string,
        set_date_treatment_plan_init  string,
        course_desc                   string,
        order_unit                    string,
        order_dose                    string,
        infusion_typ                  string,
        infusion_duration             string,
        infusion_timescale            string,
        dose_strength                 string,
        agt_vol                       string,
        agt_vol_uom                   string,
        dose_pct                      string,
        dispense_vol                  string,
        dispense_unit                 string,
        admn_dose_no_rec              string,
        admn_dose_total               string,
        reason_desc                   string,
        inst_id                       string,
        disp_qty                      string,
        dest_pharm_id                 string,
        sent_ind                      string,
        received_ind                  string,
        received_date                 string,
        drug_description              string,
        dose_form_description         string,
        prescribed_else_ind           string,
        uom_description               string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {dispense_input_path}
    ;
