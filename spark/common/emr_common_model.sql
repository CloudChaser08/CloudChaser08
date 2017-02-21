DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        record_id                    int,
        hvid                         string,
        created                      date,
        model_version                string,
        data_set                     string,
        data_feed                    string,
        data_vendor                  string,
        source_version               string,
        claim_type                   string,
        claim_id                     string,
        claim_qual                   string,
        claim_date                   string,
        claim_error_ind              string,
        patient_age                  string,
        patient_year_of_birth        string,
        patient_date_of_death        string,
        patient_zip                  string,
        patient_state                string,
        patient_deceased_flag        string,
        patient_gender               string,
        patient_race                 string,
        patient_ethnicity            string,
        provider_client_id_qual      string,
        provider_client_id           string,
        provider_rendering_id_qual   string,
        provider_rendering_id        string,
        provider_referring_id_qual   string,
        provider_referring_id        string,
        provider_billing_id_qual     string,
        provider_biling_id           string,
        provider_facility_id_qual    string,
        provider_facility_id         string,
        provider_ordering_id_qual    string,
        provider_ordering_id         string,
        provider_lab_id_qual         string,
        provider_lab_id              string,
        provider_pharmacy_id_qual    string,
        provider_pharmacy_id         string,
        provider_prescriber_id_qual  string,
        provider_prescriber_id_      string,
        payer_id_qual                string,
        payer_id                     string,
        payer_type                   string,
        payer_parent                 string,
        payer_name                   string,
        plan_name                    string,
        encounter_id                 string,
        encounter_id_qual            string,
        description                  string,
        description_qual             string,
        date_start                   date,
        date_end                     date,
        date_qual                    string,
        diagnosis_code               string,
        diagnosis_code_qual          string,
        diagnosis_code_priority      string,
        procedure_code               string,
        procedure_code_qual          string,
        procedure_code_modifier      string,
        procedure_code_priority      string,
        ndc_code                     string,
        ndc_code_qual                string,
        loinc_code                   string,
        other_code                   string,
        other_code_qual              string,
        other_code_modifier          string,
        other_code_mod_qual          string,
        other_code_priority          string,
        type                         string,
        type_qual                    string,
        category                     string,
        category_qual                string,
        panel                        string,
        panel_qual                   string,
        specimen                     string,
        specimen_qual                string,
        method                       string,
        method_qual                  string,
        result                       string,
        result_qual                  string,
        reason                       string,
        reason_qual                  string,
        ref_range                    string,
        ref_range_qual               string,
        abnormal                     string,
        abnormal_qual                string,
        uom                          string,
        uom_qual                     string,
        severity                     string,
        severity_qual                string,
        status                       string,
        status_qual                  string,
        units                        string,
        units_qual                   string,
        qty_dispensed                string,
        qty_dispensed_qual           string,
        days_supply                  string,
        elapsed_days                 string,
        num_refills                  string,
        route                        string,
        sig                          string,
        frequency_units              string,
        frequency_times              string,
        frequency_uom                string,
        dose                         string,
        dose_qual                    string,
        strength                     string,
        form                         string,
        sample                       string,
        unverified                   string,
        electronicrx                 string
        )
{properties}
