CREATE EXTERNAL TABLE IF NOT EXISTS dw.lab_fact_covid_cleansed
(
    record_id	                bigint
    ,claim_id	                string
    ,hvid	                    string
    ,created	                date
    ,model_version	            string
    ,data_set	                string
    ,data_feed	                string
    ,data_vendor	            string
    ,source_version	            string
    ,patient_gender	            string
    ,patient_age	            string
    ,patient_year_of_birth	    string
    ,patient_zip3	            string
    ,patient_state	            string
    ,date_service	            date
    ,date_specimen	            date
    ,date_report	            date
    ,time_report	            timestamp
    ,loinc_code	                string
    ,lab_id	                    string
    ,test_id	                string
    ,test_number	            string
    ,test_battery_local_id	    string
    ,test_battery_std_id	    string
    ,test_battery_name	        string
    ,test_ordered_local_id	    string
    ,test_ordered_std_id	    string
    ,test_ordered_name	        string
    ,result_id	                string
    ,result	                    string
    ,result_name	            string
    ,result_unit_of_measure	    string
    ,result_desc	            string
    ,result_comments	        string
    ,ref_range	                string
    ,abnormal_flag	            string
    ,fasting_status	            string
    ,diagnosis_code	            string
    ,diagnosis_code_qual	    string
    ,diagnosis_code_priority	string
    ,procedure_code	            string
    ,procedure_code_qual	    string
    ,procedure_modifier_1	    string
    ,procedure_modifier_2	    string
    ,procedure_modifier_3	    string
    ,procedure_modifier_4	    string
    ,lab_npi	                string
    ,ordering_npi	            string
    ,payer_id	                string
    ,payer_id_qual	            string
    ,payer_name	                string
    ,payer_parent_name	        string
    ,payer_org_name	            string
    ,payer_plan_id	            string
    ,payer_plan_name	        string
    ,payer_type	                string
    ,lab_other_id	            string
    ,lab_other_qual	            string
    ,lab_address_1	            string
    ,lab_address_2	            string
    ,lab_city	                string
    ,lab_state	                string
    ,lab_zip	                string
    ,ordering_other_id	        string
    ,ordering_other_qual	    string
    ,ordering_name	            string
    ,ordering_market_type	    string
    ,ordering_specialty	        string
    ,ordering_vendor_id	        string
    ,ordering_tax_id	        string
    ,ordering_dea_id	        string
    ,ordering_ssn	            string
    ,ordering_state_license	    string
    ,ordering_upin	            string
    ,ordering_commercial_id	    string
    ,ordering_address_1	        string
    ,ordering_address_2	        string
    ,ordering_city	            string
    ,ordering_state	            string
    ,ordering_zip	            string
    ,medication_generic_name	string
    ,medication_dose	        string
    ,logical_delete_reason	    string
    ,vendor_record_id	        string
    ,covid19_ind                int
    ,claim_bucket_id            int
    ,hv_method_flag             int
)
PARTITIONED BY
(
    part_mth                    STRING
    ,part_provider              STRING
)
STORED AS PARQUET
LOCATION {table_location}