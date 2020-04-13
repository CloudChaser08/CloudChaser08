SELECT
     unmatched.record_id,
     unmatched.claim_id,
     COALESCE(rx_pay.hvid, unmatched.hvid) as hvid,
     unmatched.created,
     unmatched.model_version,
     unmatched.data_set,
     unmatched.data_feed,
     unmatched.data_vendor,
     /* patient_gender */
     CASE
         WHEN SUBSTR(UPPER(rx_pay.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(rx_pay.gender), 1, 1)
         ELSE 'U'
     END AS patient_gender,
     /* patient_year_of_birth */
        cap_year_of_birth
            (
                NULL,
                CAST(unmatched.date_service AS DATE),
                rx_pay.year_of_birth
            ) AS patient_year_of_birth,
     /* patient_zip3 */
     MASK_ZIP_CODE(SUBSTR(rx_pay.zip, 1, 3)) AS patient_zip3,
     unmatched.date_service,
     unmatched.date_service_end,
     unmatched.inst_discharge_status_std_id,
     unmatched.place_of_service_std_id,
     unmatched.diagnosis_code,
     unmatched.diagnosis_code_qual,
     unmatched.diagnosis_priority,
     unmatched.procedure_code,
     unmatched.procedure_code_qual,
     unmatched.procedure_modifier_1,
     unmatched.procedure_modifier_2,
     unmatched.procedure_modifier_3,
     unmatched.procedure_modifier_4,
     unmatched.revenue_code,
     unmatched.prov_billing_npi,
     unmatched.prov_billing_name_1,
     unmatched.prov_billing_address_1,
     unmatched.prov_billing_address_2,
     unmatched.prov_billing_city,
     unmatched.prov_billing_state,
     unmatched.prov_billing_zip,
     unmatched.part_best_date,
     unmatched.part_provider,
     unmatched.file_date,
     unmatched.patient_id

FROM historic_unmatched_records unmatched
JOIN local_phi rx_pay ON unmatched.patient_id = rx_pay.patient_id