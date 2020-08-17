SELECT 
     column1 AS claim_payment_number,
     column2 AS record_type,
     column3 AS payer_claim_control_number,
     column4 AS payer_id,
     column5 AS claim_status,
     column6 AS chc_transaction_id,
     column7 AS claim_received_by_payer_date,
     column8 AS chc_purpose_cd,
     column9 AS payer_name,
     column10 AS billing_pr_id,
     column11 AS billing_pr_npi,
     column12 AS billing_pr_name,
     column13,
     column14 AS billing_pr_adr_line1,
     column15 AS billing_pr_adr_line2,
     column16 AS billing_pr_adr_city,
     column17 AS billing_pr_adr_state,
     column18 AS billing_pr_adr_zip,
     column19,
     column20,
     column21,
     column22,
     column23,
     column24 AS rendering_pr_npi,
     column25 AS rendering_pr_last_or_org_name,
     column26 AS rendering_pr_first_name,
     column27,
     column28,
     column29,
     column30,
     column31,
     column32,
     column33,
     column34,
     column35 AS statement_from_date,
     column36 AS statement_to_date,
     column37 AS total_paid_amt,
     column38 AS total_claim_charge_amount,
     column39 AS patient_responsibility_amount,
     column40 AS drg_code,
     column41 AS type_of_bill,
     column42 AS patient_amount_paid,
     column43,
     column44,
     column45,
     column46,
     column47,
     column48,
     column49,
     column50 AS column50,
     column51 AS column51,
     column52 AS column52,
     column53 AS column53,
     column54 AS column54,
     column55 AS column55,
     column56 AS column56,
     column57 AS column57,
     column58 AS column58,
     column59 AS column59,
     column60 AS column60,
     column61 AS type_of_coverage,
     column62 AS column62,
     column63 AS group_code_1,
     column64 AS reason_code_1,
     column65 AS adjustment_amount_1,
     column66 AS adjustment_quantity_1,
     column67 AS group_code_2,
     column68 AS reason_code_2,
     column69 AS adjustment_amount_2,
     column70 AS adjustment_quantity_2,
     column71 AS group_code_3,
     column72 AS reason_code_3,
     column73 AS adjustment_amount_3,
     column74 AS adjustment_quantity_3,
     column75 AS group_code_4,
     column76 AS reason_code_4,
     column77 AS adjustment_amount_4,
     column78 AS adjustment_quantity_4,
     column79 AS group_code_5,
     column80 AS reason_code_5,
     column81 AS adjustment_amount_5,
     column82 AS adjustment_quantity_5,
     input_file_name

FROM change_835_raw
WHERE  column2 = 'C'
