SELECT /*+ BROADCAST(pln), BROADCAST(pas) */
    clm.claim_tcn_id	            AS	claim_number,
    clm.record_type	                AS	record_type,
    clm.payer_id	                AS	service_line_number,
    clm.coding_type	                AS	service_from_date,
    clm.received_date	            AS	service_to_date,
    clm.claim_type	                AS	place_of_service,
    column_7	                    AS	procedure_code,
    clm.payer_name	                AS	procedure_code_qual,
    column_9	                    AS	procedure_modifier_1,
    column_10	                    AS	procedure_modifier_2,
    column_11	                    AS	procedure_modifier_3,
    column_12	                    AS	procedure_modifier_4,
    column_13	                    AS	service_line_charge_amount,
    column_14	                    AS	column_14,
    clm.member_birth_year	        AS	unit_count,
    column_16	                    AS	revenue_code,
    column_17	                    AS	diagnosis_code_pointer_1,
    column_18	                    AS	diagnosis_code_pointer_2,
    clm.patient_state	            AS	diagnosis_code_pointer_3,
    clm.patient_zip3	            AS	diagnosis_code_pointer_4,
    column_21	                    AS	column_21,
    clm.patient_relationship_code	AS	column_22,
    column_23	                    AS	column_23,
    column_24	                    AS	column_24,
    clm.patient_gender_code	        AS	national_drug_code,
    clm.input_file_name             AS  input_file_name,
    --------------------------------------------------------
    --- From Payload
    --------------------------------------------------------    
    CASE
        WHEN pay.hvid is not null THEN obfuscate_hvid(pay.hvid, 'change837') 
    ELSE NULL
    END               AS pay_hvid,
    pay.gender        AS pay_gender,
    pay.yearofbirth   AS pay_yearofbirth,
    pay.threedigitzip AS pay_threedigitzip,
    pay.state         AS pay_state,
    --------------------------------------------------------
    --- From passthrough
    --------------------------------------------------------
    pas.pcn     AS pas_pcn,
    --------------------------------------------------------
    --- From plainout
    --------------------------------------------------------    
    pln.patient_gender AS pln_patient_gender
    
FROM claim clm
LEFT OUTER JOIN matching_payload     pay ON  UPPER(clm.claim_tcn_id) = UPPER(pay.claimid)
LEFT OUTER JOIN pas_tiny pas ON  UPPER(clm.claim_tcn_id) = pas.claimid
LEFT OUTER JOIN pln_tiny    pln ON  UPPER(clm.claim_tcn_id) = pln.claim_number

WHERE clm.record_type = 'S'
GROUP BY 1,
2,
3,
4,
5,
6,
7,
8,
9,
10,
11,
12,
13,
14,
15,
16,
17,
18,
19,
20,
21,
22,
23,
24,
25,
26,
27,
28,
29,
30,
31,
32,
33

