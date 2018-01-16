SELECT
    NULL AS record_id,
    t.row_id AS claim_id,
    mp.hvid AS hvid,
    NULL AS created,
    NULL AS model_version,
    NULL AS data_set,
    NULL AS data_feed,
    NULL AS data_vendor,
    NULL AS source_version,
    CASE
      WHEN UPPER(COALESCE(mp.gender, t.patient_gender, 'U')) IN ('F', 'M', 'U')
      THEN UPPER(COALESCE(mp.gender, t.patient_gender, 'U'))
      ELSE 'U'
    END AS patient_gender,
    NULL AS patient_age,
    COALESCE(
        mp.yearOfBirth, YEAR(t.patient_dob)
        ) AS patient_year_of_birth,
    SUBSTR(COALESCE(
            mp.threeDigitZip, t.patient_zip
            ), 0, 3) AS patient_zip3,
    COALESCE(
        SUBSTR(REGEXP_REPLACE(mp.state, '"', ''), 0, 2),
        SUBSTR(t.state, 0, 2)
        ) AS patient_state,
    EXTRACT_DATE(
        t.report_date,
        '%Y/%m/%d'
        ) AS date_service,
    NULL AS date_specimen,
    EXTRACT_DATE(
        t.report_date,
        '%Y/%m/%d'
        ) AS date_report,
    NULL AS time_report,
    NULL AS loinc_code,
    NULL AS lab_id,
    NULL AS test_id,
    NULL AS test_number,
    NULL AS test_battery_local_id,
    NULL AS test_battery_std_id,
    NULL AS test_battery_name,
    NULL AS test_ordered_local_id,
    NULL AS test_ordered_std_id,
    t.gene AS test_ordered_name,
    NULL AS result_id,
    ARRAY(
        t.chromosome, t.exon, t.mutation_aa, t.mutation_nt, t.maf_percentage,
        t.ref_seq_transcript_Id, t.genomic_position, t.splice_effect, t.cdna,
        t.indel_type, t.fusion_gene_n, t.fusion_position_a, t.fusion_position_b,
        t.fusion_direction_a, t.fusion_direction_b, t.fusion_downstram_gene,
        t.cnv_copy_number, t.cosmic_id, t.dbsnp_id
        )[re.n] AS result,
    ARRAY(
        'chromosome', 'exon', 'mutation_aa', 'mutation_nt', 'maf_percentage',
        'ref_seq_transcript_Id', 'genomic_position', 'splice_effect', 'cdna',
        'indel_type', 'fusion_gene_n', 'fusion_position_a', 'fusion_position_b',
        'fusion_direction_a', 'fusion_direction_b', 'fusion_downstram_gene',
        'cnv_copy_number', 'cosmic_id', 'dbsnp_id'
        )[re.n] AS result_name,
    t.variant_type AS result_unit_of_measure,
    NULL AS result_desc,
    NULL AS result_comments,
    NULL AS ref_range_low,
    NULL AS ref_range_high,
    NULL AS ref_range_alpha,
    NULL AS abnormal_flag,
    NULL AS fasting_status,
    t.cancer_type AS diagnosis_code,
    CASE
      WHEN t.cancer_type IS NOT NULL THEN 'VENDOR'
    END AS diagnosis_code_qual,
    NULL AS diagnosis_code_priority,
    NULL AS procedure_code,
    NULL AS procedure_code_qual,
    NULL AS lab_npi,
    NULL AS ordering_npi,
    NULL AS payer_id,
    NULL AS payer_id_qual,
    NULL AS payer_name,
    NULL AS payer_parent_name,
    NULL AS payer_org_name,
    NULL AS payer_plan_id,
    NULL AS payer_plan_name,
    NULL AS payer_type,
    NULL AS lab_other_id,
    NULL AS lab_other_qual,
    NULL AS ordering_other_id,
    NULL AS ordering_other_qual,
    CASE
      WHEN t.physician_first_name IS NOT NULL
      THEN CONCAT(t.physician_last_name, ', ', t.physician_first_name)
      ELSE t.physician_last_name
    END AS ordering_name,
    NULL AS ordering_market_type,
    NULL AS ordering_specialty,
    NULL AS ordering_vendor_id,
    NULL AS ordering_tax_id,
    NULL AS ordering_dea_id,
    NULL AS ordering_ssn,
    NULL AS ordering_state_license,
    NULL AS ordering_upin,
    NULL AS ordering_commercial_id,
    t.physician_street_address_1 AS ordering_address_1,
    t.physician_street_address_2 AS ordering_address_2,
    t.physician_city AS ordering_city,
    t.physician_state AS ordering_state,
    t.physician_zipcode AS ordering_zip,
    NULL AS logical_delete_reason
FROM transactions t
    LEFT JOIN matching_payload mp ON t.hvjoinkey = mp.hvjoinkey
    CROSS JOIN result_exploder re
WHERE UPPER(COALESCE(t.physician_country, 'UNITED STATES')) = 'UNITED STATES'
    AND UPPER(COALESCE(t.patient_country, 'UNITED STATES')) = 'UNITED STATES'
    AND (
        ARRAY(
            t.chromosome, t.exon, t.mutation_aa, t.mutation_nt, t.maf_percentage,
            t.ref_seq_transcript_Id, t.genomic_position, t.splice_effect, t.cdna,
            t.indel_type, t.fusion_gene_n, t.fusion_position_a, t.fusion_position_b,
            t.fusion_direction_a, t.fusion_direction_b, t.fusion_downstram_gene,
            t.cnv_copy_number, t.cosmic_id, t.dbsnp_id
            )[re.n] IS NOT NULL OR (
            COALESCE(
                t.chromosome, t.exon, t.mutation_aa, t.mutation_nt, t.maf_percentage,
                t.ref_seq_transcript_Id, t.genomic_position, t.splice_effect, t.cdna,
                t.indel_type, t.fusion_gene_n, t.fusion_position_a, t.fusion_position_b,
                t.fusion_direction_a, t.fusion_direction_b, t.fusion_downstram_gene,
                t.cnv_copy_number, t.cosmic_id, t.dbsnp_id
                ) IS NULL AND re.n = 0
            )
        )
