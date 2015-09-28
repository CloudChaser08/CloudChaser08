
-- REFERENCE DATA

CREATE TABLE cpt_codes (
    code character varying(6),
    short_description character varying(32),
    long_description character varying(65535)
);
ALTER TABLE cpt_codes OWNER TO hv;

REVOKE ALL ON TABLE cpt_codes FROM PUBLIC;
REVOKE ALL ON TABLE cpt_codes FROM hv;
GRANT ALL ON TABLE cpt_codes TO hv;

CREATE TABLE diagnosis_codes (
    code character varying(6),
    short_description character varying(32),
    long_description character varying(512)
);
ALTER TABLE diagnosis_codes OWNER TO hv;

REVOKE ALL ON TABLE diagnosis_codes FROM PUBLIC;
REVOKE ALL ON TABLE diagnosis_codes FROM hv;
GRANT ALL ON TABLE diagnosis_codes TO hv;


CREATE TABLE ccs_multi_diagnosis_codes (
    diagnosis_code character varying(6),
    level_1 character varying(32),
    level_1_label character varying(255),
    level_2 character varying(32),
    level_2_label character varying(255),
    level_3 character varying(32),
    level_3_label character varying(255),
    level_4 character varying(32),
    level_4_label character varying(255)
);
ALTER TABLE ccs_multi_diagnosis_codes OWNER TO hv;

REVOKE ALL ON TABLE ccs_multi_diagnosis_codes FROM PUBLIC;
REVOKE ALL ON TABLE ccs_multi_diagnosis_codes FROM hv;
GRANT ALL ON TABLE ccs_multi_diagnosis_codes TO hv;

CREATE TABLE ccs_multi_procedure_codes (
    procedure_code character varying(6),
    level_1 character varying(32),
    level_1_label character varying(255),
    level_2 character varying(32),
    level_2_label character varying(255),
    level_3 character varying(32),
    level_3_label character varying(255)
);
ALTER TABLE ccs_multi_procedure_codes OWNER TO hv;

REVOKE ALL ON TABLE ccs_multi_procedure_codes FROM PUBLIC;
REVOKE ALL ON TABLE ccs_multi_procedure_codes FROM hv;
GRANT ALL ON TABLE ccs_multi_procedure_codes TO hv;

CREATE TABLE ndc_code (
    ndc_code character varying(65535),
    package_description character varying(1024),
    product_type character varying(64),
    proprietary_name character varying(1024),
    proprietary_name_suffix character varying(512),
    nonproprietary_name character varying(1024),
    dosage_form_name character varying(64),
    route_name character varying(128),
    start_marketing_date character varying(10),
    end_marketing_date character varying(10),
    marketing_category_name character varying(64),
    labeler_name character varying(512),
    substance_name character varying(4096),
    active_numerator_strength character varying(1024),
    active_ingred_unit character varying(4096),
    pharm_classes character varying(4096)
);
ALTER TABLE ndc_code OWNER TO hv;

REVOKE ALL ON TABLE ndc_code FROM PUBLIC;
REVOKE ALL ON TABLE ndc_code FROM hv;
GRANT ALL ON TABLE ndc_code TO hv;
GRANT ALL ON TABLE ndc_code TO aeliazar;
GRANT ALL ON TABLE ndc_code TO jsnavely;

CREATE TABLE ndc_package (
    product_id character varying(64),
    product_ndc character varying(10),
    ndc_package_code character varying(12),
    package_description character varying(1024)
);
ALTER TABLE ndc_package OWNER TO hv;

REVOKE ALL ON TABLE ndc_package FROM PUBLIC;
REVOKE ALL ON TABLE ndc_package FROM hv;
GRANT ALL ON TABLE ndc_package TO hv;


CREATE TABLE ndc_product (
    product_id character varying(64),
    product_ndc character varying(10),
    product_type character varying(64),
    proprietary_name character varying(1024),
    proprietary_name_suffix character varying(512),
    nonproprietary_name character varying(1024),
    dosage_form_name character varying(64),
    route_name character varying(128),
    start_marketing_date character varying(10),
    end_marketing_date character varying(10),
    marketing_category_name character varying(64),
    application_number character varying(32),
    labeler_name character varying(512),
    substance_name character varying(4096),
    active_numerator_strength character varying(1024),
    active_ingred_unit character varying(4096),
    pharm_classes character varying(4096),
    dea_schedule character varying(8)
);
ALTER TABLE ndc_product OWNER TO hv;

REVOKE ALL ON TABLE ndc_product FROM PUBLIC;
REVOKE ALL ON TABLE ndc_product FROM hv;
GRANT ALL ON TABLE ndc_product TO hv;


CREATE TABLE nppes (
    npi integer,
    entity_type character(1),
    replacement_npi integer,
    ein character varying(9),
    org_name character varying(70),
    last_name character varying(35),
    first_name character varying(20),
    middle_name character varying(20),
    name_prefix character varying(5),
    name_suffix character varying(5),
    credential character varying(20),
    oth_org_name character varying(70),
    oth_org_name_type character varying(1),
    oth_last_name character varying(35),
    oth_first_name character varying(20),
    oth_middle_name character varying(20),
    oth_name_prefix character varying(5),
    oth_name_suffix character varying(5),
    oth_credential character varying(20),
    oth_last_name_type character(1),
    mail_address1 character varying(55),
    mail_address2 character varying(55),
    mail_city character varying(40),
    mail_state character varying(40),
    mail_postal character varying(20),
    mail_country character varying(2),
    mail_phone character varying(20),
    mail_fax character varying(20),
    practice_address1 character varying(55),
    practice_address2 character varying(55),
    practice_city character varying(40),
    practice_state character varying(40),
    practice_postal character varying(20),
    practice_country character varying(2),
    practice_phone character varying(20),
    practice_fax character varying(20),
    enumeration_date date,
    update_date date,
    npi_deactivation_readon character varying(2),
    npi_deactivation_date date,
    npi_reactivation_date date,
    gender character varying(1),
    authorized_official_last character varying(35),
    authorized_official_first character varying(20),
    authorized_official_middle character varying(20),
    authorized_official_title character varying(35),
    authorized_official_phone character varying(20),
    taxonomy_code_1 character varying(10),
    license_number_1 character varying(20),
    license_number_state_code_1 character varying(2),
    primary_taxonomy_switch_1 character(1),
    taxonomy_code_2 character varying(10),
    license_number_2 character varying(20),
    license_number_state_code_2 character varying(2),
    primary_taxonomy_switch_2 character(1),
    taxonomy_code_3 character varying(10),
    license_number_3 character varying(20),
    license_number_state_code_3 character varying(2),
    primary_taxonomy_switch_3 character(1),
    taxonomy_code_4 character varying(10),
    license_number_4 character varying(20),
    license_number_state_code_4 character varying(2),
    primary_taxonomy_switch_4 character(1),
    taxonomy_code_5 character varying(10),
    license_number_5 character varying(20),
    license_number_state_code_5 character varying(2),
    primary_taxonomy_switch_5 character(1),
    taxonomy_code_6 character varying(10),
    license_number_6 character varying(20),
    license_number_state_code_6 character varying(2),
    primary_taxonomy_switch_6 character(1),
    taxonomy_code_7 character varying(10),
    license_number_7 character varying(20),
    license_number_state_code_7 character varying(2),
    primary_taxonomy_switch_7 character(1),
    taxonomy_code_8 character varying(10),
    license_number_8 character varying(20),
    license_number_state_code_8 character varying(2),
    primary_taxonomy_switch_8 character(1),
    taxonomy_code_9 character varying(10),
    license_number_9 character varying(20),
    license_number_state_code_9 character varying(2),
    primary_taxonomy_switch_9 character(1),
    taxonomy_code_10 character varying(10),
    license_number_10 character varying(20),
    license_number_state_code_10 character varying(2),
    primary_taxonomy_switch_10 character(1),
    taxonomy_code_11 character varying(10),
    license_number_11 character varying(20),
    license_number_state_code_11 character varying(2),
    primary_taxonomy_switch_11 character(1),
    taxonomy_code_12 character varying(10),
    license_number_12 character varying(20),
    license_number_state_code_12 character varying(2),
    primary_taxonomy_switch_12 character(1),
    taxonomy_code_13 character varying(10),
    license_number_13 character varying(20),
    license_number_state_code_13 character varying(2),
    primary_taxonomy_switch_13 character(1),
    taxonomy_code_14 character varying(10),
    license_number_14 character varying(20),
    license_number_state_code_14 character varying(2),
    primary_taxonomy_switch_14 character(1),
    taxonomy_code_15 character varying(10),
    license_number_15 character varying(20),
    license_number_state_code_15 character varying(2),
    primary_taxonomy_switch_15 character(1),
    oth_id_1 character varying(20),
    oth_id_type_code_1 character varying(2),
    oth_id_state_1 character varying(2),
    oth_id_issuer_1 character varying(80),
    oth_id_2 character varying(20),
    oth_id_type_code_2 character varying(2),
    oth_id_state_2 character varying(2),
    oth_id_issuer_2 character varying(80),
    oth_id_3 character varying(20),
    oth_id_type_code_3 character varying(2),
    oth_id_state_3 character varying(2),
    oth_id_issuer_3 character varying(80),
    oth_id_4 character varying(20),
    oth_id_type_code_4 character varying(2),
    oth_id_state_4 character varying(2),
    oth_id_issuer_4 character varying(80),
    oth_id_5 character varying(20),
    oth_id_type_code_5 character varying(2),
    oth_id_state_5 character varying(2),
    oth_id_issuer_5 character varying(80),
    oth_id_6 character varying(20),
    oth_id_type_code_6 character varying(2),
    oth_id_state_6 character varying(2),
    oth_id_issuer_6 character varying(80),
    oth_id_7 character varying(20),
    oth_id_type_code_7 character varying(2),
    oth_id_state_7 character varying(2),
    oth_id_issuer_7 character varying(80),
    oth_id_8 character varying(20),
    oth_id_type_code_8 character varying(2),
    oth_id_state_8 character varying(2),
    oth_id_issuer_8 character varying(80),
    oth_id_9 character varying(20),
    oth_id_type_code_9 character varying(2),
    oth_id_state_9 character varying(2),
    oth_id_issuer_9 character varying(80),
    oth_id_10 character varying(20),
    oth_id_type_code_10 character varying(2),
    oth_id_state_10 character varying(2),
    oth_id_issuer_10 character varying(80),
    oth_id_11 character varying(20),
    oth_id_type_code_11 character varying(2),
    oth_id_state_11 character varying(2),
    oth_id_issuer_11 character varying(80),
    oth_id_12 character varying(20),
    oth_id_type_code_12 character varying(2),
    oth_id_state_12 character varying(2),
    oth_id_issuer_12 character varying(80),
    oth_id_13 character varying(20),
    oth_id_type_code_13 character varying(2),
    oth_id_state_13 character varying(2),
    oth_id_issuer_13 character varying(80),
    oth_id_14 character varying(20),
    oth_id_type_code_14 character varying(2),
    oth_id_state_14 character varying(2),
    oth_id_issuer_14 character varying(80),
    oth_id_15 character varying(20),
    oth_id_type_code_15 character varying(2),
    oth_id_state_15 character varying(2),
    oth_id_issuer_15 character varying(80),
    oth_id_16 character varying(20),
    oth_id_type_code_16 character varying(2),
    oth_id_state_16 character varying(2),
    oth_id_issuer_16 character varying(80),
    oth_id_17 character varying(20),
    oth_id_type_code_17 character varying(2),
    oth_id_state_17 character varying(2),
    oth_id_issuer_17 character varying(80),
    oth_id_18 character varying(20),
    oth_id_type_code_18 character varying(2),
    oth_id_state_18 character varying(2),
    oth_id_issuer_18 character varying(80),
    oth_id_19 character varying(20),
    oth_id_type_code_19 character varying(2),
    oth_id_state_19 character varying(2),
    oth_id_issuer_19 character varying(80),
    oth_id_20 character varying(20),
    oth_id_type_code_20 character varying(2),
    oth_id_state_20 character varying(2),
    oth_id_issuer_20 character varying(80),
    oth_id_21 character varying(20),
    oth_id_type_code_21 character varying(2),
    oth_id_state_21 character varying(2),
    oth_id_issuer_21 character varying(80),
    oth_id_22 character varying(20),
    oth_id_type_code_22 character varying(2),
    oth_id_state_22 character varying(2),
    oth_id_issuer_22 character varying(80),
    oth_id_23 character varying(20),
    oth_id_type_code_23 character varying(2),
    oth_id_state_23 character varying(2),
    oth_id_issuer_23 character varying(80),
    oth_id_24 character varying(20),
    oth_id_type_code_24 character varying(2),
    oth_id_state_24 character varying(2),
    oth_id_issuer_24 character varying(80),
    oth_id_25 character varying(20),
    oth_id_type_code_25 character varying(2),
    oth_id_state_25 character varying(2),
    oth_id_issuer_25 character varying(80),
    oth_id_26 character varying(20),
    oth_id_type_code_26 character varying(2),
    oth_id_state_26 character varying(2),
    oth_id_issuer_26 character varying(80),
    oth_id_27 character varying(20),
    oth_id_type_code_27 character varying(2),
    oth_id_state_27 character varying(2),
    oth_id_issuer_27 character varying(80),
    oth_id_28 character varying(20),
    oth_id_type_code_28 character varying(2),
    oth_id_state_28 character varying(2),
    oth_id_issuer_28 character varying(80),
    oth_id_29 character varying(20),
    oth_id_type_code_29 character varying(2),
    oth_id_state_29 character varying(2),
    oth_id_issuer_29 character varying(80),
    oth_id_30 character varying(20),
    oth_id_type_code_30 character varying(2),
    oth_id_state_30 character varying(2),
    oth_id_issuer_30 character varying(80),
    oth_id_31 character varying(20),
    oth_id_type_code_31 character varying(2),
    oth_id_state_31 character varying(2),
    oth_id_issuer_31 character varying(80),
    oth_id_32 character varying(20),
    oth_id_type_code_32 character varying(2),
    oth_id_state_32 character varying(2),
    oth_id_issuer_32 character varying(80),
    oth_id_33 character varying(20),
    oth_id_type_code_33 character varying(2),
    oth_id_state_33 character varying(2),
    oth_id_issuer_33 character varying(80),
    oth_id_34 character varying(20),
    oth_id_type_code_34 character varying(2),
    oth_id_state_34 character varying(2),
    oth_id_issuer_34 character varying(80),
    oth_id_35 character varying(20),
    oth_id_type_code_35 character varying(2),
    oth_id_state_35 character varying(2),
    oth_id_issuer_35 character varying(80),
    oth_id_36 character varying(20),
    oth_id_type_code_36 character varying(2),
    oth_id_state_36 character varying(2),
    oth_id_issuer_36 character varying(80),
    oth_id_37 character varying(20),
    oth_id_type_code_37 character varying(2),
    oth_id_state_37 character varying(2),
    oth_id_issuer_37 character varying(80),
    oth_id_38 character varying(20),
    oth_id_type_code_38 character varying(2),
    oth_id_state_38 character varying(2),
    oth_id_issuer_38 character varying(80),
    oth_id_39 character varying(20),
    oth_id_type_code_39 character varying(2),
    oth_id_state_39 character varying(2),
    oth_id_issuer_39 character varying(80),
    oth_id_40 character varying(20),
    oth_id_type_code_40 character varying(2),
    oth_id_state_40 character varying(2),
    oth_id_issuer_40 character varying(80),
    oth_id_41 character varying(20),
    oth_id_type_code_41 character varying(2),
    oth_id_state_41 character varying(2),
    oth_id_issuer_41 character varying(80),
    oth_id_42 character varying(20),
    oth_id_type_code_42 character varying(2),
    oth_id_state_42 character varying(2),
    oth_id_issuer_42 character varying(80),
    oth_id_43 character varying(20),
    oth_id_type_code_43 character varying(2),
    oth_id_state_43 character varying(2),
    oth_id_issuer_43 character varying(80),
    oth_id_44 character varying(20),
    oth_id_type_code_44 character varying(2),
    oth_id_state_44 character varying(2),
    oth_id_issuer_44 character varying(80),
    oth_id_45 character varying(20),
    oth_id_type_code_45 character varying(2),
    oth_id_state_45 character varying(2),
    oth_id_issuer_45 character varying(80),
    oth_id_46 character varying(20),
    oth_id_type_code_46 character varying(2),
    oth_id_state_46 character varying(2),
    oth_id_issuer_46 character varying(80),
    oth_id_47 character varying(20),
    oth_id_type_code_47 character varying(2),
    oth_id_state_47 character varying(2),
    oth_id_issuer_47 character varying(80),
    oth_id_48 character varying(20),
    oth_id_type_code_48 character varying(2),
    oth_id_state_48 character varying(2),
    oth_id_issuer_48 character varying(80),
    oth_id_49 character varying(20),
    oth_id_type_code_49 character varying(2),
    oth_id_state_49 character varying(2),
    oth_id_issuer_49 character varying(80),
    oth_id_50 character varying(20),
    oth_id_type_code_50 character varying(2),
    oth_id_state_50 character varying(2),
    oth_id_issuer_50 character varying(80),
    is_sole_proprietor character(1),
    is_org_subpart character(1),
    parent_org_lbn character varying(70),
    parent_org_tin character varying(9),
    authorized_official_prefix character varying(5),
    authorized_official_suffix character varying(5),
    authorized_official_credential character varying(20),
    taxonomy_group_1 character varying(70),
    taxonomy_group_2 character varying(70),
    taxonomy_group_3 character varying(70),
    taxonomy_group_4 character varying(70),
    taxonomy_group_5 character varying(70),
    taxonomy_group_6 character varying(70),
    taxonomy_group_7 character varying(70),
    taxonomy_group_8 character varying(70),
    taxonomy_group_9 character varying(70),
    taxonomy_group_10 character varying(70),
    taxonomy_group_11 character varying(70),
    taxonomy_group_12 character varying(70),
    taxonomy_group_13 character varying(70),
    taxonomy_group_14 character varying(70),
    taxonomy_group_15 character varying(70)
);
ALTER TABLE nppes OWNER TO hv;

REVOKE ALL ON TABLE nppes FROM PUBLIC;
REVOKE ALL ON TABLE nppes FROM hv;
GRANT ALL ON TABLE nppes TO hv;

CREATE TABLE nucc_taxonomy (
    code character varying(10),
    taxonomy_type character varying(256),
    classification character varying(256),
    specialization character varying(256),
    definition character varying(256),
    notes character varying(256)
);
ALTER TABLE nucc_taxonomy OWNER TO hv;

REVOKE ALL ON TABLE nucc_taxonomy FROM PUBLIC;
REVOKE ALL ON TABLE nucc_taxonomy FROM hv;
GRANT ALL ON TABLE nucc_taxonomy TO hv;
GRANT ALL ON TABLE nucc_taxonomy TO jsnavely;

CREATE TABLE place_of_service (
    id smallint,
    pos_group character varying(64),
    pos_type character varying(64),
    description character varying(1024),
    start_date date,
    end_date date
);
ALTER TABLE place_of_service OWNER TO hv;

CREATE TABLE proc_code (
    proc_code character varying(16),
    long_description character varying(65535),
    short_description character varying(32),
    xref1 character varying(16),
    xref2 character varying(16),
    xref3 character varying(16),
    xref4 character varying(16),
    xref5 character varying(16),
    cov_code character varying(16),
    add_date character varying(8),
    act_eff_dt character varying(8),
    term_dt character varying(16),
    action_code character varying(2),
    action_description character varying(47)
);
ALTER TABLE proc_code OWNER TO hv;

REVOKE ALL ON TABLE proc_code FROM PUBLIC;
REVOKE ALL ON TABLE proc_code FROM hv;
GRANT ALL ON TABLE proc_code TO hv;

CREATE TABLE procedure_codes (
    code character varying(6),
    short_description character varying(32),
    long_description character varying(512)
);
ALTER TABLE procedure_codes OWNER TO hv;

REVOKE ALL ON TABLE procedure_codes FROM PUBLIC;
REVOKE ALL ON TABLE procedure_codes FROM hv;
GRANT ALL ON TABLE procedure_codes TO hv;

CREATE TABLE hcpcs (
    hcpc character varying(16),
    long_description character varying(2056),
    short_description character varying(32),
    price_cd1 character varying(16),
    price_cd2 character varying(16),
    price_cd3 character varying(16),
    price_cd4 character varying(16),
    multi_pi character varying(16),
    cim1 character varying(16),
    cim2 character varying(16),
    cim3 character varying(16),
    mcm1 character varying(16),
    mcm2 character varying(16),
    mcm3 character varying(16),
    statute character varying(16),
    lab_cert_cd1 character varying(16),
    lab_cert_cd2 character varying(16),
    lab_cert_cd3 character varying(16),
    lab_cert_cd4 character varying(16),
    lab_cert_cd5 character varying(16),
    lab_cert_cd6 character varying(16),
    lab_cert_cd7 character varying(16),
    lab_cert_cd8 character varying(16),
    xref1 character varying(16),
    xref2 character varying(16),
    xref3 character varying(16),
    xref4 character varying(16),
    xref5 character varying(16),
    cov_code character varying(16),
    asc_gpcd character varying(16),
    asc_eff_dt character varying(16),
    proc_note character varying(16),
    betos character varying(16),
    tos1 character varying(16),
    tos2 character varying(16),
    tos3 character varying(16),
    tos4 character varying(16),
    tos5 character varying(16),
    anes_unit character varying(16),
    add_date character varying(8),
    act_eff_dt character varying(8),
    term_dt character varying(16),
    action_code character varying(2)
);
ALTER TABLE hcpcs OWNER TO hv;

REVOKE ALL ON TABLE hcpcs FROM PUBLIC;
REVOKE ALL ON TABLE hcpcs FROM hv;
GRANT ALL ON TABLE hcpcs TO hv;

CREATE TABLE icd10_diagnosis_codes (
    ordernum character varying(5),
    code character varying(7),
    header character(1),
    short_description character varying(60),
    long_description character varying(512)
);
ALTER TABLE icd10_diagnosis_codes OWNER TO hv;

REVOKE ALL ON TABLE icd10_diagnosis_codes FROM PUBLIC;
REVOKE ALL ON TABLE icd10_diagnosis_codes FROM hv;
GRANT ALL ON TABLE icd10_diagnosis_codes TO hv;

CREATE TABLE icd10_procedure_codes (
    ordernum character varying(5),
    code character varying(7),
    header character(1),
    short_description character varying(60),
    long_description character varying(512)
);
ALTER TABLE icd10_procedure_codes OWNER TO hv;

REVOKE ALL ON TABLE icd10_procedure_codes FROM PUBLIC;
REVOKE ALL ON TABLE icd10_procedure_codes FROM hv;
GRANT ALL ON TABLE icd10_procedure_codes TO hv;

