-- Create table to hold the basic structure of wide claim rows
DROP TABLE IF EXISTS allscripts_claims_base;
CREATE TABLE allscripts_claims_base(
Create_Date text ENCODE lzo,
Version_Code text ENCODE lzo,
Entity_ID text ENCODE lzo,
Billing_or_Pay_To_Provider_Taxonomy_Code text ENCODE lzo,
Billing_Prov_Organization_Name_or_Billing_Prov_Last_Name text ENCODE lzo,
Billing_Prov_Last_Name text ENCODE lzo,
Billing_Prov_First_Name text ENCODE lzo,
Billing_Prov_MI text ENCODE lzo,
Billing_Prov_ID_Qual text ENCODE lzo,
Billing_Prov_Tax_ID text ENCODE lzo,
Billing_Prov_NPI text ENCODE lzo,
Billing_Provider_s_Address_1 text ENCODE lzo,
Billing_Provider_s_Address_2 text ENCODE lzo,
Billing_Provider_s_City text ENCODE lzo,
Billing_Provider_s_State text ENCODE lzo,
Billing_Provider_s_Zip text ENCODE lzo,
Insurance_Type_Code text ENCODE lzo,
Source_of_Payment text ENCODE lzo,
Patient_DOB text ENCODE lzo,
Patient_Sex text ENCODE lzo,
Primary_Payer_Name text ENCODE lzo,
Total_Claim_Charge_Amount text ENCODE lzo,
Diagnosis_Code_1 text ENCODE lzo,
Diagnosis_Code_2 text ENCODE lzo,
Diagnosis_Code_3 text ENCODE lzo,
Diagnosis_Code_4 text ENCODE lzo,
Diagnosis_Code_5 text ENCODE lzo,
Diagnosis_Code_6 text ENCODE lzo,
Diagnosis_Code_7 text ENCODE lzo,
Diagnosis_Code_8 text ENCODE lzo,
Referring_Provider_Last_Name text ENCODE lzo,
Referring_Provider_First_Name text ENCODE lzo,
Referring_Provider_Middle_Initial text ENCODE lzo,
Referring_Provider_Primary_ID_Qualifier text ENCODE lzo,
Referring_Provider_Primary_ID text ENCODE lzo,
Referring_Prov_NPI text ENCODE lzo,
Referring_Provider_Taxonomy_Code text ENCODE lzo,
Rendering_Provider_Last text ENCODE lzo,
Rendering_Provider_First text ENCODE lzo,
Rendering_Provider_Middle text ENCODE lzo,
Rendering_Provider_Primary_ID_Qualifier text ENCODE lzo,
Rendering_Provider_Primary_ID text ENCODE lzo,
Rendering_Provider_NPI text ENCODE lzo,
Rendering_Provider_Specialty_Code text ENCODE lzo,
Facility_Laboratory_Name text ENCODE lzo,
Facility_Lab_Primary_ID_Qualifier text ENCODE lzo,
Facility_Laboratory_Primary_Identifier text ENCODE lzo,
Facility_Lab_NPI text ENCODE lzo,
Facility_Laboratory_Street_Address_1 text ENCODE lzo,
Facility_Laboratory_Street_Address_2 text ENCODE lzo,
Facility_Laboratory_City text ENCODE lzo,
Facility_Laboratory_State text ENCODE lzo,
Facility_Laboratory_Zip_Code text ENCODE lzo,
Secondary_Payer_Sequence_Number text ENCODE lzo,
Secondary_Payer_Insurance_Type_Code text ENCODE lzo,
Secondary_Payer_Source_of_Payment text ENCODE lzo,
Teritary_Payer_Sequence_Number text ENCODE lzo,
Teritary_Payer_Insurance_Type_Code text ENCODE lzo,
Teritary_Payer_Source_of_Payment text ENCODE lzo,
Second_Payer_Primary_ID text ENCODE lzo,
Third_Payer_Primary_ID text ENCODE lzo,
Charge_Line_Number text ENCODE lzo,
Std_Chg_Line_HCPCS_Procedure_Code text ENCODE lzo,
HCPCS_Modifier_1 text ENCODE lzo,
HCPCS_Modifier_2 text ENCODE lzo,
HCPCS_Modifier_3 text ENCODE lzo,
HCPCS_Modifier_4 text ENCODE lzo,
Line_Charges text ENCODE lzo,
Units_of_Service text ENCODE lzo,
Place_of_Service text ENCODE lzo,
Diagnosis_Code_Pointer_1 text ENCODE lzo,
Diagnosis_Code_Pointer_2 text ENCODE lzo,
Diagnosis_Code_Pointer_3 text ENCODE lzo,
Diagnosis_Code_Pointer_4 text ENCODE lzo,
Service_From_Date text ENCODE lzo,
Service_To_Date text ENCODE lzo,
NDC_CODE text ENCODE lzo,
serviceline_Rendering_Provider_Last text ENCODE lzo,
serviceline_Rendering_Provider_First text ENCODE lzo,
serviceline_Rendering_Provider_Middle text ENCODE lzo,
serviceline_Rendering_Provider_Tax_ID_Qual text ENCODE lzo,
serviceline_Rendering_Provider_Primary_ID text ENCODE lzo,
serviceline_Rendering_Provider_NPI text ENCODE lzo,
serviceline_Rendering_Provider_Specialty_Code text ENCODE lzo,
serviceline_Service_Facility_Name text ENCODE lzo,
serviceline_Service_Facility_NPI text ENCODE lzo,
serviceline_Service_Facility_Address_1 text ENCODE lzo,
serviceline_Service_Facility_Address_2 text ENCODE lzo,
serviceline_Service_Facility_City text ENCODE lzo,
serviceline_Service_Facility_State text ENCODE lzo,
serviceline_Service_Facility_Zip_Code text ENCODE lzo,
serviceline_Referring_Provider_Last_Name text ENCODE lzo,
serviceline_Referring_Provider_First_Name text ENCODE lzo,
serviceline_Referring_Provider_MI text ENCODE lzo,
serviceline_Referring_Provider_NPI text ENCODE lzo,
diag_concat text ENCODE lzo,
svcptr_concat text ENCODE lzo,
related text ENCODE lzo
)
DISTKEY(entity_id) SORTKEY(entity_id);

DROP TABLE IF EXISTS related_diags_tmp;
CREATE TABLE related_diags_tmp (entity_id text ENCODE lzo, related text ENCODE lzo) DISTKEY(entity_id) SORTKEY(entity_id);

DROP TABLE IF EXISTS related_diags;
CREATE TABLE related_diags (entity_id text ENCODE lzo, related varchar(5000) ENCODE lzo) DISTKEY(entity_id) SORTKEY(entity_id);

DROP TABLE IF EXISTS allscripts_claims_unrelated;
CREATE TABLE allscripts_claims_unrelated (
Create_Date text ENCODE lzo,
Version_Code text ENCODE lzo,
Entity_ID text ENCODE lzo,
Billing_or_Pay_To_Provider_Taxonomy_Code text ENCODE lzo,
Billing_Prov_Organization_Name_or_Billing_Prov_Last_Name text ENCODE lzo,
Billing_Prov_Last_Name text ENCODE lzo,
Billing_Prov_First_Name text ENCODE lzo,
Billing_Prov_MI text ENCODE lzo,
Billing_Prov_ID_Qual text ENCODE lzo,
Billing_Prov_Tax_ID text ENCODE lzo,
Billing_Prov_NPI text ENCODE lzo,
Billing_Provider_s_Address_1 text ENCODE lzo,
Billing_Provider_s_Address_2 text ENCODE lzo,
Billing_Provider_s_City text ENCODE lzo,
Billing_Provider_s_State text ENCODE lzo,
Billing_Provider_s_Zip text ENCODE lzo,
Insurance_Type_Code text ENCODE lzo,
Source_of_Payment text ENCODE lzo,
Patient_DOB text ENCODE lzo,
Patient_Sex text ENCODE lzo,
Primary_Payer_Name text ENCODE lzo,
Total_Claim_Charge_Amount text ENCODE lzo,
Diagnosis_Code_1 text ENCODE lzo,
Diagnosis_Code_2 text ENCODE lzo,
Diagnosis_Code_3 text ENCODE lzo,
Diagnosis_Code_4 text ENCODE lzo,
Diagnosis_Code_5 text ENCODE lzo,
Diagnosis_Code_6 text ENCODE lzo,
Diagnosis_Code_7 text ENCODE lzo,
Diagnosis_Code_8 text ENCODE lzo,
Referring_Provider_Last_Name text ENCODE lzo,
Referring_Provider_First_Name text ENCODE lzo,
Referring_Provider_Middle_Initial text ENCODE lzo,
Referring_Provider_Primary_ID_Qualifier text ENCODE lzo,
Referring_Provider_Primary_ID text ENCODE lzo,
Referring_Prov_NPI text ENCODE lzo,
Referring_Provider_Taxonomy_Code text ENCODE lzo,
Rendering_Provider_Last text ENCODE lzo,
Rendering_Provider_First text ENCODE lzo,
Rendering_Provider_Middle text ENCODE lzo,
Rendering_Provider_Primary_ID_Qualifier text ENCODE lzo,
Rendering_Provider_Primary_ID text ENCODE lzo,
Rendering_Provider_NPI text ENCODE lzo,
Rendering_Provider_Specialty_Code text ENCODE lzo,
Facility_Laboratory_Name text ENCODE lzo,
Facility_Lab_Primary_ID_Qualifier text ENCODE lzo,
Facility_Laboratory_Primary_Identifier text ENCODE lzo,
Facility_Lab_NPI text ENCODE lzo,
Facility_Laboratory_Street_Address_1 text ENCODE lzo,
Facility_Laboratory_Street_Address_2 text ENCODE lzo,
Facility_Laboratory_City text ENCODE lzo,
Facility_Laboratory_State text ENCODE lzo,
Facility_Laboratory_Zip_Code text ENCODE lzo,
Secondary_Payer_Sequence_Number text ENCODE lzo,
Secondary_Payer_Insurance_Type_Code text ENCODE lzo,
Secondary_Payer_Source_of_Payment text ENCODE lzo,
Teritary_Payer_Sequence_Number text ENCODE lzo,
Teritary_Payer_Insurance_Type_Code text ENCODE lzo,
Teritary_Payer_Source_of_Payment text ENCODE lzo,
Second_Payer_Primary_ID text ENCODE lzo,
Third_Payer_Primary_ID text ENCODE lzo,
Charge_Line_Number text ENCODE lzo,
Std_Chg_Line_HCPCS_Procedure_Code text ENCODE lzo,
HCPCS_Modifier_1 text ENCODE lzo,
HCPCS_Modifier_2 text ENCODE lzo,
HCPCS_Modifier_3 text ENCODE lzo,
HCPCS_Modifier_4 text ENCODE lzo,
Line_Charges text ENCODE lzo,
Units_of_Service text ENCODE lzo,
Place_of_Service text ENCODE lzo,
Diagnosis_Code_Pointer_1 text ENCODE lzo,
Diagnosis_Code_Pointer_2 text ENCODE lzo,
Diagnosis_Code_Pointer_3 text ENCODE lzo,
Diagnosis_Code_Pointer_4 text ENCODE lzo,
Service_From_Date text ENCODE lzo,
Service_To_Date text ENCODE lzo,
NDC_CODE text ENCODE lzo,
serviceline_Rendering_Provider_Last text ENCODE lzo,
serviceline_Rendering_Provider_First text ENCODE lzo,
serviceline_Rendering_Provider_Middle text ENCODE lzo,
serviceline_Rendering_Provider_Tax_ID_Qual text ENCODE lzo,
serviceline_Rendering_Provider_Primary_ID text ENCODE lzo,
serviceline_Rendering_Provider_NPI text ENCODE lzo,
serviceline_Rendering_Provider_Specialty_Code text ENCODE lzo,
serviceline_Service_Facility_Name text ENCODE lzo,
serviceline_Service_Facility_NPI text ENCODE lzo,
serviceline_Service_Facility_Address_1 text ENCODE lzo,
serviceline_Service_Facility_Address_2 text ENCODE lzo,
serviceline_Service_Facility_City text ENCODE lzo,
serviceline_Service_Facility_State text ENCODE lzo,
serviceline_Service_Facility_Zip_Code text ENCODE lzo,
serviceline_Referring_Provider_Last_Name text ENCODE lzo,
serviceline_Referring_Provider_First_Name text ENCODE lzo,
serviceline_Referring_Provider_MI text ENCODE lzo,
serviceline_Referring_Provider_NPI text ENCODE lzo,
unrelated text ENCODE lzo
)
DISTKEY(entity_id) SORTKEY(entity_id);

INSERT INTO allscripts_claims_base SELECT
Create_Date,
Version_Code,
claims.entity_id AS entity_id,
Billing_or_Pay_To_Provider_Taxonomy_Code,
Billing_Prov_Organization_Name_or_Billing_Prov_Last_Name,
Billing_Prov_Last_Name,
Billing_Prov_First_Name,
Billing_Prov_MI,
Billing_Prov_ID_Qual,
Billing_Prov_Tax_ID,
Billing_Prov_NPI,
Billing_Provider_s_Address_1,
Billing_Provider_s_Address_2,
Billing_Provider_s_City,
Billing_Provider_s_State,
Billing_Provider_s_Zip,
Insurance_Type_Code,
Source_of_Payment,
Patient_DOB,
Patient_Sex,
Primary_Payer_Name,
Total_Claim_Charge_Amount,
Diagnosis_Code_1,
Diagnosis_Code_2,
Diagnosis_Code_3,
Diagnosis_Code_4,
Diagnosis_Code_5,
Diagnosis_Code_6,
Diagnosis_Code_7,
Diagnosis_Code_8,
claims.Referring_Provider_Last_Name,
claims.Referring_Provider_First_Name,
claims.Referring_Provider_Middle_Initial,
claims.Referring_Provider_Primary_ID_Qualifier,
claims.Referring_Provider_Primary_ID,
claims.Referring_Prov_NPI,
claims.Referring_Provider_Taxonomy_Code,
claims.Rendering_Provider_Last,
claims.Rendering_Provider_First,
claims.Rendering_Provider_Middle,
claims.Rendering_Provider_Primary_ID_Qualifier,
claims.Rendering_Provider_Primary_ID,
claims.Rendering_Provider_NPI,
claims.Rendering_Provider_Specialty_Code,
claims.Facility_Laboratory_Name,
claims.Facility_Lab_Primary_ID_Qualifier,
claims.Facility_Laboratory_Primary_Identifier,
claims.Facility_Lab_NPI,
claims.Facility_Laboratory_Street_Address_1,
claims.Facility_Laboratory_Street_Address_2,
claims.Facility_Laboratory_City,
claims.Facility_Laboratory_State,
claims.Facility_Laboratory_Zip_Code,
Secondary_Payer_Sequence_Number,
Secondary_Payer_Insurance_Type_Code,
Secondary_Payer_Source_of_Payment,
Teritary_Payer_Sequence_Number,
Teritary_Payer_Insurance_Type_Code,
Teritary_Payer_Source_of_Payment,
Second_Payer_Primary_ID,
Third_Payer_Primary_ID,
Charge_Line_Number,
Std_Chg_Line_HCPCS_Procedure_Code,
HCPCS_Modifier_1,
HCPCS_Modifier_2,
HCPCS_Modifier_3,
HCPCS_Modifier_4,
Line_Charges,
Units_of_Service,
Place_of_Service,
Diagnosis_Code_Pointer_1,
Diagnosis_Code_Pointer_2,
Diagnosis_Code_Pointer_3,
Diagnosis_Code_Pointer_4,
Service_From_Date,
Service_To_Date,
NDC_CODE,
servicelines.Rendering_Provider_Last,
servicelines.Rendering_Provider_First,
servicelines.Rendering_Provider_Middle,
servicelines.Rendering_Provider_Tax_ID_Qual,
servicelines.Rendering_Provider_Primary_ID,
servicelines.Rendering_Provider_NPI,
servicelines.Rendering_Provider_Specialty_Code,
servicelines.Service_Facility_Name,
servicelines.Service_Facility_NPI,
servicelines.Service_Facility_Address_1,
servicelines.Service_Facility_Address_2,
servicelines.Service_Facility_City,
servicelines.Service_Facility_State,
servicelines.Service_Facility_Zip_Code,
servicelines.Referring_Provider_Last_Name,
servicelines.Referring_Provider_First_Name,
servicelines.Referring_Provider_MI,
servicelines.Referring_Provider_NPI,
(COALESCE(diagnosis_code_1,
'')  || ':' || COALESCE(diagnosis_code_2,
'')  || ':' || COALESCE(diagnosis_code_3,
'')  || ':' || COALESCE(diagnosis_code_4,
'')  || ':' || COALESCE(diagnosis_code_5,
'')  || ':' || COALESCE(diagnosis_code_6,
'')  || ':' || COALESCE(diagnosis_code_7,
'')  || ':' || COALESCE(diagnosis_code_8,
'')),
(COALESCE(diagnosis_code_pointer_1,
'')  || ':' || COALESCE(diagnosis_code_pointer_2,
'')  || ':' || COALESCE(diagnosis_code_pointer_3,
'')  || ':' || COALESCE(diagnosis_code_pointer_4,
'')),
get_diagnosis_with_priority((COALESCE(diagnosis_code_1,
'')  || ':' || COALESCE(diagnosis_code_2,
'')  || ':' || COALESCE(diagnosis_code_3,
'')  || ':' || COALESCE(diagnosis_code_4,
'')  || ':' || COALESCE(diagnosis_code_5,
'')  || ':' || COALESCE(diagnosis_code_6,
'')  || ':' || COALESCE(diagnosis_code_7,
'')  || ':' || COALESCE(diagnosis_code_8,
'')),
(COALESCE(diagnosis_code_pointer_1,
'')  || ':' || COALESCE(diagnosis_code_pointer_2,
'')  || ':' || COALESCE(diagnosis_code_pointer_3,
'')  || ':' || COALESCE(diagnosis_code_pointer_4,
''))) as related
FROM allscripts_dx_raw_claims claims
    INNER JOIN allscripts_dx_raw_service servicelines USING (entity_id);

INSERT INTO medicalclaims_common_model (
claim_id,
hvid,
source_version,
patient_gender,
patient_year_of_birth,
patient_zip3,
patient_state,
claim_type,
date_received,
date_service,
date_service_end,
place_of_service_std_id,
service_line_number,
diagnosis_code,
diagnosis_priority,
procedure_code,
procedure_units,
procedure_modifier_1,
procedure_modifier_2,
procedure_modifier_3,
procedure_modifier_4,
ndc_code,
medical_coverage_type,
line_charge,
total_charge,
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_name,
payer_type,
prov_rendering_tax_id,
prov_rendering_ssn,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_ssn,
prov_referring_name_2,
prov_referring_std_taxonomy,
prov_facility_tax_id,
prov_facility_ssn,
prov_facility_name_1,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2
)
SELECT
b.entity_id,
hvid,
version_code,
patient_sex,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN service_from_date IS NOT NULL AND (patient_dob >= (extract('year' from service_from_date::date - '32873 days'::interval)::text)) AND patient_dob <= (extract('year' from getdate())::text) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threedigitzip as patient_zip3,
state as patient_state,
'P',
create_date,
service_from_date,
service_to_date,
place_of_service,
charge_line_number,
upper(replace(replace(replace(split_part(split_part(related,':',n),'_',1), ' ', ''), ',', ''), '.', '')) as diagnosis_code,
split_part(split_part(related,':',n),'_',2) as diagnosis_priority,
upper(replace(replace(std_chg_line_hcpcs_procedure_code, ' ', ''), ',', '')) as procedure_code,
units_of_service,
hcpcs_modifier_1,
hcpcs_modifier_2,
hcpcs_modifier_3,
hcpcs_modifier_4,
ndc_code,
source_of_payment,
line_charges,
total_claim_charge_amount,
CASE WHEN serviceline_rendering_provider_npi is not null THEN serviceline_rendering_provider_npi ELSE rendering_provider_npi END,
billing_prov_npi,
CASE WHEN serviceline_referring_provider_npi is not null THEN serviceline_referring_provider_npi ELSE referring_prov_npi END,
CASE WHEN serviceline_service_facility_npi is not null THEN serviceline_service_facility_npi ELSE facility_lab_npi END,
primary_payer_name,
insurance_type_code,
CASE WHEN
        serviceline_rendering_provider_primary_id is not null
        and (serviceline_rendering_provider_tax_id_qual = '24' 
        or serviceline_rendering_provider_tax_id_qual = '34')
    THEN serviceline_rendering_provider_primary_id
    WHEN
        rendering_provider_primary_id_qualifier = '24'
        or rendering_provider_primary_id_qualifier = '34'
    THEN rendering_provider_primary_id
    ELSE NULL
    END,
CASE WHEN
        serviceline_rendering_provider_primary_id is not null
        and serviceline_rendering_provider_tax_id_qual = '24' 
    THEN serviceline_rendering_provider_primary_id
    WHEN
        rendering_provider_primary_id_qualifier = '24'
    THEN rendering_provider_primary_id
    ELSE NULL
    END,
CASE WHEN serviceline_rendering_provider_last is not null
    THEN serviceline_rendering_provider_last ||
        COALESCE(', ' || serviceline_rendering_provider_first, '') ||
        COALESCE(', ' || serviceline_rendering_provider_middle, '')
    ELSE rendering_provider_last ||
        COALESCE(', ' || rendering_provider_first, '') ||
        COALESCE(', ' || rendering_provider_middle, '')
    END,
CASE WHEN serviceline_rendering_provider_specialty_code is not null
    THEN serviceline_rendering_provider_specialty_code
    ELSE rendering_provider_specialty_code
    END,
CASE WHEN
        billing_prov_id_qual = '24'
        or billing_prov_id_qual = '34'
    THEN billing_prov_tax_id
    ELSE NULL
    END,
CASE WHEN billing_prov_id_qual = '24' THEN billing_prov_tax_id ELSE NULL END,
billing_prov_organization_name_or_billing_prov_last_name,
billing_prov_last_name || COALESCE(', ' || billing_prov_first_name, '') ||
    COALESCE(', ' || billing_prov_mi, ''),
billing_provider_s_address_1,
billing_provider_s_address_2,
billing_provider_s_city,
billing_provider_s_state,
billing_provider_s_zip,
billing_or_pay_to_provider_taxonomy_code,
CASE WHEN
        referring_provider_primary_id_qualifier = '24'
        or referring_provider_primary_id_qualifier = '34'
    THEN referring_provider_primary_id
    ELSE NULL
    END,
CASE WHEN referring_provider_primary_id_qualifier = '24' THEN referring_provider_primary_id ELSE NULL END,
CASE WHEN serviceline_referring_provider_last_name is not null
    THEN serviceline_referring_provider_last_name ||
        COALESCE(', ' || serviceline_referring_provider_first_name, '') ||
        COALESCE(', ' || serviceline_referring_provider_mi, '')
    ELSE referring_provider_last_name ||
        COALESCE(', ' || referring_provider_first_name, '') ||
        COALESCE(', ' || referring_provider_middle_initial, '')
    END,
referring_provider_taxonomy_code,
CASE WHEN
        facility_lab_primary_id_qualifier = '24'
        or facility_lab_primary_id_qualifier = '34'
    THEN facility_laboratory_primary_identifier
    ELSE NULL
    END,
CASE WHEN facility_lab_primary_id_qualifier = '24' THEN facility_laboratory_primary_identifier ELSE NULL END,
CASE WHEN serviceline_service_facility_name is not null THEN serviceline_service_facility_name ELSE facility_laboratory_name END,
CASE WHEN serviceline_service_facility_address_1 is not null THEN serviceline_service_facility_address_1 ELSE facility_laboratory_street_address_1 END,
CASE WHEN serviceline_service_facility_address_2 is not null THEN serviceline_service_facility_address_2 ELSE facility_laboratory_street_address_2 END,
CASE WHEN serviceline_service_facility_city is not null THEN serviceline_service_facility_city ELSE facility_laboratory_city END,
CASE WHEN serviceline_service_facility_state is not null THEN serviceline_service_facility_state ELSE facility_laboratory_state END,
CASE WHEN serviceline_service_facility_zip_code is not null THEN serviceline_service_facility_zip_code ELSE facility_laboratory_zip_code END,
second_payer_primary_id,
secondary_payer_sequence_number,
secondary_payer_source_of_payment,
secondary_payer_insurance_type_code,
third_payer_primary_id,
teritary_payer_sequence_number,
teritary_payer_source_of_payment,
teritary_payer_insurance_type_code
FROM allscripts_claims_base b
    CROSS JOIN split_indices
    LEFT JOIN matching_payload ON b.entity_id = entityid
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
WHERE split_part(related,':',n) IS NOT NULL AND split_part(related,':',n) <> '';

-- There can be a lot of raws with the same diagnosis. Only extract unique ones
INSERT INTO related_diags_tmp
SELECT DISTINCT entity_id, related
FROM allscripts_claims_base;

INSERT INTO related_diags
SELECT entity_id, listagg(related,':') within group (order by related) AS related
FROM related_diags_tmp GROUP BY entity_id;

INSERT INTO allscripts_claims_unrelated
SELECT
Create_Date,
Version_Code,
b.Entity_ID,
Billing_or_Pay_To_Provider_Taxonomy_Code,
Billing_Prov_Organization_Name_or_Billing_Prov_Last_Name,
Billing_Prov_Last_Name,
Billing_Prov_First_Name,
Billing_Prov_MI,
Billing_Prov_ID_Qual,
Billing_Prov_Tax_ID,
Billing_Prov_NPI,
Billing_Provider_s_Address_1,
Billing_Provider_s_Address_2,
Billing_Provider_s_City,
Billing_Provider_s_State,
Billing_Provider_s_Zip,
Insurance_Type_Code,
Source_of_Payment,
Patient_DOB,
Patient_Sex,
Primary_Payer_Name,
Total_Claim_Charge_Amount,
Diagnosis_Code_1,
Diagnosis_Code_2,
Diagnosis_Code_3,
Diagnosis_Code_4,
Diagnosis_Code_5,
Diagnosis_Code_6,
Diagnosis_Code_7,
Diagnosis_Code_8,
Referring_Provider_Last_Name,
Referring_Provider_First_Name,
Referring_Provider_Middle_Initial,
Referring_Provider_Primary_ID_Qualifier,
Referring_Provider_Primary_ID,
Referring_Prov_NPI,
Referring_Provider_Taxonomy_Code,
Rendering_Provider_Last,
Rendering_Provider_First,
Rendering_Provider_Middle,
Rendering_Provider_Primary_ID_Qualifier,
Rendering_Provider_Primary_ID,
Rendering_Provider_NPI,
Rendering_Provider_Specialty_Code,
Facility_Laboratory_Name,
Facility_Lab_Primary_ID_Qualifier,
Facility_Laboratory_Primary_Identifier,
Facility_Lab_NPI,
Facility_Laboratory_Street_Address_1,
Facility_Laboratory_Street_Address_2,
Facility_Laboratory_City,
Facility_Laboratory_State,
Facility_Laboratory_Zip_Code,
Secondary_Payer_Sequence_Number,
Secondary_Payer_Insurance_Type_Code,
Secondary_Payer_Source_of_Payment,
Teritary_Payer_Sequence_Number,
Teritary_Payer_Insurance_Type_Code,
Teritary_Payer_Source_of_Payment,
Second_Payer_Primary_ID,
Third_Payer_Primary_ID,
Charge_Line_Number,
Std_Chg_Line_HCPCS_Procedure_Code,
HCPCS_Modifier_1,
HCPCS_Modifier_2,
HCPCS_Modifier_3,
HCPCS_Modifier_4,
Line_Charges,
Units_of_Service,
Place_of_Service,
Diagnosis_Code_Pointer_1,
Diagnosis_Code_Pointer_2,
Diagnosis_Code_Pointer_3,
Diagnosis_Code_Pointer_4,
Service_From_Date,
Service_To_Date,
NDC_CODE,
serviceline_Rendering_Provider_Last,
serviceline_Rendering_Provider_First,
serviceline_Rendering_Provider_Middle,
serviceline_Rendering_Provider_Tax_ID_Qual,
serviceline_Rendering_Provider_Primary_ID,
serviceline_Rendering_Provider_NPI,
serviceline_Rendering_Provider_Specialty_Code,
serviceline_Service_Facility_Name,
serviceline_Service_Facility_NPI,
serviceline_Service_Facility_Address_1,
serviceline_Service_Facility_Address_2,
serviceline_Service_Facility_City,
serviceline_Service_Facility_State,
serviceline_Service_Facility_Zip_Code,
serviceline_Referring_Provider_Last_Name,
serviceline_Referring_Provider_First_Name,
serviceline_Referring_Provider_MI,
serviceline_Referring_Provider_NPI,
string_set_diff(diag_concat,
rd.related) as unrelated 
FROM allscripts_claims_base b
    INNER JOIN related_diags rd USING (entity_id)
WHERE charge_line_number = '1';

INSERT INTO medicalclaims_common_model (
claim_id,
hvid,
source_version,
patient_gender,
patient_year_of_birth,
patient_zip3,
patient_state,
claim_type,
date_received,
date_service,
date_service_end,
place_of_service_std_id,
service_line_number,
diagnosis_code,
procedure_code,
procedure_units,
procedure_modifier_1,
procedure_modifier_2,
procedure_modifier_3,
procedure_modifier_4,
ndc_code,
medical_coverage_type,
line_charge,
total_charge,
prov_rendering_npi,
prov_billing_npi,
prov_referring_npi,
prov_facility_npi,
payer_name,
payer_type,
prov_rendering_tax_id,
prov_rendering_ssn,
prov_rendering_name_2,
prov_rendering_std_taxonomy,
prov_billing_tax_id,
prov_billing_ssn,
prov_billing_name_1,
prov_billing_name_2,
prov_billing_address_1,
prov_billing_address_2,
prov_billing_city,
prov_billing_state,
prov_billing_zip,
prov_billing_std_taxonomy,
prov_referring_tax_id,
prov_referring_ssn,
prov_referring_name_2,
prov_referring_std_taxonomy,
prov_facility_tax_id,
prov_facility_ssn,
prov_facility_name_1,
prov_facility_address_1,
prov_facility_address_2,
prov_facility_city,
prov_facility_state,
prov_facility_zip,
cob_payer_vendor_id_1,
cob_payer_seq_code_1,
cob_payer_claim_filing_ind_code_1,
cob_ins_type_code_1,
cob_payer_vendor_id_2,
cob_payer_seq_code_2,
cob_payer_claim_filing_ind_code_2,
cob_ins_type_code_2
)
SELECT
b.entity_id,
hvid,
version_code,
patient_sex,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN service_from_date IS NOT NULL AND (patient_dob >= (extract('year' from service_from_date::date - '32873 days'::interval)::text)) AND patient_dob <= (extract('year' from getdate())::text) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threedigitzip as patient_zip3,
state as patient_state,
'P',
create_date,
service_from_date,
service_to_date,
place_of_service,
charge_line_number,
upper(replace(replace(replace(split_part(unrelated,':',n), ' ', ''), ',', ''), '.', '')) as diagnosis_code,
upper(replace(replace(std_chg_line_hcpcs_procedure_code, ' ', ''), ',', '')) as procedure_code,
units_of_service,
hcpcs_modifier_1,
hcpcs_modifier_2,
hcpcs_modifier_3,
hcpcs_modifier_4,
ndc_code,
source_of_payment,
line_charges,
total_claim_charge_amount,
CASE WHEN serviceline_rendering_provider_npi is not null THEN serviceline_rendering_provider_npi ELSE rendering_provider_npi END,
billing_prov_npi,
CASE WHEN serviceline_referring_provider_npi is not null THEN serviceline_referring_provider_npi ELSE referring_prov_npi END,
CASE WHEN serviceline_service_facility_npi is not null THEN serviceline_service_facility_npi ELSE facility_lab_npi END,
primary_payer_name,
insurance_type_code,
CASE WHEN
        serviceline_rendering_provider_primary_id is not null
        and (serviceline_rendering_provider_tax_id_qual = '24' 
        or serviceline_rendering_provider_tax_id_qual = '34')
    THEN serviceline_rendering_provider_primary_id
    WHEN
        rendering_provider_primary_id_qualifier = '24'
        or rendering_provider_primary_id_qualifier = '34'
    THEN rendering_provider_primary_id
    ELSE NULL
    END,
CASE WHEN
        serviceline_rendering_provider_primary_id is not null
        and serviceline_rendering_provider_tax_id_qual = '24' 
    THEN serviceline_rendering_provider_primary_id
    WHEN
        rendering_provider_primary_id_qualifier = '24'
    THEN rendering_provider_primary_id
    ELSE NULL
    END,
CASE WHEN serviceline_rendering_provider_last is not null
    THEN serviceline_rendering_provider_last ||
        COALESCE(', ' || serviceline_rendering_provider_first, '') ||
        COALESCE(', ' || serviceline_rendering_provider_middle, '')
    ELSE rendering_provider_last ||
        COALESCE(', ' || rendering_provider_first, '') ||
        COALESCE(', ' || rendering_provider_middle, '')
    END,
CASE WHEN serviceline_rendering_provider_specialty_code is not null
    THEN serviceline_rendering_provider_specialty_code
    ELSE rendering_provider_specialty_code
    END,
CASE WHEN
        billing_prov_id_qual = '24'
        or billing_prov_id_qual = '34'
    THEN billing_prov_tax_id
    ELSE NULL
    END,
CASE WHEN billing_prov_id_qual = '24' THEN billing_prov_tax_id ELSE NULL END,
billing_prov_organization_name_or_billing_prov_last_name,
billing_prov_last_name || COALESCE(', ' || billing_prov_first_name, '') ||
    COALESCE(', ' || billing_prov_mi, ''),
billing_provider_s_address_1,
billing_provider_s_address_2,
billing_provider_s_city,
billing_provider_s_state,
billing_provider_s_zip,
billing_or_pay_to_provider_taxonomy_code,
CASE WHEN
        referring_provider_primary_id_qualifier = '24'
        or referring_provider_primary_id_qualifier = '34'
    THEN referring_provider_primary_id
    ELSE NULL
    END,
CASE WHEN referring_provider_primary_id_qualifier = '24' THEN referring_provider_primary_id ELSE NULL END,
CASE WHEN serviceline_referring_provider_last_name is not null
    THEN serviceline_referring_provider_last_name ||
        COALESCE(', ' || serviceline_referring_provider_first_name, '') ||
        COALESCE(', ' || serviceline_referring_provider_mi, '')
    ELSE referring_provider_last_name ||
        COALESCE(', ' || referring_provider_first_name, '') ||
        COALESCE(', ' || referring_provider_middle_initial, '')
    END,
referring_provider_taxonomy_code,
CASE WHEN
        facility_lab_primary_id_qualifier = '24'
        or facility_lab_primary_id_qualifier = '34'
    THEN facility_laboratory_primary_identifier
    ELSE NULL
    END,
CASE WHEN facility_lab_primary_id_qualifier = '24' THEN facility_laboratory_primary_identifier ELSE NULL END,
CASE WHEN serviceline_service_facility_name is not null THEN serviceline_service_facility_name ELSE facility_laboratory_name END,
CASE WHEN serviceline_service_facility_address_1 is not null THEN serviceline_service_facility_address_1 ELSE facility_laboratory_street_address_1 END,
CASE WHEN serviceline_service_facility_address_2 is not null THEN serviceline_service_facility_address_2 ELSE facility_laboratory_street_address_2 END,
CASE WHEN serviceline_service_facility_city is not null THEN serviceline_service_facility_city ELSE facility_laboratory_city END,
CASE WHEN serviceline_service_facility_state is not null THEN serviceline_service_facility_state ELSE facility_laboratory_state END,
CASE WHEN serviceline_service_facility_zip_code is not null THEN serviceline_service_facility_zip_code ELSE facility_laboratory_zip_code END,
second_payer_primary_id,
secondary_payer_sequence_number,
secondary_payer_source_of_payment,
secondary_payer_insurance_type_code,
third_payer_primary_id,
teritary_payer_sequence_number,
teritary_payer_source_of_payment,
teritary_payer_insurance_type_code
FROM allscripts_claims_unrelated b
    CROSS JOIN split_indices
    LEFT JOIN matching_payload ON b.entity_id = entityid
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
WHERE split_part(unrelated,':',n) IS NOT NULL AND split_part(unrelated,':',n) <> '';

