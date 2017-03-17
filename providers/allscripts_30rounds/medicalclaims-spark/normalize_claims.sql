-- Create table to hold the basic structure of wide claim rows
DROP TABLE IF EXISTS allscripts_claims_base;
CREATE TABLE allscripts_claims_base(
Create_Date string,
Version_Code string,
Entity_ID string,
Billing_or_Pay_To_Provider_Taxonomy_Code string,
Billing_Prov_Organization_Name_or_Billing_Prov_Last_Name string,
Billing_Prov_Last_Name string,
Billing_Prov_First_Name string,
Billing_Prov_MI string,
Billing_Prov_ID_Qual string,
Billing_Prov_Tax_ID string,
Billing_Prov_NPI string,
Billing_Provider_s_Address_1 string,
Billing_Provider_s_Address_2 string,
Billing_Provider_s_City string,
Billing_Provider_s_State string,
Billing_Provider_s_Zip string,
Insurance_Type_Code string,
Source_of_Payment string,
Patient_DOB string,
Patient_Sex string,
Primary_Payer_Name string,
Total_Claim_Charge_Amount string,
Diagnosis_Code_1 string,
Diagnosis_Code_2 string,
Diagnosis_Code_3 string,
Diagnosis_Code_4 string,
Diagnosis_Code_5 string,
Diagnosis_Code_6 string,
Diagnosis_Code_7 string,
Diagnosis_Code_8 string,
Referring_Provider_Last_Name string,
Referring_Provider_First_Name string,
Referring_Provider_Middle_Initial string,
Referring_Provider_Primary_ID_Qualifier string,
Referring_Provider_Primary_ID string,
Referring_Prov_NPI string,
Referring_Provider_Taxonomy_Code string,
Rendering_Provider_Last string,
Rendering_Provider_First string,
Rendering_Provider_Middle string,
Rendering_Provider_Primary_ID_Qualifier string,
Rendering_Provider_Primary_ID string,
Rendering_Provider_NPI string,
Rendering_Provider_Specialty_Code string,
Facility_Laboratory_Name string,
Facility_Lab_Primary_ID_Qualifier string,
Facility_Laboratory_Primary_Identifier string,
Facility_Lab_NPI string,
Facility_Laboratory_Street_Address_1 string,
Facility_Laboratory_Street_Address_2 string,
Facility_Laboratory_City string,
Facility_Laboratory_State string,
Facility_Laboratory_Zip_Code string,
Secondary_Payer_Sequence_Number string,
Secondary_Payer_Insurance_Type_Code string,
Secondary_Payer_Source_of_Payment string,
Teritary_Payer_Sequence_Number string,
Teritary_Payer_Insurance_Type_Code string,
Teritary_Payer_Source_of_Payment string,
Second_Payer_Primary_ID string,
Third_Payer_Primary_ID string,
Charge_Line_Number string,
Std_Chg_Line_HCPCS_Procedure_Code string,
HCPCS_Modifier_1 string,
HCPCS_Modifier_2 string,
HCPCS_Modifier_3 string,
HCPCS_Modifier_4 string,
Line_Charges string,
Units_of_Service string,
Place_of_Service string,
Diagnosis_Code_Pointer_1 string,
Diagnosis_Code_Pointer_2 string,
Diagnosis_Code_Pointer_3 string,
Diagnosis_Code_Pointer_4 string,
Service_From_Date string,
Service_To_Date string,
NDC_CODE string,
serviceline_Rendering_Provider_Last string,
serviceline_Rendering_Provider_First string,
serviceline_Rendering_Provider_Middle string,
serviceline_Rendering_Provider_Tax_ID_Qual string,
serviceline_Rendering_Provider_Primary_ID string,
serviceline_Rendering_Provider_NPI string,
serviceline_Rendering_Provider_Specialty_Code string,
serviceline_Service_Facility_Name string,
serviceline_Service_Facility_NPI string,
serviceline_Service_Facility_Address_1 string,
serviceline_Service_Facility_Address_2 string,
serviceline_Service_Facility_City string,
serviceline_Service_Facility_State string,
serviceline_Service_Facility_Zip_Code string,
serviceline_Referring_Provider_Last_Name string,
serviceline_Referring_Provider_First_Name string,
serviceline_Referring_Provider_MI string,
serviceline_Referring_Provider_NPI string,
diag_concat string,
svcptr_concat string,
related string
);

DROP TABLE IF EXISTS related_diags_tmp;
CREATE TABLE related_diags_tmp (entity_id string, related string);

DROP TABLE IF EXISTS related_diags;
CREATE TABLE related_diags (entity_id string, related varchar(5000));

DROP TABLE IF EXISTS allscripts_claims_unrelated;
CREATE TABLE allscripts_claims_unrelated (
Create_Date string,
Version_Code string,
Entity_ID string,
Billing_or_Pay_To_Provider_Taxonomy_Code string,
Billing_Prov_Organization_Name_or_Billing_Prov_Last_Name string,
Billing_Prov_Last_Name string,
Billing_Prov_First_Name string,
Billing_Prov_MI string,
Billing_Prov_ID_Qual string,
Billing_Prov_Tax_ID string,
Billing_Prov_NPI string,
Billing_Provider_s_Address_1 string,
Billing_Provider_s_Address_2 string,
Billing_Provider_s_City string,
Billing_Provider_s_State string,
Billing_Provider_s_Zip string,
Insurance_Type_Code string,
Source_of_Payment string,
Patient_DOB string,
Patient_Sex string,
Primary_Payer_Name string,
Total_Claim_Charge_Amount string,
Diagnosis_Code_1 string,
Diagnosis_Code_2 string,
Diagnosis_Code_3 string,
Diagnosis_Code_4 string,
Diagnosis_Code_5 string,
Diagnosis_Code_6 string,
Diagnosis_Code_7 string,
Diagnosis_Code_8 string,
Referring_Provider_Last_Name string,
Referring_Provider_First_Name string,
Referring_Provider_Middle_Initial string,
Referring_Provider_Primary_ID_Qualifier string,
Referring_Provider_Primary_ID string,
Referring_Prov_NPI string,
Referring_Provider_Taxonomy_Code string,
Rendering_Provider_Last string,
Rendering_Provider_First string,
Rendering_Provider_Middle string,
Rendering_Provider_Primary_ID_Qualifier string,
Rendering_Provider_Primary_ID string,
Rendering_Provider_NPI string,
Rendering_Provider_Specialty_Code string,
Facility_Laboratory_Name string,
Facility_Lab_Primary_ID_Qualifier string,
Facility_Laboratory_Primary_Identifier string,
Facility_Lab_NPI string,
Facility_Laboratory_Street_Address_1 string,
Facility_Laboratory_Street_Address_2 string,
Facility_Laboratory_City string,
Facility_Laboratory_State string,
Facility_Laboratory_Zip_Code string,
Secondary_Payer_Sequence_Number string,
Secondary_Payer_Insurance_Type_Code string,
Secondary_Payer_Source_of_Payment string,
Teritary_Payer_Sequence_Number string,
Teritary_Payer_Insurance_Type_Code string,
Teritary_Payer_Source_of_Payment string,
Second_Payer_Primary_ID string,
Third_Payer_Primary_ID string,
Charge_Line_Number string,
Std_Chg_Line_HCPCS_Procedure_Code string,
HCPCS_Modifier_1 string,
HCPCS_Modifier_2 string,
HCPCS_Modifier_3 string,
HCPCS_Modifier_4 string,
Line_Charges string,
Units_of_Service string,
Place_of_Service string,
Diagnosis_Code_Pointer_1 string,
Diagnosis_Code_Pointer_2 string,
Diagnosis_Code_Pointer_3 string,
Diagnosis_Code_Pointer_4 string,
Service_From_Date string,
Service_To_Date string,
NDC_CODE string,
serviceline_Rendering_Provider_Last string,
serviceline_Rendering_Provider_First string,
serviceline_Rendering_Provider_Middle string,
serviceline_Rendering_Provider_Tax_ID_Qual string,
serviceline_Rendering_Provider_Primary_ID string,
serviceline_Rendering_Provider_NPI string,
serviceline_Rendering_Provider_Specialty_Code string,
serviceline_Service_Facility_Name string,
serviceline_Service_Facility_NPI string,
serviceline_Service_Facility_Address_1 string,
serviceline_Service_Facility_Address_2 string,
serviceline_Service_Facility_City string,
serviceline_Service_Facility_State string,
serviceline_Service_Facility_Zip_Code string,
serviceline_Referring_Provider_Last_Name string,
serviceline_Referring_Provider_First_Name string,
serviceline_Referring_Provider_MI string,
serviceline_Referring_Provider_NPI string,
unrelated string
);

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
concat_ws(':',
diagnosis_code_1,
diagnosis_code_2,
diagnosis_code_3,
diagnosis_code_4,
diagnosis_code_5,
diagnosis_code_6,
diagnosis_code_7,
diagnosis_code_8
),
concat_ws(':',
diagnosis_code_pointer_1,
diagnosis_code_pointer_2,
diagnosis_code_pointer_3,
diagnosis_code_pointer_4
),
get_diagnosis_with_priority(concat_ws(':',
diagnosis_code_1,
diagnosis_code_2,
diagnosis_code_3,
diagnosis_code_4,
diagnosis_code_5,
diagnosis_code_6,
diagnosis_code_7,
diagnosis_code_8
),
concat_ws(':',
diagnosis_code_pointer_1,
diagnosis_code_pointer_2,
diagnosis_code_pointer_3,
diagnosis_code_pointer_4
)) as related
FROM allscripts_dx_raw_claims claims
    INNER JOIN allscripts_dx_raw_service servicelines USING (entity_id)
CLUSTER BY entity_id;

INSERT INTO TABLE medicalclaims_common_model
SELECT
NULL,
b.entity_id,
hvid,
{today},
'1',
{filename},
{feedname},
{vendor},
version_code,
patient_sex,
NULL,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN service_from_date IS NOT NULL AND (patient_dob >= cast(year(date_sub(service_from_date, 32873)) as string)) AND patient_dob <= cast(year(current_date()) as string) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threedigitzip as patient_zip3,
state as patient_state,
'P',
create_date,
service_from_date,
service_to_date,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
place_of_service,
NULL,
NULL,
charge_line_number,
upper(regexp_replace(regexp_replace(regexp_replace(split(split(related,':')[n-1],'_')[0], ' ', ''), ',', ''), '\\.', '')) as diagnosis_code,
NULL,
split(split(related,':')[n-1],'_')[1] as diagnosis_priority,
NULL,
upper(regexp_replace(regexp_replace(std_chg_line_hcpcs_procedure_code, ' ', ''), ',', '')) as procedure_code,
NULL,
NULL,
units_of_service,
hcpcs_modifier_1,
hcpcs_modifier_2,
hcpcs_modifier_3,
hcpcs_modifier_4,
NULL,
ndc_code,
source_of_payment,
line_charges,
NULL,
total_claim_charge_amount,
NULL,
CASE WHEN serviceline_rendering_provider_npi is not null THEN serviceline_rendering_provider_npi ELSE rendering_provider_npi END,
billing_prov_npi,
CASE WHEN serviceline_referring_provider_npi is not null THEN serviceline_referring_provider_npi ELSE referring_prov_npi END,
CASE WHEN serviceline_service_facility_npi is not null THEN serviceline_service_facility_npi ELSE facility_lab_npi END,
NULL,
primary_payer_name,
NULL,
NULL,
NULL,
NULL,
insurance_type_code,
NULL,
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
NULL,
CASE WHEN
        serviceline_rendering_provider_primary_id is not null
        and serviceline_rendering_provider_tax_id_qual = '24' 
    THEN serviceline_rendering_provider_primary_id
    WHEN
        rendering_provider_primary_id_qualifier = '24'
    THEN rendering_provider_primary_id
    ELSE NULL
    END,
NULL,
NULL,
NULL,
NULL,
CASE WHEN serviceline_rendering_provider_last is not null
    THEN concat_ws(', ', serviceline_rendering_provider_last,
        serviceline_rendering_provider_first,
        serviceline_rendering_provider_middle)
    ELSE concat_ws(', ', rendering_provider_last,
        rendering_provider_first,
        rendering_provider_middle)
    END,
NULL,
NULL,
NULL,
NULL,
NULL,
CASE WHEN serviceline_rendering_provider_specialty_code is not null
    THEN serviceline_rendering_provider_specialty_code
    ELSE rendering_provider_specialty_code
    END,
NULL,
NULL,
CASE WHEN
        billing_prov_id_qual = '24'
        or billing_prov_id_qual = '34'
    THEN billing_prov_tax_id
    ELSE NULL
    END,
NULL,
CASE WHEN billing_prov_id_qual = '24' THEN billing_prov_tax_id ELSE NULL END,
NULL,
NULL,
NULL,
billing_prov_organization_name_or_billing_prov_last_name,
concat_ws(', ', billing_prov_last_name, billing_prov_first_name, billing_prov_mi),
billing_provider_s_address_1,
billing_provider_s_address_2,
billing_provider_s_city,
billing_provider_s_state,
billing_provider_s_zip,
billing_or_pay_to_provider_taxonomy_code,
NULL,
NULL,
CASE WHEN
        referring_provider_primary_id_qualifier = '24'
        or referring_provider_primary_id_qualifier = '34'
    THEN referring_provider_primary_id
    ELSE NULL
    END,
NULL,
CASE WHEN referring_provider_primary_id_qualifier = '24' THEN referring_provider_primary_id ELSE NULL END,
NULL,
NULL,
NULL,
NULL,
CASE WHEN serviceline_referring_provider_last_name is not null
    THEN concat_ws(', ', serviceline_referring_provider_last_name,
        serviceline_referring_provider_first_name,
        serviceline_referring_provider_mi)
    ELSE concat_ws(', ', referring_provider_last_name,
        referring_provider_first_name,
        referring_provider_middle_initial)
    END,
NULL,
NULL,
NULL,
NULL,
NULL,
referring_provider_taxonomy_code,
NULL,
NULL,
CASE WHEN
        facility_lab_primary_id_qualifier = '24'
        or facility_lab_primary_id_qualifier = '34'
    THEN facility_laboratory_primary_identifier
    ELSE NULL
    END,
NULL,
CASE WHEN facility_lab_primary_id_qualifier = '24' THEN facility_laboratory_primary_identifier ELSE NULL END,
NULL,
NULL,
NULL,
CASE WHEN serviceline_service_facility_name is not null THEN serviceline_service_facility_name ELSE facility_laboratory_name END,
NULL,
CASE WHEN serviceline_service_facility_address_1 is not null THEN serviceline_service_facility_address_1 ELSE facility_laboratory_street_address_1 END,
CASE WHEN serviceline_service_facility_address_2 is not null THEN serviceline_service_facility_address_2 ELSE facility_laboratory_street_address_2 END,
CASE WHEN serviceline_service_facility_city is not null THEN serviceline_service_facility_city ELSE facility_laboratory_city END,
CASE WHEN serviceline_service_facility_state is not null THEN serviceline_service_facility_state ELSE facility_laboratory_state END,
CASE WHEN serviceline_service_facility_zip_code is not null THEN serviceline_service_facility_zip_code ELSE facility_laboratory_zip_code END,
NULL,
NULL,
second_payer_primary_id,
secondary_payer_sequence_number,
NULL,
secondary_payer_source_of_payment,
secondary_payer_insurance_type_code,
third_payer_primary_id,
teritary_payer_sequence_number,
NULL,
teritary_payer_source_of_payment,
teritary_payer_insurance_type_code
FROM allscripts_claims_base b
    CROSS JOIN split_indices
    LEFT JOIN matching_payload ON b.entity_id = entityid
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
WHERE split(related,':')[n-1] IS NOT NULL AND split(related,':')[n-1] <> '';

-- There can be a lot of raws with the same diagnosis. Only extract unique ones
INSERT INTO related_diags_tmp
SELECT DISTINCT entity_id, related
FROM allscripts_claims_base;

INSERT INTO related_diags
SELECT entity_id, concat_ws(':', collect_list(related)) AS related
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

INSERT INTO TABLE medicalclaims_common_model
SELECT
NULL,
b.entity_id,
hvid,
{today},
'1',
{filename},
{feedname},
{vendor},
version_code,
patient_sex,
NULL,
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
CASE WHEN service_from_date IS NOT NULL AND (patient_dob >= cast(year(date_sub(service_from_date, 32873)) as string)) AND patient_dob <= cast(year(current_date()) as string) THEN patient_dob ELSE NULL END as patient_year_of_birth,
threedigitzip as patient_zip3,
state as patient_state,
'P',
create_date,
service_from_date,
service_to_date,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
place_of_service,
NULL,
NULL,
charge_line_number,
upper(regexp_replace(regexp_replace(regexp_replace(split(unrelated,':')[n-1], ' ', ''), ',', ''), '\\.', '')) as diagnosis_code,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
source_of_payment,
line_charges,
NULL,
total_claim_charge_amount,
NULL,
CASE WHEN serviceline_rendering_provider_npi is not null THEN serviceline_rendering_provider_npi ELSE rendering_provider_npi END,
billing_prov_npi,
CASE WHEN serviceline_referring_provider_npi is not null THEN serviceline_referring_provider_npi ELSE referring_prov_npi END,
CASE WHEN serviceline_service_facility_npi is not null THEN serviceline_service_facility_npi ELSE facility_lab_npi END,
NULL,
primary_payer_name,
NULL,
NULL,
NULL,
NULL,
insurance_type_code,
NULL,
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
NULL,
CASE WHEN
        serviceline_rendering_provider_primary_id is not null
        and serviceline_rendering_provider_tax_id_qual = '24' 
    THEN serviceline_rendering_provider_primary_id
    WHEN
        rendering_provider_primary_id_qualifier = '24'
    THEN rendering_provider_primary_id
    ELSE NULL
    END,
NULL,
NULL,
NULL,
NULL,
CASE WHEN serviceline_rendering_provider_last is not null
    THEN concat_ws(', ', serviceline_rendering_provider_last,
        serviceline_rendering_provider_first,
        serviceline_rendering_provider_middle)
    ELSE concat_ws(', ', rendering_provider_last,
        rendering_provider_first,
        rendering_provider_middle)
    END,
NULL,
NULL,
NULL,
NULL,
NULL,
CASE WHEN serviceline_rendering_provider_specialty_code is not null
    THEN serviceline_rendering_provider_specialty_code
    ELSE rendering_provider_specialty_code
    END,
NULL,
NULL,
CASE WHEN
        billing_prov_id_qual = '24'
        or billing_prov_id_qual = '34'
    THEN billing_prov_tax_id
    ELSE NULL
    END,
NULL,
CASE WHEN billing_prov_id_qual = '24' THEN billing_prov_tax_id ELSE NULL END,
NULL,
NULL,
NULL,
billing_prov_organization_name_or_billing_prov_last_name,
concat_ws(', ', billing_prov_last_name, billing_prov_first_name, billing_prov_mi),
billing_provider_s_address_1,
billing_provider_s_address_2,
billing_provider_s_city,
billing_provider_s_state,
billing_provider_s_zip,
billing_or_pay_to_provider_taxonomy_code,
NULL,
NULL,
CASE WHEN
        referring_provider_primary_id_qualifier = '24'
        or referring_provider_primary_id_qualifier = '34'
    THEN referring_provider_primary_id
    ELSE NULL
    END,
NULL,
CASE WHEN referring_provider_primary_id_qualifier = '24' THEN referring_provider_primary_id ELSE NULL END,
NULL,
NULL,
NULL,
NULL,
CASE WHEN serviceline_referring_provider_last_name is not null
    THEN concat_ws(', ', serviceline_referring_provider_last_name,
        serviceline_referring_provider_first_name,
        serviceline_referring_provider_mi)
    ELSE concat_ws(', ', referring_provider_last_name,
        referring_provider_first_name,
        referring_provider_middle_initial)
    END,
NULL,
NULL,
NULL,
NULL,
NULL,
referring_provider_taxonomy_code,
NULL,
NULL,
CASE WHEN
        facility_lab_primary_id_qualifier = '24'
        or facility_lab_primary_id_qualifier = '34'
    THEN facility_laboratory_primary_identifier
    ELSE NULL
    END,
NULL,
CASE WHEN facility_lab_primary_id_qualifier = '24' THEN facility_laboratory_primary_identifier ELSE NULL END,
NULL,
NULL,
NULL,
CASE WHEN serviceline_service_facility_name is not null THEN serviceline_service_facility_name ELSE facility_laboratory_name END,
NULL,
CASE WHEN serviceline_service_facility_address_1 is not null THEN serviceline_service_facility_address_1 ELSE facility_laboratory_street_address_1 END,
CASE WHEN serviceline_service_facility_address_2 is not null THEN serviceline_service_facility_address_2 ELSE facility_laboratory_street_address_2 END,
CASE WHEN serviceline_service_facility_city is not null THEN serviceline_service_facility_city ELSE facility_laboratory_city END,
CASE WHEN serviceline_service_facility_state is not null THEN serviceline_service_facility_state ELSE facility_laboratory_state END,
CASE WHEN serviceline_service_facility_zip_code is not null THEN serviceline_service_facility_zip_code ELSE facility_laboratory_zip_code END,
NULL,
NULL,
second_payer_primary_id,
secondary_payer_sequence_number,
NULL,
secondary_payer_source_of_payment,
secondary_payer_insurance_type_code,
third_payer_primary_id,
teritary_payer_sequence_number,
NULL,
teritary_payer_source_of_payment,
teritary_payer_insurance_type_code
FROM allscripts_claims_unrelated b
    CROSS JOIN split_indices
    LEFT JOIN matching_payload ON b.entity_id = entityid
    LEFT JOIN zip3_to_state ON threeDigitZip = zip3
WHERE split(unrelated,':')[n-1] IS NOT NULL AND split(unrelated,':')[n-1] <> '';
