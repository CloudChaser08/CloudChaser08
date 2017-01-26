-- Create a table for medical claim data with only the columns we need for mapping
DROP TABLE IF EXISTS allscripts_dx_raw_claims;
CREATE TABLE allscripts_dx_raw_claims (
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
Third_Payer_Primary_ID text ENCODE lzo
)
DISTKEY(entity_id) SORTKEY(entity_id);

-- Create a table for medical service data with only the columns we need for mapping
DROP TABLE IF EXISTS allscripts_dx_raw_service;
CREATE TABLE allscripts_dx_raw_service (
Entity_ID text ENCODE lzo,
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
Rendering_Provider_Last text ENCODE lzo,
Rendering_Provider_First text ENCODE lzo,
Rendering_Provider_Middle text ENCODE lzo,
Rendering_Provider_Tax_ID_Qual text ENCODE lzo,
Rendering_Provider_Primary_ID text ENCODE lzo,
Rendering_Provider_NPI text ENCODE lzo,
Rendering_Provider_Specialty_Code text ENCODE lzo,
Service_Facility_Name text ENCODE lzo,
Service_Facility_NPI text ENCODE lzo,
Service_Facility_Address_1 text ENCODE lzo,
Service_Facility_Address_2 text ENCODE lzo,
Service_Facility_City text ENCODE lzo,
Service_Facility_State text ENCODE lzo,
Service_Facility_Zip_Code text ENCODE lzo,
Referring_Provider_Last_Name text ENCODE lzo,
Referring_Provider_First_Name text ENCODE lzo,
Referring_Provider_MI text ENCODE lzo,
Referring_Provider_NPI text ENCODE lzo
)
DISTKEY(entity_id) SORTKEY(entity_id);

-- Select medical claim data (column1 = 'H') from the transactions table and insert all necessary columns
-- Normalize gender in column 50 (patient_gender in claims) to 'M', 'F', or 'U'
-- Normalize numeric columns (column62 => total_charge, column48 => year_of_birth)
-- Normalize date columns (column4 => date_received)
INSERT INTO allscripts_dx_raw_claims SELECT
date_received.formatted,
column6,
column7,
column9,
column10,
column11,
column12,
column13,
column14,
column15,
column16,
column17,
column18,
column19,
column20,
column21,
column36,
column37,
CASE WHEN (length(column48)-length(replace(column48,'.',''))) = 1 THEN
('0' || regexp_replace(column48, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(column48, '[^0-9]'))::bigint::text END,
CASE WHEN UPPER(column50) = 'M' THEN 'M' WHEN UPPER(column50) = 'F' THEN 'F' ELSE 'U' END,
column54,
CASE WHEN (length(column62)-length(replace(column62,'.',''))) = 1 THEN
('0' || regexp_replace(column62, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(column62, '[^0-9]'))::bigint::text END,
column129,
column130,
column131,
column132,
column133,
column134,
column135,
column136,
column150,
column151,
column152,
column153,
column154,
column155,
column156,
column157,
column158,
column159,
column160,
column168,
column169,
column170,
column171,
column172,
column173,
column174,
column175,
column176,
column183,
column187,
column188,
column189,
column193,
column194,
column392,
column394
FROM allscripts_dx_raw
    LEFT JOIN dates date_received ON ('20' || column4) = date_received.date
WHERE column1 = 'H';

-- Select medical service data (column1 = 'S') from the transactions table and insert the required columns into the service table
-- Normalize numeric columns (column9 => line_charge, column10 => procedure_units)
-- Normalize date columns (column57 => date_service, column58 => date_service_ends)
INSERT INTO allscripts_dx_raw_service SELECT
column2,
column3,
column4,
column5,
column6,
column7,
column8,
CASE WHEN (length(column9)-length(replace(column9,'.',''))) = 1 THEN
('0' || regexp_replace(column9, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(column10, '[^0-9]'))::bigint::text END,
CASE WHEN (length(column10)-length(replace(column10,'.',''))) = 1 THEN
('0' || regexp_replace(column10, '[^0-9.]') || '0')::float::text
ELSE ('0' || regexp_replace(column10, '[^0-9]'))::bigint::text END,
column12,
column13,
column14,
column15,
column16,
date_service.formatted,
date_service_ends.formatted,
column104,
column109,
column110,
column111,
column112,
column113,
column114,
column115,
column118,
column119,
column120,
column121,
column122,
column123,
column124,
column138,
column139,
column140,
column141
FROM allscripts_dx_raw
    LEFT JOIN dates date_service ON column57 = date_service.date
    LEFT JOIN dates date_service_ends ON column58 = date_service_ends.date
WHERE column1 = 'S';
