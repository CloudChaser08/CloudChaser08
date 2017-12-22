DROP TABLE IF EXISTS {celgene_schema}.nppes_extract;
CREATE TABLE {celgene_schema}.nppes_extract AS
SELECT
    npi,
    entity_type,
    replacement_npi,
    ein,
    org_name,
    last_name,
    first_name,
    middle_name,
    name_prefix,
    name_suffix,
    practice_address1,
    practice_address2,
    practice_city,
    practice_state,
    practice_postal,
    practice_country,
    practice_phone,
    taxonomy_code_1
FROM default.ref_nppes a
    INNER JOIN cel242.pharmacyclaims_extract b ON a.npi = b.prov_prescribing_npi
ORDER BY 1
    ;
