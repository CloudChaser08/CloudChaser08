SELECT
    npi                         AS prov_rendering_npi,
    cpt_code                    AS procedure_code,
    '0_'                        AS procedure_code_qual
FROM careset_transactions
