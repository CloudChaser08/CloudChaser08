SELECT
    addon.hv_join_key,
    trunk.accn_id,
    trunk.dosid,
    trunk.local_order_code,
    trunk.standard_order_code,
    trunk.order_name,
    trunk.loinc_code,
    trunk.local_result_code,
    trunk.result_name,
    addon.lab_id,
    addon.date_of_service,
    addon.date_collected,
    addon.diagnosis_code,
    addon.icd_codeset_ind,
    COALESCE(addon.acct_zip, prov_addon.acct_zip, NULL) AS acct_zip,
    COALESCE(addon.npi, prov_addon.npi, NULL) AS npi,
    trunk.input_file_name
FROM transactions_trunk trunk
    INNER JOIN transactions_addon addon
        ON trunk.accn_id = addon.accn_id
        AND trunk.dosid = addon.dosid
    LEFT JOIN transactions_provider_addon prov_addon
        ON trunk.accn_id = prov_addon.accn_id
        AND trunk.dosid = prov_addon.dosid
