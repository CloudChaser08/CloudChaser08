SELECT
    hvid                 ,
    created              ,
    model_version        ,
    data_set             ,
    data_feed            ,
    data_vendor          ,
    patient_gender       ,
    source_record_date   ,
    date_start           ,
    date_end             ,
    benefit_type         ,
    payer_type           ,
    payer_grp_txt        ,
    part_provider        ,
    part_best_date
FROM inv_enr_norm_01 txn
GROUP BY
    hvid                 ,
    created              ,
    model_version        ,
    data_set             ,
    data_feed            ,
    data_vendor          ,
    patient_gender       ,
    source_record_date   ,
    date_start           ,
    date_end             ,
    benefit_type         ,
    payer_type           ,
    payer_grp_txt        ,
    part_provider        ,
    part_best_date