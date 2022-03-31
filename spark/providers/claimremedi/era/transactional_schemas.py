from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(   
        'csv',
        separator='|',
        columns=[
            'mystery_column',
            'ignore_1',
            'ignore_2',
            'ignore_3',
            'ignore_4',
            'ignore_5',
            'ignore_6',
            'ignore_7',
            'ignore_8',
            'ignore_9',
            'ignore_10',
            'ignore_11',
            'ignore_12',
            'ignore_13',
            'ignore_14',
            'ignore_15',
            'ignore_16',
            'ignore_17',
            'ignore_18',
            'ignore_19',
            'ignore_20',
            'ignore_21',
            'ignore_22',
            'ignore_23',
            'ignore_24',
            'ignore_25',
            'ignore_26',
            'ignore_27',
            'ignore_28',
            'ignore_29',
            'ignore_30',
            'ignore_31',
            'ignore_32',
            'ignore_33',
            'ignore_34',
            'ignore_35',
            'ignore_36',
            'ignore_37',
            'ignore_38',
            'ignore_39',
            'ignore_40',
            'ignore_41',
            'ignore_42',
            'ignore_43',
            'ignore_44',
            'ignore_45',
            'ignore_46',
            'ignore_47',
            'ignore_48',
            'ignore_49',
            'ignore_50',
            'ignore_51',
            'ignore_52',
            'ignore_53',
            'ignore_54',
            'ignore_55',
            'ignore_56',
            'ignore_57',
            'ignore_58',
            'ignore_59',
            'ignore_60',
            'ignore_61',
            'ignore_62',
            'ignore_63',
            'ignore_64',
            'ignore_65',
            'ignore_66',
            'ignore_67',
            'ignore_68',
            'ignore_69',
            'ignore_70',
            'ignore_71',
            'ignore_72',
            'ignore_73',
            'ignore_74',
            'ignore_75',
            'ignore_76',
            'ignore_77',
            'ignore_78',
            'ignore_79',
            'ignore_80',
            'ignore_81',
            'ignore_82',
            'ignore_83',
            'ignore_84',
            'ignore_85',
            'ignore_86',
            'ignore_87',
            'ignore_88',
            'ignore_89',
            'ignore_90',
            'ignore_91',
            'ignore_92',
            'ignore_93',
            'ignore_94',
            'ignore_95',
            'ignore_96',
            'ignore_97',
            'ignore_98',
            'ignore_99',
            'ignore_100',
            'ignore_101',
            'ignore_102',
            'ignore_103',
            'ignore_104',
            'ignore_105',
            'ignore_106',
            'ignore_107',
            'ignore_108',
            'ignore_109',
            'ignore_110',
            'ignore_111',
            'ignore_112',
            'ignore_113',
            'ignore_114',
            'ignore_115',
            'ignore_116',
            'ignore_117',
            'ignore_118',
            'ignore_119',
            'ignore_120',
            'ignore_121',
            'ignore_122',
            'ignore_123',
            'ignore_124',
            'ignore_125',
            'ignore_126',
            'ignore_127',
            'ignore_128',
            'ignore_129',
            'ignore_130',
            'ignore_131',
            'ignore_132',
            'ignore_133',
            'ignore_134',
            'ignore_135',
            'ignore_136',
            'ignore_137',
            'ignore_138',
            'ignore_139',
            'ignore_140',
            'ignore_141',
            'ignore_142',
            'ignore_143',
            'ignore_144',
            'ignore_145',
            'ignore_146',
            'ignore_147',
            'ignore_148',
            'ignore_149',
            'ignore_150',
            'ignore_151',
            'src_era_claim_id',
            'src_claim_id',
            'claim_type_cd',
            'edi_interchange_creation_dt',
            'edi_interchange_creation_time',
            'txn_handling_cd',
            'tot_actual_provdr_paymt_amt',
            'credit_debit_fl_cd',
            'paymt_method_cd',
            'paymt_eff_dt',
            'payer_claim_cntl_nbr',
            'payer_claim_cntl_nbr_origl',
            'payer_nm',
            'payer_cms_plan_id',
            'payer_id',
            'payer_naic_id',
            'payer_group_nbr',
            'payer_claim_flng_ind_cd',
            'payee_tax_id',
            'payee_npi',
            'payee_nm',
            'payee_addr_1',
            'payee_addr_2',
            'payee_addr_city',
            'payee_addr_state',
            'payee_addr_zip',
            'rendr_provdr_npi',
            'rendr_provdr_upin',
            'rendr_provdr_comm_nbr',
            'rendr_provdr_loc_nbr',
            'rendr_provdr_stlc_nbr',
            'rendr_provdr_last_nm',
            'rendr_provdr_first_nm',
            'claim_sts_cd',
            'tot_claim_charg_amt',
            'tot_paid_amt',
            'patnt_rspbty_amt',
            'fclty_type_pos_cd',
            'claim_freq_cd',
            'stmnt_from_dt',
            'stmnt_to_dt',
            'covrg_expired_dt',
            'claim_received_dt',
            'drg_cd',
            'drg_amt',
            'clm_cas',
            'src_era_svc_id',
            'src_svc_id',
            'line_item_cntl_nbr',
            'svc_rendr_provdr_npi',
            'svc_rendr_provdr_upin',
            'svc_rendr_provdr_comm_nbr',
            'svc_rendr_provdr_loc_nbr',
            'svc_rendr_provdr_stlc_nbr',
            'adjtd_proc_cd_qual',
            'adjtd_proc_cd',
            'adjtd_proc_modfr_1',
            'adjtd_proc_modfr_2',
            'adjtd_proc_modfr_3',
            'adjtd_proc_modfr_4',
            'line_item_charg_amt',
            'line_item_allowed_amt',
            'line_item_paid_amt',
            'revnu_cd',
            'paid_units',
            'submd_proc_cd_qual',
            'submd_proc_cd',
            'submd_proc_modfr_1',
            'submd_proc_modfr_2',
            'submd_proc_modfr_3',
            'submd_proc_modfr_4',
            'origl_units_of_svc_cnt',
            'svc_from_dt',
            'svc_to_dt',
            'svc_cas'
        ]
    )
}
