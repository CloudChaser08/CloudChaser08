from pyspark.sql.types import *
from spark.common.schema import Schema
from spark.common.utility.output_type import DataType

schema_v3 = StructType([
        StructField('row_id', LongType(), True),
        StructField('hv_lab_ord_id', StringType(), True),
        StructField('crt_dt', DateType(), True),
        StructField('mdl_vrsn_num', StringType(), True),
        StructField('data_set_nm', StringType(), True),
        StructField('src_vrsn_id', StringType(), True),
        StructField('hvm_vdr_id', IntegerType(), True),
        StructField('hvm_vdr_feed_id', IntegerType(), True),
        StructField('vdr_org_id', StringType(), True),
        StructField('vdr_lab_ord_id', StringType(), True),
        StructField('vdr_lab_ord_id_qual', StringType(), True),
        StructField('hvid', StringType(), True),
        StructField('ptnt_birth_yr', IntegerType(), True),
        StructField('ptnt_age_num', StringType(), True),
        StructField('ptnt_lvg_flg', StringType(), True),
        StructField('ptnt_dth_dt', DateType(), True),
        StructField('ptnt_gender_cd', StringType(), True),
        StructField('ptnt_state_cd', StringType(), True),
        StructField('ptnt_zip3_cd', StringType(), True),
        StructField('hv_enc_id', StringType(), True),
        StructField('enc_dt', DateType(), True),
        StructField('lab_ord_dt', DateType(), True),
        StructField('lab_ord_test_schedd_dt', DateType(), True),
        StructField('lab_ord_smpl_collctn_dt', DateType(), True),
        StructField('lab_ord_ordg_prov_npi', StringType(), True),
        StructField('lab_ord_ordg_prov_vdr_id', StringType(), True),
        StructField('lab_ord_ordg_prov_vdr_id_qual', StringType(), True),
        StructField('lab_ord_ordg_prov_alt_id', StringType(), True),
        StructField('lab_ord_ordg_prov_alt_id_qual', StringType(), True),
        StructField('lab_ord_ordg_prov_tax_id', StringType(), True),
        StructField('lab_ord_ordg_prov_dea_id', StringType(), True),
        StructField('lab_ord_ordg_prov_state_lic_id', StringType(), True),
        StructField('lab_ord_ordg_prov_comrcl_id', StringType(), True),
        StructField('lab_ord_ordg_prov_upin', StringType(), True),
        StructField('lab_ord_ordg_prov_ssn', StringType(), True),
        StructField('lab_ord_ordg_prov_nucc_taxnmy_cd', StringType(), True),
        StructField('lab_ord_ordg_prov_alt_taxnmy_id', StringType(), True),
        StructField('lab_ord_ordg_prov_alt_taxnmy_id_qual', StringType(), True),
        StructField('lab_ord_ordg_prov_mdcr_speclty_cd', StringType(), True),
        StructField('lab_ord_ordg_prov_alt_speclty_id', StringType(), True),
        StructField('lab_ord_ordg_prov_alt_speclty_id_qual', StringType(), True),
        StructField('lab_ord_ordg_prov_fclty_nm', StringType(), True),
        StructField('lab_ord_ordg_prov_frst_nm', StringType(), True),
        StructField('lab_ord_ordg_prov_last_nm', StringType(), True),
        StructField('lab_ord_ordg_prov_addr_1_txt', StringType(), True),
        StructField('lab_ord_ordg_prov_addr_2_txt', StringType(), True),
        StructField('lab_ord_ordg_prov_state_cd', StringType(), True),
        StructField('lab_ord_ordg_prov_zip_cd', StringType(), True),
        StructField('lab_ord_loinc_cd', StringType(), True),
        StructField('lab_ord_snomed_cd', StringType(), True),
        StructField('lab_ord_alt_cd', StringType(), True),
        StructField('lab_ord_alt_cd_qual', StringType(), True),
        StructField('lab_ord_test_nm', StringType(), True),
        StructField('lab_ord_panel_nm', StringType(), True),
        StructField('lab_ord_diag_cd', StringType(), True),
        StructField('lab_ord_diag_cd_qual', StringType(), True),
        StructField('data_captr_dt', DateType(), True),
        StructField('rec_stat_cd', StringType(), True),
        StructField('prmy_src_tbl_nm', StringType(), True)
])

schema_v6 = StructType([
    StructField('row_id', LongType(), True),
    StructField('hv_lab_ord_id', StringType(), True),
    StructField('crt_dt', DateType(), True),
    StructField('mdl_vrsn_num', StringType(), True),
    StructField('data_set_nm', StringType(), True),
    StructField('src_vrsn_id', StringType(), True),
    StructField('hvm_vdr_id', IntegerType(), True),
    StructField('hvm_vdr_feed_id', IntegerType(), True),
    StructField('vdr_org_id', StringType(), True),
    StructField('vdr_lab_ord_id', StringType(), True),
    StructField('vdr_lab_ord_id_qual', StringType(), True),
    StructField('vdr_alt_lab_ord_id', StringType(), True),
    StructField('vdr_alt_lab_ord_id_qual', StringType(), True),
    StructField('hvid', StringType(), True),
    StructField('ptnt_birth_yr', IntegerType(), True),
    StructField('ptnt_age_num', StringType(), True),
    StructField('ptnt_lvg_flg', StringType(), True),
    StructField('ptnt_dth_dt', DateType(), True),
    StructField('ptnt_gender_cd', StringType(), True),
    StructField('ptnt_state_cd', StringType(), True),
    StructField('ptnt_zip3_cd', StringType(), True),
    StructField('hv_enc_id', StringType(), True),
    StructField('enc_dt', DateType(), True),
    StructField('lab_ord_dt', DateType(), True),
    StructField('lab_ord_test_schedd_dt', DateType(), True),
    StructField('lab_ord_smpl_collctn_dt', DateType(), True),
    StructField('lab_ord_ordg_prov_npi', StringType(), True),
    StructField('lab_ord_ordg_prov_vdr_id', StringType(), True),
    StructField('lab_ord_ordg_prov_vdr_id_qual', StringType(), True),
    StructField('lab_ord_ordg_prov_alt_id', StringType(), True),
    StructField('lab_ord_ordg_prov_alt_id_qual', StringType(), True),
    StructField('lab_ord_ordg_prov_tax_id', StringType(), True),
    StructField('lab_ord_ordg_prov_dea_id', StringType(), True),
    StructField('lab_ord_ordg_prov_state_lic_id', StringType(), True),
    StructField('lab_ord_ordg_prov_comrcl_id', StringType(), True),
    StructField('lab_ord_ordg_prov_upin', StringType(), True),
    StructField('lab_ord_ordg_prov_ssn', StringType(), True),
    StructField('lab_ord_ordg_prov_nucc_taxnmy_cd', StringType(), True),
    StructField('lab_ord_ordg_prov_alt_taxnmy_id', StringType(), True),
    StructField('lab_ord_ordg_prov_alt_taxnmy_id_qual', StringType(), True),
    StructField('lab_ord_ordg_prov_mdcr_speclty_cd', StringType(), True),
    StructField('lab_ord_ordg_prov_alt_speclty_id', StringType(), True),
    StructField('lab_ord_ordg_prov_alt_speclty_id_qual', StringType(), True),
    StructField('lab_ord_ordg_prov_fclty_nm', StringType(), True),
    StructField('lab_ord_ordg_prov_frst_nm', StringType(), True),
    StructField('lab_ord_ordg_prov_last_nm', StringType(), True),
    StructField('lab_ord_ordg_prov_addr_1_txt', StringType(), True),
    StructField('lab_ord_ordg_prov_addr_2_txt', StringType(), True),
    StructField('lab_ord_ordg_prov_state_cd', StringType(), True),
    StructField('lab_ord_ordg_prov_zip_cd', StringType(), True),
    StructField('lab_ord_loinc_cd', StringType(), True),
    StructField('lab_ord_snomed_cd', StringType(), True),
    StructField('lab_ord_alt_cd', StringType(), True),
    StructField('lab_ord_alt_cd_qual', StringType(), True),
    StructField('lab_ord_test_nm', StringType(), True),
    StructField('lab_ord_panel_nm', StringType(), True),
    StructField('lab_ord_diag_cd', StringType(), True),
    StructField('lab_ord_diag_cd_qual', StringType(), True),
    StructField('lab_ord_stat_cd', StringType(), True),
    StructField('lab_ord_stat_cd_qual', StringType(), True),
    StructField('data_src_cd', StringType(), True),
    StructField('data_captr_dt', DateType(), True),
    StructField('rec_stat_cd', StringType(), True),
    StructField('prmy_src_tbl_nm', StringType(), True)
])

data_type = DataType.EMR
schemas = {
    'schema_v3': Schema(name='schema_v3',
                        schema_structure=schema_v3,
                        distribution_key='row_id',
                        data_type=data_type,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=DataType(data_type).value + '/2017-08-23/lab_order'
                        ),
    'schema_v6': Schema(name='schema_v6',
                        schema_structure=schema_v6,
                        distribution_key='row_id',
                        data_type=data_type,
                        provider_partition_column='part_hvm_vdr_feed_id',
                        date_partition_column='part_mth',
                        output_directory=DataType(data_type).value + '/2017-08-23/lab_order'
                        )
}
