"""
cardinal pms era summaru
"""
import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

# columns to nullify depending on place of service or inst type of bill
columns_to_nullify = [
    'bllg_prov_npi', 'bllg_prov_vdr_id', 'bllg_prov_tax_id',
    'bllg_prov_1_nm', 'bllg_prov_addr_1_txt',
    'bllg_prov_addr_2_txt', 'bllg_prov_city_nm',
    'bllg_prov_state_cd', 'bllg_prov_zip_cd', 'rndrg_prov_npi',
    'rndrg_prov_vdr_id', 'rndrg_prov_tax_id',
    'rndrg_prov_ssn', 'rndrg_prov_state_lic_id', 'rndrg_prov_upin',
    'rndrg_prov_comrcl_id', 'rndrg_prov_1_nm', 'rndrg_prov_2_nm'
]


def filter_due_to_pos_itb(
        column, place_of_service_std_id, inst_type_of_bill_std_id
):
    return post_norm_cleanup.filter_due_to_place_of_service(
        post_norm_cleanup.filter_due_to_inst_type_of_bill(column, inst_type_of_bill_std_id),
        place_of_service_std_id
    )


era_summary_transformer = priv_common.Transformer(
    drg_cd=[
        priv_common.TransformFunction(post_norm_cleanup.nullify_drg_blacklist, ['drg_cd'])
    ],
    pos_cd=[
        priv_common.TransformFunction(post_norm_cleanup.obscure_place_of_service, ['pos_cd'])
    ],
    instnl_typ_of_bll_cd=[
        priv_common.TransformFunction(post_norm_cleanup.obscure_inst_type_of_bill,
                                      ['instnl_typ_of_bll_cd'])
    ]
).append(
    priv_common.Transformer(**dict([
        (c, [
            priv_common.TransformFunction(filter_due_to_pos_itb,
                                          [c, 'pos_cd', 'instnl_typ_of_bll_cd'])
        ]) for c in columns_to_nullify
    ]))
)


def apply_privacy(df, additional_transforms=None):
    return priv_common.filter(df, era_summary_transformer)
