from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
from pyspark.sql.functions import md5

medication_transformer = Transformer(
    medctn_diag_cd=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['medctn_diag_cd', 'medctn_diag_cd_qual', 'enc_dt'])
    ],
    medctn_ndc=[
        TransformFunction(post_norm_cleanup.clean_up_ndc_code, ['medctn_ndc'])
    ],
    rx_num=[
        TransformFunction(md5, ['rx_num'], True)
    ]
)

whitelists = [
    {
        'column_name': 'medctn_admin_sig_cd',
        'domain_name': 'emr_medctn.medctn_admin_sig_cd'
    },
    {
        'column_name': 'medctn_admin_sig_txt',
        'domain_name': 'emr_medctn.medctn_admin_sig_txt'
    },
    {
        'column_name': 'medctn_admin_form_nm',
        'domain_name': 'emr_medctn.medctn_admin_form_nm'
    },
    {
        'column_name': 'medctn_strth_txt',
        'domain_name': 'emr_medctn.medctn_strth_txt'
    },
    {
        'column_name': 'medctn_strth_txt_qual',
        'domain_name': 'emr_medctn.medctn_strth_txt_qual'
    },
    {
        'column_name': 'medctn_admin_rte_txt',
        'domain_name': 'emr_medctn.medctn_admin_rte_txt'
    }
]


def filter(sqlc, update_whitelists=lambda x: x, additional_transformer=None):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, medication_transformer.overwrite(additional_transformer))
        )
    return out
