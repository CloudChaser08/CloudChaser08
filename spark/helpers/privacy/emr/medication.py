import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
from pyspark.sql.functions import md5

medication_transformer = {
    'medctn_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['medctn_diag_cd', 'medctn_diag_cd_qual', 'medctn_admin_dt']
    },
    'rx_num': {
        'func': md5,
        'args': ['rx_num'],
        'built-in': True
    }
}

whitelists = [
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

def filter(sqlc):
    def out(df):
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whitelists
            ]
        )(
            emr_priv_common.filter(df, medication_transformer)
        )
    return out
