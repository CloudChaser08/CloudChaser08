import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

encounter_transformer = {
    'ptnt_birth_yr': {
        'func': post_norm_cleanup.cap_year_of_birth,
        'args': ['ptnt_age_num', 'enc_start_dt', 'ptnt_birth_yr']
    }
}

whitelists = [
    {
        'column_name': 'enc_typ_nm',
        'domain_name': 'emr_enc.enc_typ_nm'
    },
    {
        'column_name': 'enc_desc',
        'domain_name': 'emr_enc.enc_desc'
    }
]

def filter(sqlc, update_whitelists=lambda x: x):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, encounter_transformer)
        )
    return out
