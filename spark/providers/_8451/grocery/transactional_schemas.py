""" Source table definitions for 84.51 Grocery """
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'meta': SourceTable(
        'csv',
        separator='|',
        columns=[
            "hshd_id",
            "pref_store_state",
            "cont_panel_1yr",
            "cont_panel_2yr",
            "hbc_cont_panel",
            "wic_flag",
            "retail_loyalty",
            "digital_engagement",
            "coup_resp_behavior",
            "bsk_coup_proportion_percent",
            "store_brands_eng_seg",
            "health_hml",
            "quality_hml",
            "convenience_hml",
            "price_hml",
            "inspiration_hml",
            "agg_level",
            "agg_level_code",
            "agg_level_desc",
            "agg_total_units",
            "agg_total_visits",
            "agg_total_spend",
            "period_start_dt",
            "period_end_dt",
            "hvjoinkey"
        ]
    )
}
