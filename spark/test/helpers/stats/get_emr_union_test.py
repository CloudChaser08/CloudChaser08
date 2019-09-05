"""
    Tests for stats utils
"""

import re
from mock import Mock

from pyspark.sql.types import Row

from spark.stats.models import ProviderModel
from spark.helpers.stats.utils import get_emr_union


def test_emr_union(spark):
    mock_sql_context = Mock()
    data_row = Row('hvid', 'hv_enc_id', 'coalesced_emr_date')
    df1 = spark['spark'].sparkContext.parallelize([
        data_row('a', '123', '1975-12-11',),
        data_row('b', '234', '2017-11-08',),
    ]).toDF()
    df2 = spark['spark'].sparkContext.parallelize([
        data_row('c', '345', '1993-01-01',),
        data_row('d', '456', '1992-01-01',),
    ]).toDF()
    mock_sql_context.sql.side_effect = [df1, df2]

    models = [
        ProviderModel(
            datatype='emr_clin_obsn',
            date_fields=['date1', 'date2']
        ),
        ProviderModel(
            datatype='emr_medctn',
        )
    ]
    assert get_emr_union(mock_sql_context, models, '5').count() == 4
    assert len(mock_sql_context.sql.call_args_list) == 2
    sql1, sql2 = [
        # consolidate white spaces
        re.sub(r'\s+', ' ', c[0][0]).strip()
        for c in mock_sql_context.sql.call_args_list
    ]

    assert sql1 == (
        "SELECT hvid, hv_enc_id, coalesce(date1,date2) as coalesced_emr_date "
        "FROM dw.hvm_emr_clin_obsn WHERE part_hvm_vdr_feed_id='5'"
    )
    assert sql2 == (
        "SELECT hvid, hv_enc_id, coalesce(hv_medctn_dt) as coalesced_emr_date "
        "FROM dw.hvm_emr_medctn WHERE part_hvm_vdr_feed_id='5'"
    )
