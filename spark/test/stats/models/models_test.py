"""
    Models test
"""

from spark.stats.models import Provider, ProviderModel

def test_merge():
    """ Tests the Provider.merge_provider_model method """
    prov = Provider(
        name='name',
        datafeed_id='5',
        earliest_date='2018-01-1',
        datatype='medicalclaims',
        fill_rate=True
    )

    res = prov.merge_provider_model(
        ProviderModel(
            datatype='emr_proc',
            fill_rate=False
        )
    )
    assert res.datafeed_id == '5'
    assert res.datatype == 'emr_proc'
    assert res.fill_rate is False
