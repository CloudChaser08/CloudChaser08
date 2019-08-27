
from mock import patch

from spark.stats.models import (
    Provider, ProviderModel, TableMetadata, Column
)
from spark.stats.models.results import (
    StatsResult, ProviderStatsResult, FillRateResult, TopValuesResult
)
from spark.stats.models.layout import (
    Layout, LayoutField, LayoutDataTable
)
from spark.stats.data_layout import generate_data_layout_version_sql


@patch('spark.stats.data_layout.create_runnable_sql_file', autospec=True)
def test_single_table(create_sql):
    """ Test data layout creation for provider with one table """
    stats = ProviderStatsResult(
        results=StatsResult(
            fill_rate=[
                FillRateResult(
                    field='field_1',
                    fill=1.0
                ),
                FillRateResult(
                    field='field_2',
                    fill=2.0
                ),
            ],
            top_values=[
                TopValuesResult(
                    field='field_1',
                    value='val1',
                    count=1000,
                    percentage=0.01
                ),
                TopValuesResult(
                    field='field_1',
                    value='val2',
                    count=60,
                    percentage=5.05
                ),
            ]
        ),
        model_results={},
        config=Provider(
            name="test",
            datafeed_id="1",
            datatype="pharmacyclaims",
            earliest_date="2018-01-01",
            table=TableMetadata(
                name='hvm_pharmacyclaims',
                description='Pharmacy Claims Table',
                columns=[
                    Column(
                        name='field_1',
                        field_id='1',
                        sequence='0',
                        datatype='bigint',
                        description='Field 1',
                        category='Baseline',
                        top_values=True,
                    ),
                    Column(
                        name='field_2',
                        field_id='2',
                        sequence='1',
                        datatype='bigint',
                        description='Field 2',
                        category='Baseline',
                        top_values=True,
                    )
                ]
            )
        )
    )

    generate_data_layout_version_sql(stats, 'v1')

    create_sql.assert_called_with(
        '1',
        Layout(stats=stats, fields=[
            LayoutField(
                id='1',
                name='field_1',
                description='Field 1',
                category='Baseline',
                data_feed='1',
                sequence='0',
                datatable=LayoutDataTable(
                    id='hvm_pharmacyclaims',
                    name='hvm_pharmacyclaims',
                    description='Pharmacy Claims Table',
                    sequence='1'
                ),
                field_type_name='bigint',
                supplemental_type_name=None,
                fill=1.0,
                top_values='val1 (1000:0.01), val2 (60:5.05)',
            ),
            LayoutField(
                id='2',
                name='field_2',
                description='Field 2',
                category='Baseline',
                data_feed='1',
                sequence='1',
                datatable=LayoutDataTable(
                    id='hvm_pharmacyclaims',
                    name='hvm_pharmacyclaims',
                    description='Pharmacy Claims Table',
                    sequence='1'
                ),
                field_type_name='bigint',
                supplemental_type_name=None,
                fill=2.0,
                top_values=None,
            )
        ]),
        'v1'
    )

@patch('spark.stats.data_layout.create_runnable_sql_file', autospec=True)
def test_multi_table(create_sql):
    """ Test data layout creation for provider with multiple tables """
    stats = ProviderStatsResult(
        model_results={
            'emr_clin_obsn': StatsResult(
                fill_rate=[
                    FillRateResult(
                        field='field_1',
                        fill=1.0
                    ),
                ],
                top_values=[
                    TopValuesResult(
                        field='field_1',
                        value='val1',
                        count=1000,
                        percentage=0.01
                    ),
                    TopValuesResult(
                        field='field_1',
                        value='val2',
                        count=60,
                        percentage=5.05
                    ),
                ]
            ),
            'emr_medctn': StatsResult(
                fill_rate=[
                    FillRateResult(
                        field='field_1',
                        fill=3.0
                    ),
                    FillRateResult(
                        field='field_2',
                        fill=2.0
                    ),
                ],
                top_values=[
                    TopValuesResult(
                        field='field_1',
                        value='val1',
                        count=1000,
                        percentage=0.12
                    ),
                    TopValuesResult(
                        field='field_2',
                        value='val1',
                        count=40,
                        percentage=0.02
                    ),
                    TopValuesResult(
                        field='field_2',
                        value='val2',
                        count=30,
                        percentage=4.02
                    ),
                ]
            ),
        },
        results=StatsResult(),
        config=Provider(
            name="test",
            datafeed_id="1",
            datatype="emr",
            earliest_date="2018-01-01",
            models=[
                ProviderModel(
                    datatype='emr_clin_obsn',
                    table=TableMetadata(
                        name='hvm_emr_clin_obsn',
                        description='EMR Clinical Observation',
                        columns=[
                            Column(
                                name='field_1',
                                field_id='1',
                                sequence='0',
                                datatype='bigint',
                                description='Field 1',
                                category='Baseline',
                                top_values=True,
                            ),
                            Column(
                                name='field_2',
                                field_id='2',
                                sequence='1',
                                datatype='bigint',
                                description='Field 2',
                                category='Baseline',
                                top_values=True,
                            )
                        ]
                    )
                ),
                ProviderModel(
                    datatype='emr_medctn',
                    table=TableMetadata(
                        name='hvm_emr_medctn',
                        description='EMR Medication',
                        columns=[
                            Column(
                                name='field_1',
                                field_id='1',
                                sequence='0',
                                datatype='bigint',
                                description='Field 1',
                                category='Baseline',
                                top_values=True,
                            ),
                            Column(
                                name='field_2',
                                field_id='2',
                                sequence='1',
                                datatype='bigint',
                                description='Field 2',
                                category='Baseline',
                                top_values=True,
                            )
                        ]
                    )
                )
            ]
        )
    )

    generate_data_layout_version_sql(stats, 'v1')

    create_sql.assert_called_with(
        '1',
        Layout(stats=stats, fields=[
            LayoutField(
                id='1',
                name='field_1',
                description='Field 1',
                category='Baseline',
                data_feed='1',
                sequence='0',
                datatable=LayoutDataTable(
                    id='hvm_emr_clin_obsn',
                    name='hvm_emr_clin_obsn',
                    description='EMR Clinical Observation',
                    sequence='1'
                ),
                field_type_name='bigint',
                supplemental_type_name=None,
                fill=1.0,
                top_values='val1 (1000:0.01), val2 (60:5.05)',
            ),
            LayoutField(
                id='2',
                name='field_2',
                description='Field 2',
                category='Baseline',
                data_feed='1',
                sequence='1',
                datatable=LayoutDataTable(
                    id='hvm_emr_clin_obsn',
                    name='hvm_emr_clin_obsn',
                    description='EMR Clinical Observation',
                    sequence='1'
                ),
                field_type_name='bigint',
                supplemental_type_name=None,
                fill=None,
                top_values=None,
            ),
            LayoutField(
                id='1',
                name='field_1',
                description='Field 1',
                category='Baseline',
                data_feed='1',
                sequence='0',
                datatable=LayoutDataTable(
                    id='hvm_emr_medctn',
                    name='hvm_emr_medctn',
                    description='EMR Medication',
                    sequence='1'
                ),
                field_type_name='bigint',
                supplemental_type_name=None,
                fill=3.0,
                top_values='val1 (1000:0.12)',
            ),
            LayoutField(
                id='2',
                name='field_2',
                description='Field 2',
                category='Baseline',
                data_feed='1',
                sequence='1',
                datatable=LayoutDataTable(
                    id='hvm_emr_medctn',
                    name='hvm_emr_medctn',
                    description='EMR Medication',
                    sequence='1'
                ),
                field_type_name='bigint',
                supplemental_type_name=None,
                fill=2.0,
                top_values='val1 (40:0.02), val2 (30:4.02)',
            )
        ]),
        'v1'
    )
