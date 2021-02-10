from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'enr': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        columns=[
            'memberuid',
            'effectivedate',
            'terminationdate',
            'payergroupcode',
            'payertypecode',
            'productcode',
            'medicalindicator',
            'rxindicator',
            'sourceid',
            'groupplantypecode',
            'macontracttypecode',
            'acaindicator',
            'acaissuerstatecode',
            'acagrandfatheredindicator',
            'acaonexchangeindicator',
            'acametallevel',
            'acaactuarialvalue',
            'createddate'
        ]
    ),
    'mbr': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        columns=[
            'memberuid',
            'birthyear',
            'gendercode',
            'statecode',
            'zip3value',
            'raceethnicitytypecode',
            'createddate'
        ]
    )
}
