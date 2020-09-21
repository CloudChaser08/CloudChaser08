from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'enr': SourceTable(
        'csv',
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