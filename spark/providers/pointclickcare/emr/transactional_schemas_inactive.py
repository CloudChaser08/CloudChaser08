from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'dimcensusfromtotype': SourceTable(
        'csv',
        separator='|',
        columns=[
            'censusfromtotype',
            'censusfromtotypeid'
        ]
    ),
    'dimclient': SourceTable(
        'csv',
        separator='|',
        columns=[
            'admissiondate',
            'clientid',
            'clientidnumber',
            'dischargedate',
            'facilityid',
            'originaladmissiondate',
            'outpatient',
            'primaryphysicianid',
            'residentid'
        ]
    ),
    'dimcensussction': SourceTable(
        'csv',
        separator='|',
        columns=[
            'censusactionid',
            'censusactionlong',
            'censusactionshort',
            'censusactiontype'
        ]
    ),
    'dimcensusstatus': SourceTable(
        'csv',
        separator='|',
        columns=[
            'censusstatusid',
            'censusstatuslong',
            'censusstatusshort',
            'censusstatustype'
        ]
    ),
    'dimdischargestatus': SourceTable(
        'csv',
        separator='|',
        columns=[
            'dischargestatus',
            'dischargestatusid',
            'isdischargeind'
        ]
    ),
    'dimfacility': SourceTable(
        'csv',
        separator='|',
        columns=[
            'address1',
            'address2',
            'ccn',
            'city',
            'countryid',
            'facilityid',
            'facilityname',
            'facilitynpi',
            'facilitystartdate',
            'facilitytype',
            'healthtype',
            'isexternalind',
            'isinactiveind',
            'postalzipcode',
            'state'
        ]
    ),
    'dimpayer': SourceTable(
        'csv',
        separator='|',
        columns=[
            'payerid',
            'payerlabel',
            'payertype',
            'payertypecode'
        ]
    ),
    'dimrouteofadmin': SourceTable(
        'csv',
        separator='|',
        columns=[
            'abbreviation',
            'medispanrouteofadmin',
            'prefix',
            'requirelocation',
            'routeofadmin',
            'routeofadminid'
        ]
    ),
    'dimstaff': SourceTable(
        'csv',
        separator='|',
        columns=[
            'npi',
            'personname',
            'professiontype',
            'staffid'
        ]
    ),
    'dimtransferoutcome': SourceTable(
        'csv',
        separator='|',
        columns=[
            'transferoutcome',
            'transferoutcomeid'
        ]
    ),
    'dimtransferreason': SourceTable(
        'csv',
        separator='|',
        columns=[
            'transferreason',
            'transferreasonid'
        ]
    )
}
