"""record schema ubc avexis"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'records': SourceTable(
        'csv',
        separator='|',
        columns=[
            'uniquepatientnumber',
            'uniquerecordnumber',
            'firstname',
            'lastname',
            'gender',
            'socialsecuritynumber',
            'dateofbirth',
            'address1_addressline1',
            'address1_addressline2',
            'city',
            'state',
            'country',
            'zipcode',
            'emailaddress',
            'telephone',
            'policygroupnumber',
            'policymembebrid',
            'policygroupname',
            'ubcapp',
            'ubcdb',
            'ubcprogram',
            'truncated_dob',
            'dateofservice',
            'firstservicedate',
            'hv_join_key'
        ]
    )
}
