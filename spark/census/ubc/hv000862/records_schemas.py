'''
HV000862 UBC record table definitions
'''
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'transactions': SourceTable(
            'csv',
            separator='|',
            columns=[
                    'guid',
                    'urn',
                    'first',
                    'last',
                    'gender',
                    'ssn',
                    'dob',
                    'address1',
                    'address2',
                    'city',
                    'state',
                    'country',
                    'zip',
                    'email',
                    'tel',
                    'policyGroupNum',
                    'policyMemderId',
                    'policyGroupName',
                    'ubcapp',
                    'ubcdb',
                    'ubcprogram',
                    'hvjoinkey'
                ]
            )
    }
