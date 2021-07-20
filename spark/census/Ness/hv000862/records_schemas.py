"""
HV000862 UBC NESSrecord table definitions
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'transactions': SourceTable(
        'csv',
        separator='|',
        columns=[
            'GUID',
            'UniqueRecordNumebr',
            'First_Name',
            'Last_Name',
            'Gender',
            'SocialSecurityNumber',
            'Date_of_Birth',
            'Address1_Address_Line_1',
            'Address1_Address_Line_2',
            'City',
            'State',
            'Country',
            'Zip_Code',
            'EmailAddress',
            'Telephone',
            'PolicyGroupNumber',
            'PolicyMembebrID',
            'PolicyGroupName',
            'UBCApp',
            'UBCDB',
            'UBCProgram',
            'hvjoinkey'
        ]
    )
}
