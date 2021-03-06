"""
inovalon pharmacyclaims source schema v4
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'mbr': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        confirm_schema=True,
        columns=[
            'memberuid',
            'birthyear',
            'gendercode',
            'statecode',
            'zip3value',
            'raceethnicitytypecode',
            'createddate'
        ]
    ),
    'prv': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        confirm_schema=True,
        columns=[
            'provideruid',
            'lastname',
            'firstname',
            'middlename',
            'companyname',
            'npi1',
            'npitypecode1',
            'parentorganization1',
            'npi2',
            'npitypecode2',
            'parentorganization2',
            'primarypracticeaddress',
            'secondarypracticeaddress',
            'practicecity',
            'practicestate',
            'practicezip',
            'practicezip4',
            'practicephone',
            'primarybillingaddress',
            'secondarybillingaddress',
            'billingcity',
            'billingstate',
            'billingzip',
            'billingzip4',
            'billingphone',
            'taxonomycode1',
            'taxonomytype1',
            'taxonomyclassification1',
            'taxonomyspecialization1',
            'taxonomycode2',
            'taxonomytype2',
            'taxonomyclassification2',
            'taxonomyspecialization2',
            'createddate'
        ]
    ),
    'psp': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        confirm_schema=True,
        columns=[
            'provideruid',
            'nameprefix',
            'name',
            'namesuffix',
            'address1',
            'address2',
            'city',
            'state',
            'zip',
            'phone',
            'fax',
            'deanumber',
            'npinumber',
            'createddate'
        ]
    ),
    'rxc': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        confirm_schema=True,
        columns=[
            'rxclaimuid',
            'memberuid',
            'provideruid',
            'claimstatuscode',
            'filldate',
            'ndc11code',
            'supplydayscount',
            'dispensedquantity',
            'billedamount',
            'allowedamount',
            'copayamount',
            'paidamount',
            'costamount',
            'prescribingnpi',
            'dispensingnpi',
            'sourcemodifieddate',
            'createddate'
        ]
    ),
    'rxcc': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        confirm_schema=True,
        columns=[
            'rxfilluid',
            'memberuid',
            'filldate',
            'ndc11code',
            'supplydayscount',
            'unitquantity',
            'unadjustedprice'
        ]
    ),
    'rxcw': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='|',
        confirm_schema=True,
        columns=[
            'rxclaimuid',
            'rxfilluid',
            'memberuid',
            'filldate',
            'ndc11code'
        ]
    )
}
