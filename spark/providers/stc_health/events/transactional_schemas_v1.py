from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'row_level_identifier',
            'first_name',
            'last_name',
            'date_of_birth',
            'gender',
            'race',
            'ethnicity',
            'address_line_1',
            'address_line_2',
            'city',
            'state',
            'zip_code',
            'country',
            'home_phone',
            'mobile_phone',
            'email',
            'member_id',
            'group_id',
            'group_name',
            'pcn',
            'ssn',
            'vaccine',
            'date_administered',
            'dose_number',
            'series_complete_after_this_dose',
            'manufacturer',
            'lot_number',
            'expiration_date',
            'anatomical_site',
            'anatomical_route',
            'dose_amount',
            'administered_by',
            'tenant',
            'store_address',
            'store_number',
            'hvjoinkey'
        ]
    )
}
