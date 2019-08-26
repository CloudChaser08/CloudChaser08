"""
    data models
"""

from .models import TableMetadata, Column


_DESCRIBE_FORMATTED_TABLE_SQL = 'describe formatted dw.hvm_{datatype}'


def get_table_metadata(sql_context, datatype):
    """ Gets metadata about the table associated with the provided datatype """

    columns = []
    metadata = {}

    all_columns_collected = False
    sql = _DESCRIBE_FORMATTED_TABLE_SQL.format(datatype=datatype)
    for idx, row in enumerate(sql_context.sql(sql).collect()):

        # The describe formatted query first returns column data, then returns
        # an completely empty row ('', '', ''), followed by metadata rows.
        # We use the empty row to flip the `all_columns_collected` flag to True
        if not row.col_name:
            all_columns_collected = True

        # If all the table's columns have been collected, parse the remainder
        # as metadata rows
        if all_columns_collected:
            # Metadata rows take the form of:
            # (<Meta field name>, <value>, <empty>)
            field_name, value, _ = row
            metadata[field_name] = value

        else:

            # Custom column-level metadata is stored in the `comment` field,
            # as key value pairs in the format:
            #       "key1=value1 | key2=value2 | key3=value3"
            # So we parse this and store in a dictionary
            fields = dict(
                item.strip().split('=', 1) for item in row.comment.split('|')
            )

            # Only include columns in the standard data model
            include_field = _convert_yes_no_bool(
                fields['included_in_customer_std_data_model']
            )
            if include_field:
                columns.append(
                    Column(
                        name=row.col_name,
                        field_id=str(idx + 1),
                        sequence=str(idx),
                        datatype=row.data_type,
                        top_values=_convert_yes_no_bool(fields['top_values']),
                        description=fields['description'],
                        # TODO: remove the default value of Baseline once this
                        # field gets populated in the warehouse
                        category=fields.get('category', 'Baseline')
                    )
                )

    return TableMetadata(
        name=metadata['Table'],
        description=metadata['Comment'],
        columns=columns,
        # TODO: pull supplemental metadata from warehouse
        is_supplemental=False
    )


def _convert_yes_no_bool(val):
    if val.lower() == 'yes':
        return True
    if val.lower() == 'no':
        return False
    raise ValueError(
        'Received the value {} for a boolean field. Expected "Yes" or "No"'
        .format(val)
    )
