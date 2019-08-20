"""
    data models
"""

import attr

from .models import Column

@attr.s(frozen=True)
class _WarehouseColumn(object):
    """ A data structure to represent metadata about a warehouse column """
    name = attr.ib()
    datatype = attr.ib()
    top_values = attr.ib()
    description = attr.ib()
    include_in_model = attr.ib()


def get_columns_for_model(sql_context, datatype):
    """ Gets all relevant Columns for a specified datatype as a map, keyed
        by the name of the column
    """
    dw_columns = (
        _parse_warehouse_column_row(row)
        for row in sql_context.sql(
            'describe table dw.hvm_{}'.format(datatype)
        ).collect()
    )
    return {
        dwc.name: Column(
            name=dwc.name,
            field_id=str(idx + 1),
            sequence=str(idx),
            datatype=dwc.datatype,
            top_values=dwc.top_values,
            description=dwc.description,
        ) for idx, dwc in enumerate(dw_columns)
        # filter out any columns where the "include_in_model" flag is false
        if dwc.include_in_model
    }


def _parse_warehouse_column_row(row):
    """ Parses a spark SQL row of warehouse datamodel column metadata into
        a _WarehouseColumn object, provided that the column indicates that
        it should be included in the customer data model.
    """
    comment_fields = dict(
        item.strip().split('=', 1) for item in row.comment.split('|')
    )

    return _WarehouseColumn(
        name=row.col_name,
        datatype=row.data_type,
        top_values=_convert_yes_no_bool(
            comment_fields['top_values']
        ),
        description=comment_fields['description'],
        include_in_model=_convert_yes_no_bool(
            comment_fields['included_in_customer_std_data_model']
        )
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
