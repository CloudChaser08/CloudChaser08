from spark.stats.config.reader.config_reader import EMR_DATATYPE_NAME_MAP


def _get_fill_rate_for_field(field_name, filtered_stats):
    """
    Given a field name and a list of fill_rates filtered so that field_name is a unique key,
    return the fill_rate for this field (should only be one).
    """
    fill_rate_stats = filtered_stats.get('fill_rate', [])

    for current_fill_rate in fill_rate_stats:
        stat_field_name = current_fill_rate.get('field', '')
        if field_name == stat_field_name:
            return current_fill_rate.get('fill')

    return None


def _get_top_values_for_field(field_name, filtered_stats):
    """
    Given a field name and a list of top_values filtered so that field_name is a searchable key,
    format each top_value relevant to field_name and concatenate them with commas.
    """
    top_values_stats = filtered_stats.get('top_values', [])

    relevant_top_values = []
    for current_top_value in top_values_stats:
        stat_field_name = current_top_value.get('column', '')
        if field_name == stat_field_name:
            formatted_top_value = (
                '{value} ({total_count}:{percentage})'.format(
                    value=current_top_value['value'].encode('utf-8'),
                    total_count=current_top_value['count'],
                    percentage=current_top_value['percentage']
                )
            )
            relevant_top_values.append(formatted_top_value)

    if relevant_top_values:
        return ', '.join(relevant_top_values)
    return None


def _populate_stats_multi_table(provider_config, data_layout, all_stats):
    """ Populate stats in the case where there are more than one DataTable present. """

    # DataTables are called models in provider_config
    for table_config in provider_config['models']:
        table_name = table_config['datatype']
        table_stats = all_stats.get(table_name, {})

        # Convert table_name to the same format that is in the data_layout
        layout_table_name = EMR_DATATYPE_NAME_MAP.get(table_name, default=table_name)

        # For each field_dict in the layout, populate values
        # if it belongs to our current table.
        for field_dict in data_layout:
            field_table_name = field_dict.get('datatable', {}).get('name')

            if layout_table_name == field_table_name:
                field_name = field_dict['name']  # if this isn't true we actually should error out

                field_fill_rate = _get_fill_rate_for_field(field_name, table_stats)
                field_top_values = _get_top_values_for_field(field_name, table_stats)
                field_dict.update(
                    {'fill_rate': field_fill_rate, 'top_values': field_top_values}
                )

    return data_layout


def _populate_stats_single_table(data_layout, stats):
    """ Populate stats in the case where there is only one DataTable present. """
    for field_dict in data_layout:
        field_name = field_dict['name']  # if this isn't true we actually should error out.

        field_fill_rate = _get_fill_rate_for_field(field_name, stats)
        field_top_values = _get_top_values_for_field(field_name, stats)
        field_dict.update(
            {'fill_rate': field_fill_rate, 'top_values': field_top_values}
        )

    return data_layout


def populate_stat_values(provider_config, data_layout, stats):
    """
    Given a data_layout and stats returned by stats_runner.py, populate the fields within
    data_layout with the stats relevant to them.
    """
    is_multi_table = (provider_config['datatype'] == 'emr')

    if is_multi_table:
        data_layout = _populate_stats_multi_table(provider_config, data_layout, stats)

    else:
        data_layout = _populate_stats_single_table(data_layout, stats)

    return data_layout


"""
### Examples of stats returned by stats_runner.py ###

    # EMR (multi-table) #
    {
        'emr_datatype_1': {
            'fill_rate': [
                {'field': 'field_1', 'fill': 1.0},
            ],
            'top_values': [
                {
                    'column': 'field_1',
                    'value': '20',
                    'count': '1000',
                    'percentage': 0.02
                },
            ],
            ...other stats
        }
        'emr_datatype_2': {
            'fill_rate': [
                {'field': 'field_1', 'fill': 0.6},
                {'field': 'field_3', 'fill': 0.5}
            ],
            'top_values': [
                {
                    'column': 'field_1',
                    'value': '30',
                    'count': '60',
                    'percentage': 50.00
                },
            ],
            ...other stats
        },
        ...other datatypes
    }

    # NON EMR (single-table) #
    {
        'fill_rate': [
            {'field': 'field_1', 'fill': 1.0},
            {'field': 'field_2', 'fill': 1.0},
            {'field': 'field_3', 'fill': 0.5}
        ],
        'top_values': [
            {
                'column': 'field_1',
                'value': '10',
                'count': '1000',
                'percentage': 0.01
            },
            {
                'column': 'field_2',
                'value': '2',
                'count': '4',
                'percentage': 50.00
            },
        ],
        ...other stats
    }
"""
