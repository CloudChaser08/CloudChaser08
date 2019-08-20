"""
Uses stats output from stats_runner.py to populate a data_layout dictionary.
For an example of stats output from stats_runner.py, see example_stats.txt in this directory.
"""
from collections import defaultdict

from spark.stats.config.reader.config_reader import EMR_DATATYPE_NAME_MAP


def _get_fill_rate_for_field(field_name, filtered_stats):
    """
    Given a field name and a list of fill_rates filtered so that field_name is a unique key,
    return the fill_rate for this field (should only be one).
    """
    fill_rate_stats = filtered_stats.get('fill_rate', [])

    for current_fill_rate in fill_rate_stats:
        stat_field_name = current_fill_rate['field']
        if field_name == stat_field_name:
            return current_fill_rate['fill']

    return None


def _get_top_values_for_field(field_name, filtered_stats):
    """
    Given a field name and a list of top_values filtered so that field_name is a searchable key,
    format each top_value relevant to field_name and concatenate them with commas.
    """
    top_values_stats = filtered_stats.get('top_values', [])

    relevant_top_values = []
    for current_top_value in top_values_stats:
        stat_field_name = current_top_value['column']
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
    """
    Populate stats in the case where there are more than one DataTable present.
    MUTATES: The input 'data_layout'
    """

    # group data_layout by table
    layout_fields_by_table = defaultdict(list)
    for field_dict in data_layout:
        field_table_name = field_dict['datatable']['name']
        layout_fields_by_table[field_table_name].append(field_dict)

    # DataTables are called models in provider_config
    for table_config in provider_config.models:
        table_name_in_stats = table_config.datatype
        table_stats = all_stats.get(table_name_in_stats, {})

        # Convert table_name to the same format that is in the data_layout
        table_name_in_layout = EMR_DATATYPE_NAME_MAP.get(table_name_in_stats)

        layout_slice_for_table = (
            layout_fields_by_table.get(table_name_in_layout, [])
        )
        _populate_stats_single_table(layout_slice_for_table, table_stats)


def _populate_stats_single_table(data_layout, stats):
    """
    Populate stats in the case where there is only one DataTable present.
    MUTATES: The input 'data_layout'
    """
    for field_dict in data_layout:
        field_name = field_dict['name']

        field_fill_rate = _get_fill_rate_for_field(field_name, stats)
        field_top_values = _get_top_values_for_field(field_name, stats)
        field_dict.update(
            {'fill_rate': field_fill_rate, 'top_values': field_top_values}
        )


def populate_stat_values(provider_config, data_layout, stats):
    """
    Given a data_layout and stats returned by stats_runner.py, populate the fields within
    data_layout with the stats relevant to them.
    MUTATES: The input 'data_layout'
    """
    is_multi_table = (provider_config.datatype == 'emr')

    if is_multi_table:
        _populate_stats_multi_table(provider_config, data_layout, stats)

    else:
        _populate_stats_single_table(data_layout, stats)
