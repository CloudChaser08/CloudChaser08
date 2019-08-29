"""
    Data layout package
"""
import json
import boto3

from spark.stats.data_layout.sql_writer import create_runnable_sql_file

from ..models.layout import (
    LayoutDataTable,
    LayoutField,
    Layout
)

def generate_data_layout_version_sql(stats, version_name):
    """
        Given a single DataFeed's config and generated stats, create a runnable SQL file that
        inserts a complete data_layout version into a Marketplace DB for that feed.

        :param stats: Stats run result
        :type stats: spark.stats.models.results.ProviderStatsResult

        :param version_name: The name of the version that will get written to
                             marketplace
        :type version_name: str
    """
    datafeed_id = stats.config.datafeed_id

    # Get the top-level model LayoutField values
    fields = list(get_fields(stats.results, stats.config, datafeed_id))

    # Get the LayoutField objects for all sub models
    if stats.config.models:
        for model in stats.config.models:
            model_results = stats.model_results[model.datatype]
            fields.extend(get_fields(model_results, model, datafeed_id))

    layout = Layout(fields=fields, stats=stats)
    # Create the runnable SQL file for adding this new version of data_layout
    create_runnable_sql_file(datafeed_id, layout, version_name)


def get_fields(results, config, datafeed_id):
    """ Gets all layout fields for a single data model, given the model's
        config, the stats results and a datafeed ID

        :param results: the stats results for this model
        :type results: spark.stats.models.StatsResult

        :param config: a config object for this model
        :type config: Union[spark.stats.models.Provider, spark.stats.models.ProviderModel]

        :param datafeed_id: The data feed's unique ID
        :type datafeed_id: str

        :return: Generator of spark.stats.models.layout.LayoutField objects
    """
    # Only relevant if the config has a table property
    if config.table:
        datatable = LayoutDataTable(
            id=config.table.name,
            name=config.table.name,
            description=config.table.description,
            sequence='1',
        )
        suppl_type = config.table.name if config.table.is_supplemental else None

        for column in config.table.columns:
            fill_rate = get_fill_rate_for_column(results, column.name)
            top_values = get_top_values_for_column(results, column.name)

            field = LayoutField(
                id=column.field_id,
                name=column.name,
                description=column.description,
                category=column.category,
                data_feed=datafeed_id,
                sequence=column.sequence,
                datatable=datatable,
                field_type_name=column.datatype,
                supplemental_type_name=suppl_type,
                fill=fill_rate,
                top_values=top_values
            )
            yield field


def get_fill_rate_for_column(results, col_name):
    """ Gets the fill rate for a specific column, given the stats results
        for the relevant model and the column name
    """
    for fill_rate_result in results.fill_rate:
        if fill_rate_result.field == col_name:
            return fill_rate_result.fill
    return None


def get_top_values_for_column(results, col_name):
    """ Gets the top values for a specific column, given the stats results
        for the relevant model and the column name
    """
    return ', '.join([
        _format_top_value(tv_result)
        for tv_result in results.top_values
        if tv_result.field == col_name
    ]) or None


def _format_top_value(top_value_result):
    return '{value} ({total_count}:{percentage})'.format(
        value=top_value_result.value.encode('utf-8'),
        total_count=top_value_result.count,
        percentage=top_value_result.percentage
    )


def write_summary_file_to_s3(stats, version):
    """
    Upload summary json file to s3
    """
    summary_json = {
        "feeds": [{
            "provider_config": stats.config.to_dict(),
            "stats": stats.results,
            "model_stats": getattr(stats, 'model_results', {}),
            "version": version
            }]
    }
    datafeed_id = stats.config.datafeed_id
    s3_output_dir = 's3://healthveritydev/marketplace_stats/{}/{}/'.format(datafeed_id, version)
    filename = 'stats_summary_{}.json'.format(datafeed_id)
    output_dir = 'output/{}/{}/'.format(datafeed_id, version)

    try:
        os.makedirs(output_dir)
    except:
        pass

    with open(output_dir + filename, 'w+') as summary:
        summary.write(json.dumps(summary_json))
    boto3.client('s3').upload_file(
        output_dir + filename,
        s3_output_dir.split('/')[2],
        '/'.join(s3_output_dir.split('/')[3:]) + filename
    )
