from spark.stats.data_layout.layout_populator import populate_stat_values
from spark.stats.data_layout.layout_reader import get_base_data_layout
from spark.stats.data_layout.sql_writer import create_runnable_sql_file


def generate_data_layout_version_sql(provider_config, stats, version_name):
    """
    Given a single DataFeed's config and generated stats, create a runnable SQL file that
    inserts a complete data_layout version into a Marketplace DB for that feed.
    """
    datafeed_id = provider_config['datafeed_id']

    # 1) Get the base data_layout, without top_values and fill_rate populated
    data_layout = get_base_data_layout(datafeed_id)

    # 2) Populate top_values and fill_rate for each field in data_layout
    populate_stat_values(provider_config, data_layout, stats)

    # 3) Create the runnable SQL file for adding this new version of data_layout
    create_runnable_sql_file(datafeed_id, data_layout, version_name)
