from contextlib import closing

import psycopg2
from psycopg2.extras import DictCursor


def _layout_sql(datafeed_id):
    """ Create the SQL statement for extracting the base data_layout from DB. """
    get_config_sql = """
        SELECT
            f.physical_name as name,
            f.id as field_id,
            f.category as category,
            f.description as description,
            f.sequence as sequence,
            t.id as table_id,
            t.description as table_desc,
            t.name as table_name,
            t.sequence as table_seq,
            ft.name as field_type_name,
            m.is_supplemental as is_supplemental,
            m.name as datamodel_name
        FROM marketplace_datafield f
            JOIN marketplace_fieldtype ft on ft.id = f.field_type_id
            JOIN marketplace_datatable t on t.id = f.datatable_id
            JOIN marketplace_datamodel m on m.id = t.datamodel_id
            JOIN marketplace_datafeed_datamodels dm on dm.datamodel_id = m.id
        WHERE dm.datafeed_id = {datafeed_id};
    """.format(datafeed_id=datafeed_id)

    return get_config_sql


def _run_sql(query):
    """
    Run a SQL statement and return the output.
    Uses DictCursor to allow for easy JSON-ification of the output.
    (Could be a common Util)
    """

    """
    MUSTDO
    conn = psycopg2.connect(
        host='pg-dev.healthverity.com',
        database='config',
        user='hvreadonly',
        password=os.environ.get('PGPASSWORD')
    )
    """

    # TODO: Make configurable
    conn = psycopg2.connect(
        host='localhost',
        database='config',
        user='hvsuperuser',
        password='hvsuperuser',
        port=32768
    )
    with closing(conn.cursor(cursor_factory=DictCursor)) as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    return results


def _normalize_field_layout(layout_field):
    """
    Given a single field_dict in a data_layout, manipulate the key-values
    to match proper format of a data_layout in Marketplace.
    """
    field_dict = dict(layout_field)
    field_dict.update({
        'fill_rate': None,
        'top_values': None,
        'datatable': {
            'id': field_dict.pop('table_id', ''),
            'name': field_dict.pop('table_name', ''),
            'description': field_dict.pop('table_desc', ''),
            'sequence': field_dict.pop('table_seq', ''),
        }
    })

    # If the field is supplemental, supplemental_type_name is DataModel.name
    datamodel_name = field_dict.pop('datamodel_name', '')
    if field_dict['is_supplemental'] == 't':
        field_dict['supplemental_type_name'] = datamodel_name
    else:
        field_dict['supplemental_type_name'] = None

    return field_dict


def get_base_data_layout(feed_id):
    """
    Query for the data_layout associated to a given DataFeed,
    and manipulate the output to reflect a proper Marketplace format.
    """
    # Get data_layout
    layout_sql = _layout_sql(feed_id)
    layout_result = _run_sql(layout_sql)

    # Properly format the layout
    layout_dict = [
        _normalize_field_layout(layout_field)
        for layout_field in layout_result
    ]
    return layout_dict
