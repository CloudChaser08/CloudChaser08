"""schemaq definition"""
from collections import namedtuple
Schema = namedtuple('Schema', 'name, '
                              'data_type, '
                              'schema_structure, '
                              'provider_partition_column, '
                              'date_partition_column, '
                              'distribution_key, '
                              'output_directory')
Schema.__new__.__defaults__ = (None,
                               None,
                               None,
                               'part_hvm_vdr_feed_id',
                               'part_mth',
                               'record_id',
                               '')

