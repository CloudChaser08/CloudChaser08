# properties for unload tables
# use 'format' to fill in the output path
unload_properties_template = 'PARTITIONED BY ({} string, {} string) '  \
  + 'STORED AS PARQUET ' \
  + 'LOCATION \'{}\''

hdfs_staging_dir = '/staging/'
