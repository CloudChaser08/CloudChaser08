# properties for unload tables
# use 'format' to fill in the output path
unload_properties_template = 'PARTITIONED BY (part_provider string, part_best_date string) '  \
  + 'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' '   \
  + 'WITH SERDEPROPERTIES ( '                                            \
  + '\'separatorChar\' = \'|\' '                                         \
  + ') '                                                                 \
  + 'LOCATION \'{}\''
