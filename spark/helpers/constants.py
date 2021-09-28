"""constants"""
# properties for unload tables
# use 'format' to fill in the output path
unload_properties_template = 'PARTITIONED BY ({} string, {} string) '  \
  + 'STORED AS PARQUET ' \
  + 'LOCATION \'{}\''

custom_unload_properties_template = 'PARTITIONED BY ({} string) '  \
  + 'STORED AS PARQUET ' \
  + 'LOCATION \'{}\''

hdfs_staging_dir = '/staging/'

states = [
    'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
    'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
    'MD', 'MH', 'MA', 'MI', 'FM', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV',
    'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'MP', 'OH', 'OK', 'OR', 'PW',
    'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'VI',
    'WA', 'WV', 'WI', 'WY', 'AA', 'AE', 'AP', 'UM'
]

genders = ['M', 'F', 'U']

# optimal parquet file size is 500mb - 1gb
# smaller means faster writing (to a point)
# larger means faster reading (to a point)
PARQUET_FILE_SIZE = 1024 * 1024 * 500
