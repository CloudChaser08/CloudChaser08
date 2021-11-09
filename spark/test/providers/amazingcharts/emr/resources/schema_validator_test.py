# import csv
# import os
#
# from spark.providers.amazingcharts.emr.load_transactions import get_tablename_for_date, TABLE_COLS
#
#
# def get_headers(file_name, table_name):
#     table_name = table_name.lower()
#
#     file_schema = TABLE_COLS[table_name]
#
#     with open(file_name, 'r') as infile:
#         dialect = csv.Sniffer().sniff(infile.read(4096))
#         infile.seek(0)
#         reader = csv.reader(infile, dialect)
#         header = next(reader)
#
#     return header, file_schema
#
#
# def validate_file(file_name, table_name):
#     header, file_schema = get_headers(file_name, table_name)
#     return header == file_schema
#
#
# def test_general_validate():
#     assert validate_file(
#         file_name='test/providers/amazingcharts/emr/resources/input/d_costar/sample-d_costar.zip.psv',
#         table_name='d_costar')
#
# def amazing(path, exec_date):
#     validation_flag = True
#     for filename in os.listdir(path):
#         try:
#             tablename = get_tablename_for_date(filename.split('.')[0], exec_date)
#             if tablename.startswith('sample-'):
#                 tablename = tablename.replace('sample-', '')
#             full_file = path + filename
#             validation = validate_file(full_file, tablename)
#             if not validation:
#                 validation_flag = False
#                 print('========================')
#                 print(filename)
#                 print('========================')
#                 header, schema = get_headers(full_file, tablename)
#                 diff = [(i, j) for (i, j) in zip(header, schema) if i != j]
#                 print(diff)
#                 print('========================')
#                 print('\n\n')
#             else:
#                 print('{} correct'.format(filename))
#         except KeyError:
#             print('Unknown file: {}', filename)
#     return validation_flag
#
#
# def test_amazing_oct():
#     assert amazing('test/providers/amazingcharts/emr/resources/input/d_costar/', '2018-10-23')
