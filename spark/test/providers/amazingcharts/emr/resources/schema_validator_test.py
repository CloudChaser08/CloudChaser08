import csv
import os

import spark.providers.amazingcharts.emr.transactional_schemas_v1 as transactional_schemas_v1
import spark.providers.amazingcharts.emr.transactional_schemas_v2 as transactional_schemas_v2
import spark.providers.amazingcharts.emr.transactional_schemas_v3 as transactional_schemas_v3


def get_table_conf(date_input):
    if date_input >= '2021-08-31':
        source_table_schemas = transactional_schemas_v3
    elif date_input >= '2018-01-01':
        source_table_schemas = transactional_schemas_v2
    else:
        source_table_schemas = transactional_schemas_v1
    return source_table_schemas

def get_headers(file_name, table_name, date_input):
    table_name = table_name.lower()
    source_table_schemas = get_table_conf(date_input)
    file_schema = source_table_schemas.TABLE_CONF[table_name]
    with open(file_name, 'r') as infile:
        dialect = csv.Sniffer().sniff(infile.read(4096))
        infile.seek(0)
        reader = csv.reader(infile, dialect)
        header = next(reader)
    return header, file_schema.columns


def validate_file(file_name, table_name, date_input):
    header, file_schema = get_headers(file_name, table_name, date_input)
    return header == file_schema


def test_general_validate():
    assert validate_file(
        file_name='test/providers/amazingcharts/emr/resources/input/d_costar/sample-d_costar.zip.psv',
        table_name='d_costar', date_input='2021-08-31')


def get_tablename_for_date(table, batch_date):
    source_table_schemas = get_table_conf(batch_date)
    table = table.lower()
    tns = sorted([tn for tn in source_table_schemas.TABLE_CONF.keys() if tn.startswith(table)])
    tn = None
    for t in tns:
        if t > (table + '_' + batch_date.replace('-', '')):
            tn = t
            break
    if tn is None:
        tn = table
    return tn


def amazing(path, exec_date):
    validation_flag = True
    for filename in os.listdir(path):
        try:
            tablename = get_tablename_for_date(filename.split('.')[0], exec_date)
            if tablename.startswith('sample-'):
                tablename = tablename.replace('sample-', '')
            full_file = path + filename
            validation = validate_file(full_file, tablename, exec_date)
            if not validation:
                validation_flag = False
                print('========================')
                print(filename)
                print('========================')
                header, schema = get_headers(full_file, tablename)
                diff = [(i, j) for (i, j) in zip(header, schema) if i != j]
                print(diff)
                print('========================')
                print('\n\n')
            else:
                print('{} correct'.format(filename))
        except KeyError:
            print('Unknown file: {}', filename)
    return validation_flag


def test_amazing_oct():
    assert amazing('test/providers/amazingcharts/emr/resources/input/d_costar/', '2021-08-31')
