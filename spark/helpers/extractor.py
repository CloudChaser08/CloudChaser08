def export_table(sqlContext, table_name, schema_name, location, partitions=20, delimiter='|'):
    full_table_name = '{}.{}'.format(schema_name, table_name)

    sqlContext.sql('select * from {table_name}'.format(table_name=full_table_name)).repartition(partitions)  \
        .write                                                                                               \
        .csv(path=location, compression="gzip", sep=delimiter, quoteAll=True, header=True)
