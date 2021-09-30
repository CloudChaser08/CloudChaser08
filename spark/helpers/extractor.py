"""extractor"""
import subprocess


def export_table(
        sqlContext, table_name, schema_name, hdfs_location,
        partitions=20, delimiter='|', output_file_name=None
):
    full_table_name = '{}.{}'.format(schema_name, table_name) if schema_name else table_name

    sqlContext.sql('select * from {table_name}'.format(table_name=full_table_name)).repartition(partitions)  \
        .write \
        .csv(path=hdfs_location, compression="gzip", sep=delimiter, quoteAll=True, header=True)

    # rename files
    if output_file_name:

        # list of raw filenames
        part_filenames = [
            line.split()[7] for line in
            subprocess.check_output(['hdfs', 'dfs', '-ls', '-R', hdfs_location]).split('\n')
            if len(line.split()) >= 8 and 'part-' in line
        ]

        for i, full_filename in enumerate(part_filenames):
            directory = '/'.join(full_filename.split('/')[:-1])
            filename = full_filename.split('/')[-1]
            suffix = filename[filename.index('.'):]

            new_filename = directory + '/' + output_file_name

            # if there are more than one to rename, suffix the
            # filename with an integer
            if len(part_filenames) > 1:
                new_filename = new_filename + '-' + str(i)

            subprocess.check_call(['hdfs', 'dfs', '-mv', full_filename, new_filename + suffix])
