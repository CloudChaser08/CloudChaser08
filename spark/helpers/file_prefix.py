import subprocess
import re


def prefix_part_files(spark, base_dir, prefix):
    if base_dir.startswith('hdfs'):
        # filter out directories in ls -R output with re.match
        dirs = map(
            lambda out_str: out_str.split()[-1],
            filter(
                lambda filename: re.match(
                    '^part-[0-9]{5}\.(gz|bz2)$', filename.split('/')[-1]
                ),
                subprocess.check_output([
                    'hdfs', 'dfs', '-ls', '-R', base_dir
                ]).split('\n')
            )
        )

        # parallelize filenames and add a prefix to each one in hdfs
        def prefixer(filename):
            subprocess.check_call([
                'hdfs', 'dfs', '-mv', filename,
                '/'.join(filename.split('/')[:-1]) + '/'
                + prefix + filename.split('/')[-1]
            ])

        spark.sparkContext.parallelize(dirs).foreach(prefixer)

    else:
        # TODO: Add support for S3
        raise Exception('Invalid filesystem')
