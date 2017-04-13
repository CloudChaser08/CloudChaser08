import subprocess
import re


def prefix_part_files(spark, staging_dir, dest_dir, prefix):
    if staging_dir.startswith('hdfs'):
        # filter out directories in ls -R output with re.match
        dirs = map(
            lambda out_str: out_str.split()[-1],
            filter(
                lambda filename: re.match(
                    '^part-[0-9]{5}\.(gz|bz2)$', filename.split('/')[-1]
                ),
                subprocess.check_output([
                    'hdfs', 'dfs', '-ls', '-R', staging_dir
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

        if not staging_dir.endswith('/'):
            staging_dir = staging_dir + '/'

        if not dest_dir.endswith('/'):
            dest_dir = dest_dir + '/'

        subprocess.check_call([
            'hadoop', 'distcp', '-update', staging_dir, dest_dir
        ])

    else:
        raise Exception('Invalid filesystem, staging_dir should be in hdfs')
