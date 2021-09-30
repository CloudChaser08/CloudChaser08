"""manifest utils"""
import subprocess
from pyspark import SparkContext


HDFS_PREFIX = "hdfs://"
OUTPUT_DIR = "/tmp/split-files/"


def list(src):
    """Lists all manifest files that are contained within a given location.

    Args:
        src (str): The HDFS directory whose manifest files will be listed.

            NOTE:
                This is the directory name itself and not a URI.

    Returns:
        files ([str]): The manifest files stored at the given directory.
    """

    list_hdfs_cmd = ['hadoop', 'fs', '-ls', '-R', 'hdfs://{}'.format(src)]
    grep_cmd = ['grep', 'part-']
    awk_cmd = ['awk', '{ print $8 }']

    ls = subprocess.Popen(list_hdfs_cmd, stdout=subprocess.PIPE)
    grep = subprocess.Popen(grep_cmd, stdin=ls.stdout, stdout=subprocess.PIPE)
    result = subprocess.check_output(awk_cmd, stdin=grep.stdout)

    files = result.decode().strip().split("\n")

    return [file for file in files if '.crc' not in file]


def write(file_list, files_per_manifest=1000, output_dir=OUTPUT_DIR):
    """Writes manifests from a list of files that are to be used by s3-dist-cp.
    This function uses Spark to format the file names and then write them out.

    Args:
        file_list ([str]): A list of URIs where each one points to a single,
            target file.
        files_per_manifest (int, optional): The number of files that should be
            gropued per manifest. This value also represents the amount of
            data that will be contained per partition. Default is, 1000.
        output_dir (str, optional): The directory where the manifests should be written.
            Default is, /tmp/split-files/.
    """

    # Becuase this function uses Spark, it's important that we keep track of
    # where it's called during the workflow; as other operations depend on
    # Spark either being active or closed. Thus, if there was no active
    # SparkContext when this function is executing, we will have to
    # stop it at the end.
    previous_context = SparkContext._active_spark_context

    context = SparkContext.getOrCreate()

    files_rdd = context.parallelize(file_list, len(file_list) / files_per_manifest)

    if file_list[0].startswith(HDFS_PREFIX):
        # When using srcPrefixesFile, the src and prefixes need to start with the same URI.
        # However, s3-dist-cp exands /staging/ to hdfs://{ip-address}:port/, so we have
        # to correct the URI's in the file by changing their prefixes.
        new_prefix = subprocess.check_output(['hdfs', 'getconf', '-confKey',
                                              'fs.defaultFS']).decode().strip()

        def reformat_uri(iterator):
            final_iterator = []
            for uri in iterator:
                file_path = uri.split(HDFS_PREFIX)[1]

                final_iterator.append(''.join([new_prefix, file_path]))

            return iter(final_iterator)

        formatted_rdd = files_rdd.mapPartitions(reformat_uri, preservesPartitioning=True)

        formatted_rdd.saveAsTextFile(output_dir)

    else:
        files_rdd.saveAsTextFile(output_dir)

    if not previous_context:
        context.stop()
