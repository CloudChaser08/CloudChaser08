import argparse
import subprocess
import gzip
import bz2
import os
import re
import logging

from spark.spark_setup import init

TEST_PATIENTS_DRIVER_S3_KEY = 'incoming/allscripts/Tier1_Patients_For_Removal.csv.gz'


def gen_scrubbing_func(dryrun):
    """This is a wrapper so that we can run this in dryrun mode (do everything
        except for replacing the origin files"""

    def scrub(s3_key):
        """Scrub test patients out of the source data"""
        # we will store the files locally in a tmp location under their own
        # directory names
        dir_name = s3_key.split('.')[0].replace('/', '_')
        try:
            os.makedirs('/mnt/tmp/' + dir_name)
        except OSError as err:
            if err.strerror == "File exists":
                pass
            else:
                raise err

        # The test patient ids can be in any source file, so we each partition
        # processed by this function needs its own copy of the lists of patients
        # to remove
        driver_local_path = '/mnt/tmp/' + dir_name + '/' + TEST_PATIENTS_DRIVER_S3_KEY.split('/')[-1]
        subprocess.check_call(['aws', 's3', 'cp',
                               's3://healthverity/' + TEST_PATIENTS_DRIVER_S3_KEY,
                               driver_local_path])

        test_patients = set()
        with gzip.GzipFile(driver_local_path) as fin:
            for line in fin:
                test_patients.add(line.strip())

        local_path = '/mnt/tmp/' + dir_name + '/' + s3_key.split('/')[-1]
        new_local_path = re.sub('(.gz|.bz2)$', '.bz2.corrected', local_path)

        # getting boto3 on the slave nodes requires too much effort. the CLI is
        # already available
        subprocess.check_call(['aws', 's3', 'cp', 's3://salusv/' + s3_key, local_path])

        fin = None
        if s3_key.endswith('.bz2'):
            fin = bz2.BZ2File(local_path)
        elif s3_key.endswith('.gz'):
            fin = gzip.GzipFile(local_path)
        else:
            raise ValueError("Unknown file type")

        test_patients_removed = set()
        with bz2.BZ2File(new_local_path, 'w') as fout:
            for line in fin:
                src_tokens = line.strip().split('|')

                # The patient ID is in the 6th column of the source files
                if len(src_tokens) >= 6 and src_tokens[5] in test_patients:
                    test_patients_removed.add(src_tokens[5])
                    continue
                else:
                    fout.write(line)

        fin.close()
        os.remove(local_path)

        if not dryrun:
            new_s3_key = re.sub('.gz$', '.bz2', s3_key)
            # S3 delete original
            subprocess.check_call(['aws', 's3', 'rm', 's3://salusv/' + s3_key])
            # S3 upload new file
            subprocess.check_call(['aws', 's3', 'cp', new_local_path, 's3://salusv/' + new_s3_key])

        return (s3_key, len(test_patients_removed))

    return scrub


def run(spark, dryrun):
    """Execute the spark job"""
    # S3 API only returns 1000 results at a time. It's easier to use the cli to
    # list everything
    res = subprocess.check_output(['aws', 's3', 'ls', '--recursive', 's3://salusv/incoming/emr/allscripts/'])
    as_keys = [r.split(" ")[-1] for r in res.split("\n") if r != '']

    as_keys_filtered = []
    for k in as_keys:
        # Test Patients only exist in tier1 data
        if 'tier2' in k:
            continue
        # The Clients table has no patient id in it
        elif 'clients' in k:
            continue
        # The backfill data only joins onto the original data, so it doesn't
        # matter if we don't remove the test patients from it
        elif 'backfill' in k:
            continue
        # deid files are encrypted and can't be corrected. They also don't need
        # correction because they are only joined to other tables
        elif 'deid' in k:
            continue
        # $folder$ files are artifacts of Hive external tables
        elif '$folder$' in k:
            continue
        else:
            as_keys_filtered.append(k)

    as_keys = as_keys_filtered

    rdd = spark.sparkContext.parallelize(as_keys)
    res = rdd.map(gen_scrubbing_func(dryrun))

    # Log the results of the scrubbing: pairs of s3 keys and how many "test
    # patients" from the driver that were found/scrubbed. If everything works
    # out 100%, we should expect to see 1000s scrubbed from each key
    for r in res.collect():
        logging.warn("%s", r)


def main(args):
    # init
    spark, sqlContext = init("Scrub Allscripts Test Patients")

    run(spark, args.dryrun)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', action='store_true', default=False)
    args = parser.parse_args()
    main(args)
