import argparse
import csv
import io
import subprocess
import gzip
import bz2
import hashlib
import base64
import os
import itertools
import re
import logging

import boto3
from spark.spark_setup import init

HIDDEN_PHI_DRIVER_S3_KEY = 'incoming/ng-lssa/HiddenPHIHandling/DriverFile/LSDXPHIThru20180717-HV.txt'


def gen_scrubbing_func(dryrun):
    """This is a wrapper so that we can run this in dryrun mode (do everything
        except for replacing the origin files"""

    def scrub(tup):
        """Scrub "hidden PHI" out of the source data"""

        # pairs of s3_key - list of "hidden PHI" metadata
        # The "hidden PHI" metadata tells us what values need to be scrubbed
        s3_key = tup[0]
        hphi_metas = tup[1]

        # keys will be row-level metadata and values are a list of value-level
        # metadata (column sequence number, sha256 of value)
        # there can be multiple "hidden PHI" values in a single source data row
        value_metas_by_row = {}

        # we will define row-level metadata as what exists in columns 7, 5,
        # and 4, preemble, encounter_id, and patient_id, respectively
        for meta in hphi_metas:
            row_id = (meta[7], meta[5], meta[4])
            value_id = (meta[8], meta[9])
            if row_id not in value_metas_by_row:
                value_metas_by_row[row_id] = []

            value_metas_by_row[row_id].append(value_id)

        # we will store the files locally in a tmp location under their own
        # directory names
        dir_name = s3_key.split('/')[-1].split('.')[0]
        try:
            os.makedirs('/mnt/tmp/' + dir_name)
        except OSError as err:
            if err.strerror == "File exists":
                pass
            else:
                raise err

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

        with bz2.BZ2File(new_local_path, 'w') as fout:
            for line in fin:
                src_tokens = line.strip().split('|')
                found = False

                # See spark/providers/nextgen/emr/load_transactions.py for
                # file layouts
                if len(src_tokens) >= 5:
                    src_row_id = (src_tokens[3], src_tokens[4], src_tokens[1])
                    if src_row_id in value_metas_by_row:
                        remaining = []
                        for value_meta in value_metas_by_row[src_row_id]:
                            src_value_idx = 3 + int(value_meta[0])
                            src_value = src_tokens[src_value_idx]
                            if base64.b64encode(hashlib.sha256(src_value).digest()) == value_meta[1]:
                                src_tokens[src_value_idx] = ''
                                found = True
                            else:
                                remaining.append(value_meta)
                        value_metas_by_row[src_row_id] = remaining

                if found:
                    # reconstruct the row now that we've scrubbed it
                    fout.write('|'.join(src_tokens) + "\n")
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

        not_scrubbed = 0
        for (k, v) in value_metas_by_row.items():
            not_scrubbed += len(v)

        return s3_key, not_scrubbed

    return scrub


def run(spark, dryrun):
    """Execute the spark job"""
    # S3 API only returns 1000 results at a time. It's easier to use the cli to
    # list everything
    res = subprocess.check_output(['aws', 's3', 'ls', '--recursive', 's3://salusv/incoming/emr/nextgen/'])
    ng_keys = [r.split(" ")[-1] for r in res.split("\n") if r != '']

    # In the "hidden PHI driver file", files are identified by enterprise id and
    # batch date
    identifier_to_key = {re.search('LSSA_(....._......)', ngk).groups()[0]: ngk for ngk in ng_keys}

    s3 = boto3.resource('s3')
    obj = s3.Object('healthverity', HIDDEN_PHI_DRIVER_S3_KEY)
    res = obj.get()

    # Put the contents into a file-like object
    csv_file = io.StringIO()
    csv_file.write(res['Body'].read())
    csv_file.seek(0)

    # Fields:
    # HV|eid|file_date|control_number|patient_id|encounter_id|encounter_date|preemble|record_seq_num|sha256_of_value
    csv_reader = csv.reader(csv_file, delimiter='|')

    rows = [r for r in csv_reader]

    # Some of the "hidden PHI" prescribed to enterprise ID 00186 batch date
    # 2017/12 was observed to be in the following month's batch. This logic adds
    # creates extra rows of "hidden PHI" that need to be looked up and removed
    # from the 2018/01 batch
    extra_rows = []
    for r in rows:
        if r[1] == '00186' and r[2] == '201712':
            r2 = list(r)
            r2[2] = '201801'
            extra_rows.append(r2)

    rows = rows + extra_rows

    # Group these potential "hidden PHI" rows by file name, so each file can be
    # worked on in parallel
    grouped = []
    for k, g in itertools.groupby(rows, lambda r: (r[1], r[2])):
        ng_key = identifier_to_key.get(k[0] + '_' + k[1])
        if not ng_key:
            logging.warn("No file found for enterprise %s file_date %s", k[0], k[1])
            continue
        grouped.append((ng_key, list(g)))

    rdd = spark.sparkContext.parallelize(grouped)
    res = rdd.map(gen_scrubbing_func(dryrun))

    # Log the results of the scrubbing: pairs of s3 keys and how many
    # "hidden phi" rows from the driver that could not be found/scrubbed. If
    # everything works out 100%, we should expect to see 0s for every s3 key
    for r in res.collect():
        logging.info("%s", r)


def main(args):
    # init
    spark, sqlContext = init("Scrub Nextgen")

    run(spark, args.dryrun)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', action='store_true', default=False)
    args = parser.parse_args()
    main(args)
