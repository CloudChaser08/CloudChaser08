import argparse

from pyspark.sql.functions import *

import spark.helpers.postprocessor as postprocessor
from spark.spark_setup import init


def run(spark, args):
    """
    Schema source: https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS-Items/2018-HCPCS-Record-Layout-.html

    File: https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets/Alpha-Numeric-HCPCS-Items/2018-Alpha-Numeric-HCPCS-File-.html?DLPage=1&DLEntries=10&DLSort=0&DLSortDir=descending
    """
    hcpcs = spark.read.text(args.incoming)

    hcpcs = hcpcs.where(length(col("value")) == 320)

    print(hcpcs.head(50))

    new_df = hcpcs.select(
        hcpcs.value.substr(1, 5).alias('hcpc'),
        hcpcs.value.substr(6, 4).alias('seqnum'),
        hcpcs.value.substr(11, 1).alias('recid'),
        hcpcs.value.substr(12, 79).alias('long_description'),
        hcpcs.value.substr(92, 28).alias('short_description'),
        hcpcs.value.substr(120, 2).alias('price1'),
        hcpcs.value.substr(122, 2).alias('price2'),
        hcpcs.value.substr(124, 2).alias('price3'),
        hcpcs.value.substr(126, 2).alias('price4'),
        hcpcs.value.substr(128, 1).alias('multi_pi'),
        hcpcs.value.substr(129, 6).alias('cim1'),
        hcpcs.value.substr(135, 6).alias('cim2'),
        hcpcs.value.substr(141, 6).alias('cim3'),
        hcpcs.value.substr(147, 8).alias('mcm1'),
        hcpcs.value.substr(155, 8).alias('mcm2'),
        hcpcs.value.substr(163, 8).alias('mcm3'),
        hcpcs.value.substr(171, 10).alias('statute'),
        hcpcs.value.substr(181, 3).alias('labcert1'),
        hcpcs.value.substr(184, 3).alias('labcert2'),
        hcpcs.value.substr(187, 3).alias('labcert3'),
        hcpcs.value.substr(190, 3).alias('labcert4'),
        hcpcs.value.substr(193, 3).alias('labcert5'),
        hcpcs.value.substr(196, 3).alias('labcert6'),
        hcpcs.value.substr(199, 3).alias('labcert7'),
        hcpcs.value.substr(202, 3).alias('labcert8'),
        hcpcs.value.substr(205, 5).alias('xref1'),
        hcpcs.value.substr(210, 5).alias('xref2'),
        hcpcs.value.substr(215, 5).alias('xref3'),
        hcpcs.value.substr(220, 5).alias('xref4'),
        hcpcs.value.substr(225, 5).alias('xref5'),
        hcpcs.value.substr(230, 1).alias('cov'),
        hcpcs.value.substr(231, 2).alias('asc_grp'),
        hcpcs.value.substr(233, 8).alias('asc_dt'),
        hcpcs.value.substr(241, 3).alias('opps'),
        hcpcs.value.substr(244, 1).alias('opps_pi'),
        hcpcs.value.substr(245, 8).alias('opps_dt'),
        hcpcs.value.substr(253, 4).alias('procnote'),
        hcpcs.value.substr(257, 3).alias('betos'),
        hcpcs.value.substr(261, 1).alias('tos1'),
        hcpcs.value.substr(262, 1).alias('tos2'),
        hcpcs.value.substr(263, 1).alias('tos3'),
        hcpcs.value.substr(264, 1).alias('tos4'),
        hcpcs.value.substr(265, 1).alias('tos5'),
        hcpcs.value.substr(266, 3).alias('anest_bu'),
        hcpcs.value.substr(269, 8).alias('add_dt'),
        hcpcs.value.substr(277, 8).alias('act_eff_dt'),
        hcpcs.value.substr(285, 8).alias('term_dt'),
        hcpcs.value.substr(293, 1).alias('action_cd')

    )

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(new_df).repartition(args.partitions).write.parquet(args.s3_parquet_loc, mode='overwrite')


def main(args):
    spark, sqlContext = init('Reference HCPCS')

    run(spark, args)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--incoming', type=str)
    parser.add_argument('--s3_parquet_loc', type=str)
    parser.add_argument('--partitions', default=20, type=int)
    args = parser.parse_args()
    main(args)
