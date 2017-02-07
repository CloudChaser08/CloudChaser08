#! /bin/bash
spark-submit --jars ~/spark_norm_common/json-serde-1.3.7-jar-with-dependencies.jar --conf spark.sql.shuffle.partitions=40 --conf spark.executor.cores=8 --conf spark.executor.instances=5 sparkNormalizeAllscriptsDX.py --date='2016-11-30' --setid='DEID_CLAIMS_20161130.out' --first_run > spark.out 2>&1
hadoop fs -get /text/medicalclaims/allscripts_30rounds ./
for d in $(ls allscripts_30rounds)
do
    for f in $(ls allscripts_30rounds/$d)
    do
        f2="2016-11-30_$f"
        mv allscripts_30rounds/$d/$f allscripts_30rounds/$d/$f2
    done
done
aws s3 cp --recursive allscripts_30rounds s3://healthveritydev/ifishbein/allscripts_emr_test/
rm -r allscripts_30rounds
hadoop fs -rm -r /text
