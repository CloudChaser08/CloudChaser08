set -e
for line in $(cat files_to_crosswalk.txt)
do
  source=$(echo $line | awk -F ',' '{print $1}')
  target=$(echo $line | awk -F ',' '{print $2}')
  echo running crosswalk on $source
  spark-submit --py-files target/dewey.zip --conf spark.executor.instances=15 --conf spark.executor.cores=2 --conf spark.executor.memory=10G --conf spark.driver.memory=13G providers/nextgen/crosswalk/sparkApplyCrosswalk.py --nextgen_source $source --crosswalk_source s3://salusv/tmp/nextgenCrossWalk/ --nextgen_output $target >> spark.log 2>&1 
done
