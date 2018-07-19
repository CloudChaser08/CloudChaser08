prefix=$(echo $0 | sed 's/bin\/run.sh$/./')
prefix=$(echo $prefix | sed 's/\/bin\/run.sh$//')
prefix=$(echo $prefix | sed 's/.\/run.sh$/../')
spark-submit --py-files $prefix/target/dewey.zip --jars $prefix/common/json-serde-1.3.7-jar-with-dependencies.jar,$prefix/common/HiveJDBC41.jar --conf spark.driver.memory=10G --conf spark.executor.cores=4 --conf spark.file.useFetchCache=false --conf spark.hadoop.fs.s3a.connection.maximum=1000 $@
