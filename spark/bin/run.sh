function get_optimal_memory_config()
{
    for cid in $(aws emr list-clusters --active | jq '.Clusters[].Id' | sed 's/"//g')
    do
        if [[ $(aws emr describe-cluster --cluster-id $cid | jq '.Cluster.MasterPublicDnsName' | sed 's/"//g') = $(hostname)* ]]
        then
            instance=$(aws emr describe-cluster --cluster-id $cid | jq '.Cluster.InstanceGroups[] | if (.Name == "CORE") then .InstanceType else empty end' | sed 's/"//g' | cut -d'.' -f1)
            mult=$(aws emr describe-cluster --cluster-id $cid | jq '.Cluster.InstanceGroups[] | if (.Name == "CORE") then .InstanceType else empty end' | sed 's/"//g' | cut -d'.' -f2 | cut -d'x' -f1)
            #Default multiplier of 1
            mult=$(if [ -z $mult ]; then echo 1; else echo $mult; fi)
            #These numbers were derived based documented Hadoop memory allocation and a 10% overhead per executor
            #https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html
            case $instance in
                m[4-5])
                    mem='--conf spark.executor.memory='$(expr 10500 + 200 '*' $mult)'M'
                    ;;
                c[4-5])
                    mem='--conf spark.executor.memory=5G'
                    ;;
                r[4-5])
                    mem='--conf spark.executor.memory='$(expr 20500 + 400 '*' $mult)'M'
                    ;;
                i3)
                    mem='--conf spark.executor.memory='$(expr 20500 + 400 '*' $mult)'M'
                    ;;
                *)
                    mem=''
                    ;;
            esac
            break
        fi
    done
    echo $mem
}

prefix=$(echo $0 | sed 's/bin\/run.sh$/./')
prefix=$(echo $prefix | sed 's/\/bin\/run.sh$//')
prefix=$(echo $prefix | sed 's/.\/run.sh$/../')
mem_cfg=$(get_optimal_memory_config)
spark-submit --py-files $prefix/target/dewey.zip \
    --jars $prefix/common/json-serde-1.3.7-jar-with-dependencies.jar,$prefix/common/HiveJDBC41.jar \
    --conf spark.driver.memory=10G --conf spark.executor.cores=4 --conf spark.sql.shuffle.partitions=5000 \
    --conf spark.default.parallelism=5000 --conf spark.file.useFetchCache=false \
    --conf spark.hadoop.fs.s3a.connection.maximum=1000 \
    --conf spark.sql.autoBroadcastJoinThreshold=209715200 \
    --conf spark.driver.maxResultSize=3G \
    $mem_cfg $@
