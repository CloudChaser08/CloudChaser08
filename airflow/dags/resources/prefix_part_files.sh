#!/bin/bash

STAGING_DIR=$1
PREFIX=$2

MAX_OPERATIONS=4

add_prefix() {
  hdfs dfs -mv $1 \
       $(echo $1 | rev | cut -d/ -f2- | rev)/${PREFIX}_$(echo $1 \
                                                           | rev | cut -d/ -f1 | rev)
}

# Apply the add_prefix function concurrently to MAX_OPERATIONS files at a time 

files_to_rename=( $(hdfs dfs -ls -R $STAGING_DIR | awk '{print $8}' | grep '.gz$') )
while [ ${#files_to_rename[@]} -gt 0 ]
do 
  running_jobs=( $(jobs -pr) )
  if [ ${#running_jobs[@]} -lt $MAX_OPERATIONS ]
  then
    echo "Prefixing ${files_to_rename[0]}"
    add_prefix ${files_to_rename[0]} &
    files_to_rename=( "${files_to_rename[@]:1}" )
  else
    sleep 1
  fi
done

wait
