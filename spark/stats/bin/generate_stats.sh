#!/bin/bash
set -e
set -x

# Generate all provider stats

if [ -z $4 ] || [ $1 = "-h" ]
then
    echo "usage: ./generate_stats.sh [quarter] [start_date] [end_date] [output_dir]"
    exit 0
fi

QUARTER=$1
START_DATE=$2
END_DATE=$3
S3_OUTPUT_DIR=$4

function generate_stats()
{
    local OPTIND f e
    while getopts "f:e:" opt; do
        case "$opt" in
            f)  feed_id=$OPTARG
                ;;
            e)  earliest_date=$OPTARG
                ;;
            *)
                echo "Invalid arg"
                exit 1
                ;;
        esac
    done

    echo "Generating stats for feed $feed_id"

    echo spark-submit --py-files ../target/dewey.zip --conf spark.executor.instances=15 --conf spark.executor.cores=2 --conf spark.executor.memory=10G --conf spark.driver.memory=13G --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.scheduler.minRegisteredResourcesRatio=1 --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s ../stats_runner.py --feed_id $feed_id --quarter $QUARTER --start_date $START_DATE --end_date $END_DATE --earliest_date $earliest_date --output_dir output/$feed_id

    echo aws s3 cp --recursive output/$feed_id $S3_OUTPUT_DIR$QUARTER/$feed_id
}

# Medical Claims (Old Model)
generate_stats -f 24 -e 2015-01-01      # Private Source 34 (Navicure)
generate_stats -f 26 -e 2014-12-01      # Allscripts Medical Claims
generate_stats -f 19 -e 2015-08-01      # Corrona
generate_stats -f 15 -e 2014-01-01      # Private Source 14 (Ability)

# Medical Claims
generate_stats -f 22 -e 2011-12-01      # Practice Insight
generate_stats -f 10 -e 2013-01-01      # Emdeon DX
generate_stats -f 29 -e 2010-02-01      # Cardinal RCM

# Labs
generate_stats -f 32 -e 2014-01-01      # NeoGenomics
generate_stats -f 14 -e 2008-01-01      # Caris
generate_stats -f 28 -e 2012-04-01      # Courtagen
generate_stats -f 18 -e 2014-08-01      # Quest
generate_stats -f 46 -e 1901-01-01      # LabCorp

# Events
generate_stats -f 27 -e 1994-12-01      # Obit Data
generate_stats -f 38 -e 2015-04-01      # MindBody
generate_stats -f 42 -e 1901-01-01      # Epsilon
generate_stats -f 50 -e 1990-01-01      # Acxiom
generate_stats -f 56 -e 1990-01-01      # Alliance

# Pharmacy Claims (Old Model)
generate_stats -f 11 -e 2013-01-01      # WebMD Rx
generate_stats -f 21 -e 2010-12-01      # Genoa
generate_stats -f 16 -e 2009-07-01      # ESI

# Pharmacy Claims
generate_stats -f 33 -e 2011-01-01      # McKesson Rx
generate_stats -f 36 -e 2010-03-01      # McKesson Rx Restricted
generate_stats -f 34 -e 2015-01-01      # Diplomat
generate_stats -f 45 -e 2017-01-01      # Apothecary By Design
generate_stats -f 30 -e 2010-02-01      # Cardinal Vitalpath
generate_stats -f 39 -e 2011-02-01      # Cardinal PDS

