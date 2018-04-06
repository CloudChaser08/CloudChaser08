#!/bin/bash
set -e
set -x

# Generate all provider stats

if [ -z $3 ] || [ $1 = "-h" ]
then
    echo "usage: ./generate_stats.sh [quarter] [start_date] [end_date]"
    exit 0
fi

QUARTER=$1
START_DATE=$2
END_DATE=$3

function generate_stats()
{
    local OPTIND f
    while getopts "f:" opt; do
        case "$opt" in
            f)  feed_id=$OPTARG
                ;;
            *)
                echo "Invalid arg"
                exit 1
                ;;
        esac
    done

    echo "Generating stats for feed $feed_id"

    spark-submit --py-files ../../target/dewey.zip --conf spark.executor.instances=15 --conf spark.executor.cores=2 --conf spark.executor.memory=10G --conf spark.driver.memory=13G --conf spark.yarn.executor.memoryOverhead=1024 --conf spark.scheduler.minRegisteredResourcesRatio=1 --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s ../stats_runner.py --feed_id $feed_id --quarter $QUARTER --start_date $START_DATE --end_date $END_DATE
}

# Medical Claims (Old Model)
generate_stats -f 24        # Private Source 34 (Navicure)
generate_stats -f 26        # Allscripts Medical Claims
generate_stats -f 19        # Corrona
generate_stats -f 15        # Private Source 14 (Ability)

# Medical Claims
generate_stats -f 22        # Practice Insight
generate_stats -f 10        # Emdeon DX
generate_stats -f 29        # Cardinal RCM

# Labs
generate_stats -f 32        # NeoGenomics
generate_stats -f 14        # Caris
generate_stats -f 28        # Courtagen
generate_stats -f 18        # Quest
generate_stats -f 46        # LabCorp

# Events
generate_stats -f 27        # Obit Data
generate_stats -f 38        # MindBody
generate_stats -f 42        # Epsilon
generate_stats -f 50        # Acxiom
generate_stats -f 56        # Alliance

# Pharmacy Claims (Old Model)
generate_stats -f 11        # WebMD Rx
generate_stats -f 21        # Genoa
generate_stats -f 16        # ESI

# Pharmacy Claims
generate_stats -f 33        # McKesson Rx
generate_stats -f 36        # McKesson Rx Restricted
generate_stats -f 34        # Diplomat
generate_stats -f 45        # Apothecary By Design
generate_stats -f 30        # Cardinal Vitalpath
generate_stats -f 39        # Cardinal PDS

