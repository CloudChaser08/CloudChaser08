import argparse
import subprocess
from datetime import datetime, timedelta
import os
import time
import requests
import boto3


try:
    os.mkdir("logs/")
except:
    pass

def get_cluster_id(cluster_name=None, start_time=None, verbose=True):
    emr = boto3.client('emr')

    vals = {
        "ClusterStates": ['STARTING', 'WAITING', 'RUNNING', 'BOOTSTRAPPING']
    }

    if start_time:
        vals["CreatedAfter"] = start_time

    page_iterator = emr.get_paginator('list_clusters').paginate(**vals)

    cluster_id = None
    for page in page_iterator:
        for cluster in page['Clusters']:
            if cluster['Name'] == cluster_name:
                cluster_id = cluster['Id']

                if verbose:
                    print(cluster['Id'], cluster['Name'], cluster['Status']['State'])

                return cluster_id
    
    return cluster_id

def get_master_dns(cluster_id=None, cluster_name=None, start_time=None, verbose=True):
    emr = boto3.client('emr')

    if cluster_name:
        cluster_id = get_cluster_id(cluster_name=cluster_name, start_time=start_time)

    # wait until the cluster is ready
    if cluster_id is not None:
        if verbose:
            print("getting cluster address")
        waiter = emr.get_waiter('cluster_running')
        waiter.wait(
            ClusterId=cluster_id,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )

        cluster_info = emr.describe_cluster(ClusterId=cluster_id)
        return cluster_info['Cluster']['MasterPublicDnsName']
    else:
        print("Couldn't get a cluster id")
        return None

def is_job_failed(query_url, application_id):
    new_query_url = query_url + "{}/stages/".format(application_id)

    r = requests.get(new_query_url)
    try:
        items = r.json()
        return any([item["status"] == "FAILED" for item in items])
    except Exception as e:
        # print("error parsing stages")
        return True

def check_for_running_jobs(query_url):
    r = requests.get(query_url)
    try:
        items = r.json()
        incomplete_jobs = []
        failed_jobs = []
        for item in items:
            for attempt in item["attempts"]:
                if attempt["completed"] and is_job_failed(query_url, item["id"]):
                    failed_jobs.append(item)
                    # print("job failed:", item["name"])
                elif not attempt["completed"]:
                    incomplete_jobs.append(item)

        # print([name["name"][:10] for name in failed_jobs])
        return incomplete_jobs, failed_jobs
    except Exception as e:
        print("error?")
        return [], []
    

def wait_for_running_jobs(query_url, interval=120):
    jobs, failed = check_for_running_jobs(query_url)
    done = len(jobs) == 0

    while not done:
        print("Still waiting on these {}".format([item["name"] for item in jobs]))

        jobs, failed = check_for_running_jobs(query_url)
        done = len(jobs) == 0

        time.sleep(interval)
    
    print("num failed:", len(failed))
    # print(failed)

def distcp(dest, src="/staging/"):
    dist_cp_command = [
        's3-dist-cp',
        '--s3ServerSideEncryption',
        '--deleteOnSuccess',
        '--src', src,
        '--dest', dest
    ]
    try:
        subprocess.check_call(dist_cp_command)
    except Exception as e:
        print("Error in distcp", e)


def run_for_days(run_dates, s3_output_location, query_url, feed_name, feed_type, chunk_size):
    command_template = """
        nohup /bin/sh -c "PYSPARK_PYTHON=python3 /home/hadoop/spark/bin/run.sh /home/hadoop/spark/providers/{}/{}/normalize.py --date {}" &> logs/{} &
    """
    count = 0
    for run_date in run_dates:
        pass_date = run_date.strftime("%Y-%m-%d")
        file_date = run_date.strftime("%Y%m%d") + ".out"
        command = command_template.format(feed_name, feed_type, pass_date, file_date)

        print(pass_date)
        subprocess.call(command, shell=True)

        count += 1

        if count % chunk_size == chunk_size - 1: # every chunk_size, push to s3
            wait_for_running_jobs(query_url=query_url)

            distcp(s3_output_location)

        time.sleep(60)

    wait_for_running_jobs(query_url=query_url)

    distcp(s3_output_location)

    # wait for transfer to complete
    wait_for_running_jobs(query_url=query_url)


def run(start_date, end_date, query_url, feed_name, feed_type, s3_output_location="s3://salusv/warehouse/transformed/"):

    wait_for_running_jobs(query_url=query_url)

    for year in [2016, 2017, 2018, 2019, 2020, 2021]:
        for month in range(1, 13):
            if datetime(year, month, 1) > end_date:
                return
            if datetime(year, month, 1) >= start_date:
                start_date = datetime(year, month, 1)
                end_date = datetime(year + 1 if month == 12 else year, 1 if month == 12 else month + 1, 1)

                delta = (end_date - start_date).days
                run_dates = [start_date + timedelta(days=day) for day in range(delta)]

                run_for_days(
                    run_dates=run_dates,
                    s3_output_location=s3_output_location, 
                    query_url=query_url, 
                    feed_name=feed_name,
                    feed_type=feed_type
                )


def get_query_url(feed_name, feed_type, version):
    cluster_name_template = "data-platform-normalization-{}-{}-e2e{}"
    cluster_name = cluster_name_template.format(
        feed_name,
        feed_type,
        version
    )

    address = get_master_dns(cluster_name=cluster_name)

    # address = "ip-10-24-22-114.ec2.internal"
    query_url = "http://{}:18080/api/v1/applications".format(address)

    return query_url

if __name__ == "__main__":

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, default="2019-01-01")
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--qa', default=False, action='store_true')
    parser.add_argument('--feed-name', type=str, default="change_relay")
    parser.add_argument('--feed-type', type=str, default="medicalclaims")
    parser.add_argument('--version', type=str, default="")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.today()


    feed_name = args.feed_name
    feed_type = args.feed_type
    version = args.version

    query_url = get_query_url(feed_name, feed_type, version)

    if args.qa:
        s3_output_location = "s3://healthveritydev/marc/change/dx_relay/20210223_mdv_one_month/"
        run(datetime(2018, 1, 1), datetime(2018, 2, 1), query_url=query_url, s3_output_location=s3_output_location)
    else:
        run(start_date, end_date, query_url=query_url, feed_name=feed_name, feed_type=feed_type)
