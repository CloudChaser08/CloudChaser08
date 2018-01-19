#! /usr/bin/python

import argparse
import subprocess
import requests
import json
import datetime
import os
import math

parser = argparse.ArgumentParser()
parser.add_argument('cluster_name', type=str, help='Name of EMR cluster')
parser.add_argument('cluster_size', type=int,
        help='Number of Core nodes in EMR cluster')
parser.add_argument('core_instance_type', type=str, 
        help='EC2 instance type for Core nodes')
parser.add_argument('applications', type=str, 
        help='Applications to install on EMR cluster')
parser.add_argument('use_ebs', type=str, 
        help='Attach EBS volumes to Core nodes')
parser.add_argument('ebs_size', type=int, 
        help='Size of EBS volumes (GB) to attach to Core nodes')
parser.add_argument('use_spot_bids', type=str, 
        help='Bid on spot instances instead of using on-demand pricing')
parser.add_argument('use_analytics_metastore', type=str, 
        help='Include the EMR configuration file that enables use of the analytics metastore')
parser.add_argument('cluster_purpose', type=str, 
        help='Description of the purpose of the cluster')
args = parser.parse_args()

offers = None
def get_on_demand_price(instance_type):
    global offers
    if offers is None:
        resp = requests.get("https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json")
        offers = json.loads(resp.text)

    for product in offers['products']:
        if 'productFamily' in offers['products'][product] \
            and offers['products'][product]['productFamily'] == 'Compute Instance' \
            and offers['products'][product]['attributes']['location'] == 'US East (N. Virginia)' \
            and offers['products'][product]['attributes']['operatingSystem'] == 'Linux' \
            and offers['products'][product]['attributes']['instanceType'] == instance_type \
            and offers['products'][product]['attributes']['tenancy'] == 'Shared':
                return float(offers['terms']['OnDemand'][product] \
                        .values()[0]['priceDimensions'].values()[0]['pricePerUnit']['USD'])

AZ_INFO = {
    '1a' : {'subnet' : 'subnet-049f4c4f'},
    '1b' : {'subnet' : 'subnet-48b6bc12'},
    '1d' : {'subnet' : 'subnet-315f4f1d'},
    '1e' : {'subnet' : 'subnet-a435e69b'}
}

def get_avg_spot_price(instance_type):
    az_to_use = None
    best_expected_cost = None
    average = {}
    for az in AZ_INFO:
        aws_command = ['aws', 'ec2', 'describe-spot-price-history', '--start-time',
                        (datetime.datetime.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d'),
                        '--product-description', 'Linux/UNIX', '--instance-type',
                        instance_type, '--availability-zone', 'us-east-' + az]
        price_history = json.loads(subprocess.check_output(aws_command))
        price_history['SpotPriceHistory'].sort(key=lambda x: x['Timestamp'])

        # The last price change period will go through now
        price_history['SpotPriceHistory'].append(
            {'Timestamp' : datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')}
        )

        # Calculate the average price per hour over the past 7 days. Use the
        # durations of the spot prices as weights when calculating average price
        total = 0
        total_time = 0.0
        for i, x in enumerate(price_history['SpotPriceHistory'][:-1]):
            start = datetime.datetime.strptime(x['Timestamp'], '%Y-%m-%dT%H:%M:%S.000Z')
            end   = datetime.datetime.strptime(
                price_history['SpotPriceHistory'][i+1]['Timestamp'], '%Y-%m-%dT%H:%M:%S.000Z'
            )
            span  = end - start
            total += float(x['SpotPrice']) * (span.days * 24.0 + span.seconds / 3600.0) 
            total_time += (span.days * 24.0 + span.seconds / 3600.0)
        average[az] = total / total_time

        # Calculate the per hour price deviation. Use the durations of the
        # deviations as weights when calculating average deviation
        var_total = 0
        for i, x in enumerate(price_history['SpotPriceHistory'][:-1]):
            start = datetime.datetime.strptime(x['Timestamp'], '%Y-%m-%dT%H:%M:%S.000Z')
            end   = datetime.datetime.strptime(
                price_history['SpotPriceHistory'][i+1]['Timestamp'], '%Y-%m-%dT%H:%M:%S.000Z'
            )
            span  = end - start
            var_total += math.pow(float(x['SpotPrice']) - average[az], 2) * (span.days * 24.0 + span.seconds / 3600.0)
        std_dev = math.sqrt(var_total / (total_time - 1))

        # We pick the best Availability Zone based on the best expected price
        # We can be 97.5% sure that the price will be less than avg + 2 * std_dev
        if best_expected_cost is None or (average[az] + 2 * std_dev) < best_expected_cost:
            az_to_use = az
            best_expected_cost = (average[az] + 2 * std_dev)
    return (average[az_to_use], az_to_use)


# Definte the EMR Instance Groups
MASTER_TYPE = 'm3.xlarge'
MASTER_IG  = "Name=MASTER,InstanceGroupType=MASTER,"
MASTER_IG += "InstanceCount=1,InstanceType={}".format(MASTER_TYPE)
od_price = get_on_demand_price(MASTER_TYPE)
max_price = od_price
expected_price = None
if args.use_spot_bids == 'true':
    expected_price = get_avg_spot_price(MASTER_TYPE)[0]
    MASTER_IG += ",BidPrice={}".format(od_price)

CORE_IG  = "InstanceGroupType=CORE,"
CORE_IG += "InstanceCount={},InstanceType={}".format(args.cluster_size, args.core_instance_type)
od_price = get_on_demand_price(args.core_instance_type)
max_price += od_price * args.cluster_size
best_az = '1d'
if args.use_spot_bids == 'true':
    (price, best_az) = get_avg_spot_price(args.core_instance_type)
    expected_price += price * args.cluster_size
    CORE_IG += ",BidPrice={}".format(od_price)
if args.use_ebs == 'true':
    CORE_IG += (",EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs="
            "[{VolumeSpecification={VolumeType=gp2,SizeInGB=" + str(args.ebs_size) + "}}]}")

EC2_ATTR=("KeyName=emr_deployer,SubnetId={},".format(AZ_INFO[best_az]['subnet']) + \
        "InstanceProfile=EMR_EC2_DefaultRole,ServiceAccessSecurityGroup=sg-39e8c241,"
        "EmrManagedMasterSecurityGroup=sg-46e8c23e,EmrManagedSlaveSecurityGroup=sg-47e8c23f")
emr_config_options = []
if args.use_analytics_metastore == 'true':
    emr_config_options = ['--configurations', 'file://bin/general-conf/hiveConfiguration.json']

emr_tags = "EMRCluster=" + args.cluster_name + "_" + datetime.datetime.now().strftime('%Y%m%d%H%M') + \
        " Service=" + args.cluster_purpose

print subprocess.check_output(['aws', 'emr', 'create-cluster',
    '--release-label', 'emr-5.4.0',
    '--applications'] + args.applications.split(' ') + \
    emr_config_options + \
    ['--service-role', 'EMR_DefaultRole',
    '--enable-debugging',
    '--ec2-attributes', EC2_ATTR,
    '--name', args.cluster_name,
    '--tags'] + emr_tags.split(' ') + \
    ['--log-uri', 's3://salusvlogs/presto/jenkins/{}/'.format(datetime.datetime.now().strftime('%Y%m%dT%H%M%S')),
    '--instance-groups', MASTER_IG, CORE_IG])
