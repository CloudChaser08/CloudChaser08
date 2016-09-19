#! /usr/bin/python
import subprocess
import re
import argparse
import time
import sys
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument('command', type=str, help='Command to execute (create, create-no-wait, or delete)')
parser.add_argument('--identifier', type=str, help='Redshift cluster identifier')
parser.add_argument('--rs_user', type=str, nargs='?', help='Master username for the Redshift database')
parser.add_argument('--rs_password', type=str, nargs='?', help='Password for the Redshift master username')
parser.add_argument('--num_nodes', type=int, nargs='?', help='Number of nodes for the Redshift cluster')
parser.add_argument('--node_type', type=int, nargs='?', help='EC2 node type for the Redshift cluster')
args = parser.parse_args()

# We're going to pipe stdout and stderr from some commands to /dev/null
FNULL = open(os.devnull, 'w')
if args.command == 'create' or args.command == 'create-no-wait':
    node = args.node_type if args.node_type else 'dc1.large'
    cluster_type = 'multi-node' if args.num_nodes > 1 else 'single-node'
    user = args.rs_user or 'hvuser'
    password = args.rs_password or 'HV1user2'
    num_nodes = args.num_nodes or 10

    # Check if we already have a cluster with this identifier
    try:
        cluster_details = subprocess.check_call(['aws', 'redshift', 'describe-clusters', 
            '--cluster-identifier', args.identifier], stdout=FNULL, stderr=subprocess.STDOUT)
        print "A cluster with this identifier already exists. Please try again..."
        sys.exit(0)
    except Exception as err:
        # No such cluster, we're good
        pass

    # Create the cluster
    cmd = ['aws', 'redshift', 'create-cluster', '--cluster-identifier', args.identifier,
            '--node-type', node, '--cluster-type', cluster_type, '--master-username', user,
            '--master-user-password', password, '--tags', 'Key=temporary,Value=1',
            '--no-publicly-accessible', '--vpc-security-group-ids', 'sg-ca9fabae',
            '--cluster-subnet-group', 'vpcrsgroup']
    if num_nodes > 1:
        cmd.extend(['--number-of-nodes', str(num_nodes)])

    print "Cluster created. Waiting for cluster to become available..."
    subprocess.call(cmd, stdout=FNULL, stderr=subprocess.STDOUT)

    # Wait until the cluster is ready and print out the psql connection variables
    if args.command == 'create':
        cmd = ['aws', 'redshift', 'wait', 'cluster-available', 
            '--cluster-identifier', args.identifier]
        subprocess.call(cmd, stdout=FNULL, stderr=subprocess.STDOUT)

        cmd = ['aws', 'redshift', 'describe-clusters', 
            '--cluster-identifier', args.identifier]
        description = json.loads(subprocess.check_output(cmd, stderr=FNULL))
        host = description['Clusters'][0]['Endpoint']['Address']

        # So this script can be eval'd
        print 'export PGUSER="' + user + '"'
        print 'export PGPASSWORD="' + password + '"'
        print 'export PGHOST="' + host + '"'
        print 'export PGPORT="5439"'
        print 'export PGDATABASE="dev"'

if args.command == 'delete':
    # Check if there is a deletable cluster with this identifier
    cmd = ['aws', 'redshift', 'describe-clusters', '--cluster-identifier', args.identifier]
#    , '--tag-keys', 'temporary',
#        '--tag-values', '1']
    try:
        description = json.loads(subprocess.check_output(cmd, stderr=FNULL))
        killable = False
        tags = description['Clusters'][0]['Tags']
        for t in tags:
            if t['Key'] == 'temporary' and t['Value'] == '1':
                killable = True
                break
        if not killable:
            raise Exception("Cluster is not killable")
    except Exception as err:
        print "There are no deletable clusters with id {}".format(args.identifier)
        sys.exit(0)

    # Delete the cluster
    cmd = ['aws', 'redshift', 'delete-cluster', '--cluster-identifier', args.identifier,
            '--skip-final-cluster-snapshot']
    try:
        subprocess.check_call(cmd, stdout=FNULL, stderr=subprocess.STDOUT)
        print "Deleting cluster {}".format(args.identifier)
    except Exception as err:
        print "An error occurred while trying to delete cluster {}\n{}".format(args.identifier, err)
        sys.exit(0)

