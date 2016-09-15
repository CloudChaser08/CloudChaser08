#! /usr/bin/python
import subprocess
import re
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('command', type=str, help='Command to execute (create, create-no-wait, or delete)')
parser.add_argument('--identifier', type=str, help='Redshift cluster identifier')
parser.add_argument('--rs_user', type=str, nargs='?', help='Master username for the Redshift database')
parser.add_argument('--rs_password', type=str, nargs='?', help='Password for the Redshift master username')
parser.add_argument('--num_nodes', type=int, nargs='?', help='Number of nodes for the Redshift cluster')
parser.add_argument('--node_type', type=int, nargs='?', help='EC2 node type for the Redshift cluster')
args = parser.parse_args()

if args.command == 'create' or args.command = 'create-no-wait':
    node = args.node_type if args.node_type else 'dc1.large'
    cluster_type = 'multi-node' if args.num_nodes > 1 else 'single-node'
    ['aws', 'redshift', 'create-cluster', '--cluster-identifier', args.identifier,
            '--node-type', node, '--number-of-nodes', args.num_nodes,
            '--cluster-type', cluster_type, '--master-username', args.rs_user,
            '--master-user-password', args.rs_password]

    if args.comman == 'create':
        ['aws', 'redshift', 'wait', 'cluster-available', 
            '--cluster-identifier', args.identifier]
        print 'export PGUSER="' + args.rs_user + '"'
        print 'export PGPASSWORD="' + args.rs_password + '"'
        print 'export PGHOST="' + args.rs_user + '"'
        print 'export PGPORT="5439"'
        print 'export PGDATABASE="dev"'

if args.command == 'kill':
    ['aws', 'redshift', 'delete-cluster', '--cluster-identifier', args.identifier]
