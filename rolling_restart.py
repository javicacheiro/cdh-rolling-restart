#!/usr/bin/env python
# encoding: utf-8
"""
Do a rolling restart based on the status of the instances:

Fields to consider:

 u'configStalenessStatus': u'STALE'
 u'type': u'NODEMANAGER'
 u'maintenanceMode': False
 u'roleState': u'STARTED'
 u'entityStatus': u'GOOD_HEALTH'
 u'healthSummary': u'GOOD'
 u'name': u'yarn-NODEMANAGER-708c5c3ed00070ee1d7cf9b2a39fa0d0'

"""
from __future__ import print_function
import requests
import re
import time
import sys
import argparse

# Disable Unverified HTTPS request warnings
import urllib3
urllib3.disable_warnings()


API_URL = 'https://<CM_HOSTNAME>:7183/api/v31'
USER = '<USER>'
PASSWD = '<PASSWORD>'
DEFAULT_DELAY = 30


def extract_node_identifier(host):
    """Extract the node identifier part in a way it can be used to sort them"""
    m = re.search(r'c(\d+)-(\d+)', host)
    if m:
        rack = m.group(1)
        node = m.group(2)
        return int('{:02d}{:02d}'.format(int(rack), int(node)))
    else:
        raise ValueError('Incorrect hostname: {}'.format(host))


def get_instance_information(instance):
    r = requests.get(
        '{}/clusters/cluster/services/{}/roles/{}'.format(
            API_URL, instance['serviceRef']['serviceName'], instance['name']),
        verify=False, auth=(USER, PASSWD))
    return r.json()


def is_healthy(instance):
    status = get_instance_information(instance)
    if (status['configStalenessStatus'] == 'FRESH' and
            status['roleState'] == 'STARTED' and
            status['entityStatus'] == 'GOOD_HEALTH' and
            status['healthSummary'] == 'GOOD'):
        return True
    else:
        return False


def restart(instance):
    """Restart a given role instance"""
    r = requests.post('{}/clusters/cluster/services/{}/roleCommands/restart'
                      .format(API_URL, instance['serviceRef']['serviceName']),
                      verify=False,
                      auth=(USER, PASSWD),
                      json={"items": [instance['name']]})
    return r.json()


def restart_instances(instances, state='healthy', delay=DEFAULT_DELAY):
    """Restart the given instances that are not in maintainance mode

    By default only healthy instances are restarted, this behaviour can be
    changed using the state parameter.

    :param state: it can have the following values:
        'healthy': only instances thar are healthy are restarted (default)
        'stale': only healthy instances with stale configurations are restarted
        'all': all instances are restarted
    :param delay: seconds to wait between instance restarts
    """
    # We want to restart the cluster using hostname order
    nodes = sorted(instances, key=extract_node_identifier)

    print('Restarting role {} instances'.format(state))
    for node in nodes:
        instance = instances[node]
        if (check_instance_state(instance, state)):
            print('Restarting', node)
            result = restart(instance)
            if len(result['errors']) != 0:
                print('Error restarting instance on', node)
                print('This is the error message returned by the command:')
                print(result['errors'])
                sys.exit(1)
            while not is_healthy(instance):
                print('Waiting for {} to be ready'.format(node))
                time.sleep(30)
            print('{} is now ready waiting additional {} seconds'
                  .format(node, delay))
            time.sleep(delay)
        else:
            print('Skipping', node)


def check_instance_state(instance, state):
    """Check if the given instance is in the given state

    :param state: it can have the following values:
        'healthy': only instances thar are healthy are restarted (default)
        'stale': only healthy instances with stale configurations are restarted
        'all': all instances are restarted
    """
    if state == 'healthy':
        state_condition = (
            instance['maintenanceMode'] is False and
            instance['roleState'] == 'STARTED' and
            instance['entityStatus'] == 'GOOD_HEALTH' and
            instance['healthSummary'] == 'GOOD'
        )
    elif state == 'stale':
        state_condition = (
            instance['configStalenessStatus'] == 'STALE' and
            instance['maintenanceMode'] is False and
            instance['roleState'] == 'STARTED' and
            instance['entityStatus'] == 'GOOD_HEALTH' and
            instance['healthSummary'] == 'GOOD'
        )
    elif state == 'all':
        state_condition = (instance['maintenanceMode'] is False)
    return state_condition


def list_services():
    """List the available services in the cluster"""
    response = requests.get(API_URL + '/clusters/cluster/services/',
                            verify=False, auth=(USER, PASSWD)).json()
    services = [s['name'] for s in response['items']]
    return services


def list_types(service):
    """List the available types in a given service"""
    response = requests.get('{}/clusters/cluster/services/{}/roles/'.format(API_URL, service),
                            verify=False, auth=(USER, PASSWD)).json()
    types = set([s['type'] for s in response['items']])
    return types


def parse_args():
    """Parse command arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('service', choices=list_services(),
                        help='Service to restart')
    parser.add_argument('-d', '--delay', default=DEFAULT_DELAY,
                        help='Delay between instance restarts')
    parser.add_argument('-s', '--staled', action='store_true',
                        help='Restart only instances with staled configuration')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Restart all instances even if they are unhealthy')
    parser.add_argument('-t', '--type', default=None,
                        help='Instance type to restart for the given service')
    parser.add_argument('-l', '--list-types', action='store_true',
                        help='List instace types for the given service')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    service = args.service

    instance_type = args.type

    delay = float(args.delay)

    if args.list_types:
        types = list_types(service)
        print('\n'.join(types))
        sys.exit(0)

    if not args.type:
        print('For the given service specify the instance type that you want to restart.')
        types = list_types(service)
        print('\n'.join(types))
        sys.exit(0)

    # Service
    service = requests.get('{}/clusters/cluster/services/{}/roles/'.format(API_URL, service),
                           verify=False, auth=(USER, PASSWD)).json()
    # All role instances of this service
    instances = service['items']

    # Selected role instances to restart based on type of instance
    if instance_type:
        selected = [i for i in instances if i['type'] == instance_type]
    else:
        selected = instances
    # Associate instance and host
    selected_by_host = {i['hostRef']['hostname']: i for i in selected}
    # We assume that no host has more than one role instance
    assert len(selected) == len(selected_by_host)

    if args.staled:
        restart_instances(selected_by_host, state='staled', delay=delay)
    elif args.force:
        restart_instances(selected_by_host, state='all', delay=delay)
    else:
        restart_instances(selected_by_host, state='healthy', delay=delay)
