#!/usr/bin/env python
# -*- coding: utf-8 -*-
# noris network AG 2020
# Tim Zöllner
__date__ = '2020-06-22'
__version__ = '0.4.2'

#from docopt import docopt
import argparse
import sys
import ssl
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
# check elasticsearch module
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import ConnectionError, \
        TransportError, \
        ConnectionTimeout, \
        NotFoundError, \
        RequestError
except ImportError as missing:
    print (
        'Error - could not import all required Python modules\n"%s"'
        % missing + '\nDependency installation with pip:\n'
        '"# pip install docopt elasticsearch"'
        'or use your prefered package manage, i.e. APT or YUM.\n Example: yum install python-docopt python-elasticsearch')
    sys.exit(2)
#ssl._create_default_https_context = ssl._create_unverified_context

def getAPI(url):
    try:
        req = requests.get(url , auth=HTTPBasicAuth(args.es_user, args.es_password), verify=args.cert_path)
        return json.loads(req.text)
    except:
        print("CRITICAL - Unable to get API url '{}'".format(url))
        sys.exit(2)

def is_number(x):
    return isinstance(x, (int, float, complex))
    #return isinstance(x, (int, long, float, complex))


def check_status(
    value,
    message,
    only_graph=False,
    critical=None,
    warning=None,
    ok=None,
):
    if only_graph:
        print("{}".format(message))
        sys.exit(0)

    if (is_number(value) and is_number(critical) and is_number(warning)):
        if value >= critical:
            print("CRITICAL - {}".format(message))
            sys.exit(2)
        elif value >= warning:
            print("WARNING - {}".format(message))
            sys.exit(1)
        else:
            print("OK - {}".format(message))
            sys.exit(0)
    else:
        if value in critical:
            print("CRITICAL - {}".format(message))
            sys.exit(2)
        elif value in warning:
            print("WARNING - {}".format(message))
            sys.exit(1)
        elif value in ok:
            print("OK - {}".format(message))
            sys.exit(0)
        else:
            print("UNKNOWN - Unexpected value: {}".format(value))
            sys.exit(3)


def parser_command_line():
    parser = argparse.ArgumentParser(
        description='Elasticsearch Nagios checks'
    )
    subparsers = parser.add_subparsers(
        help='All Elasticsearch checks groups',
        dest='subparser_name',
    )

    # Common args
    parser.add_argument(
        '-n',
        '--node-name',
        default='_local',
        help='Node name in the Cluster',
        dest='node_name',
    )

    parser.add_argument(
        '-C',
        '--cert-path',
        default='_local',
        help='Path to Certificate',
        dest='cert_path',
    )

    parser.add_argument(
        '-c',
        '--client-node',
        default='localhost',
        help='Client node name (FQDN) for HTTP communication',
        dest='client_node',
    )

    parser.add_argument(
        '-D',
        '--perf-data',
        action='store_true',
        help='Enable Nagios performance data (Valid for all checks groups)',
        dest='perf_data',
    )

    parser.add_argument(
        '-G',
        '--only-graph',
        action='store_true',
        help='Enable Nagios to print only message',
        dest='only_graph',
    )

    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version='%(prog)s {}'.format(__version__)
    )

    parser.add_argument(
        '-u',
        '--user',
        help='User who has access to elasticsearch',
        dest= 'es_user'
    )
    parser.add_argument(
        '-p',
        '--password',
        help='Password for User who has access to elasticsearch',
        dest= 'es_password'
    )

    # Cluster Checks
    cluster = subparsers.add_parser(
        'cluster',
        help='All Cluster checks',
    )
    cluster.add_argument(
        '--cluster-health',
        action='store_true',
        help='Check the Cluster health (green, yellow, red)',
    )

    # Node Checks
    node = subparsers.add_parser(
        'node',
        help='All Node checks',
    )
    node.add_argument(
        '--heap-used-percent',
        action='store_true',
        help='Check the Heap used percent',
    )
    node.add_argument(
        '--documents-count',
        action='store_true',
        help='Documents on node',
    )
    node.add_argument(
        '--ratio-search-query-time',
        action='store_true',
        help='Ratio search query_time_in_millis/query_total',
    )

    # Indices Checks
    indices = subparsers.add_parser(
        'indices',
        help='All indices checks',
    )
    indices.add_argument(
        '--index',
        default=None,
        help='Index name',
    )
    indices.add_argument(
        '--prefix',
        default=None,
        help='Include only indices beginning with prefix',
    )
    indices.add_argument(
        '--doc-type',
        default=None,
        help='Include only documents with doc-type',
    )
    indices.add_argument(
        '--last-entry',
        action='store_true',
        help='Check last entry in the index. Only for timestamp in UTC',
    )

    return parser.parse_args()


def check_cluster_health(
    result,
    perf_data=None,
    only_graph=False
):
    critical = 'red'
    warning = 'yellow'
    ok = 'green'
    message = 'The cluster health status is {}'.format(result)
    if perf_data:
        lookup = {
            'green': 2,
            'yellow': 1,
            'red': 0,
        }
        message += " | cluster_status={}".format(lookup[result])
    check_status(
        result,
        message,
        only_graph,
        critical,
        warning,
        ok,
    )


def check_heap_used_percent(
    result,
    perf_data=None,
    only_graph=False,
    critical=None,
    warning=None,
):
    critical = critical or 90
    warning = warning or 75
    message = 'The Heap used percent is {}%'.format(result)
    if perf_data:
        message += " | heap_used_percent={}".format(result)
    check_status(
        result,
        message,
        only_graph,
        critical,
        warning,
    )


def check_documents_count(
    result,
    perf_data=None,
    only_graph=False,
    critical=None,
    warning=None,
):
    critical = critical or 0
    warning = warning or 0
    message = 'The documents count is {}'.format(result)
    if perf_data:
        message += " | documents_count={}".format(result)
    check_status(
        result,
        message,
        only_graph,
        critical,
        warning,
    )


def check_ratio_search_query_time(
    result,
    perf_data=None,
    only_graph=False,
    critical=None,
    warning=None,
):
    critical = critical or 0
    warning = warning or 0
    message = 'The ratio query_time_in_millis/query_total is {}'.format(result)
    if perf_data:
        message += " | ratio_search_query_time={}".format(result)
    check_status(
        result,
        message,
        only_graph,
        critical,
        warning,
    )


def check_last_entry(
    result,
    perf_data=None,
    only_graph=False,
    critical=None,
    warning=None,
):
    critical = critical or 120
    warning = warning or 60
    message = 'Last entry {} seconds ago'.format(result)
    if perf_data:
        message += " | seconds={}".format(result)
    check_status(
        result,
        message,
        only_graph,
        critical,
        warning,
    )


def get_indices(url):
    indices_dict = getAPI(url)
    return sorted(indices_dict.keys())

def get_last_timestamp(index):
    API_LAST_ENTRY = 'https://{}:9200/{}/_search?sort=@timestamp:desc&size=1&_source=@timestamp'.format(
        args.client_node,
        args.index
    )
    resultquery = getAPI(API_LAST_ENTRY)
    print(resultquery)
    return datetime.strptime(
        resultquery['hits']['hits'][0]['_source']['@timestamp'].split(".")[0],
        "%Y-%m-%dT%H:%M:%S"
    )

if __name__ == '__main__':
    args = parser_command_line()

    if args.subparser_name == 'cluster':
        API_CLUSTER_HEALTH = 'https://{}:9200/_cluster/health'.format(
            args.client_node
        )

        if args.cluster_health:
            result = getAPI(API_CLUSTER_HEALTH)
            check_cluster_health(
                result['status'],
                args.perf_data,
                args.only_graph,
            )

    if args.subparser_name == 'node':
        API_NODES_STATS = 'https://{}:9200/_nodes/{}/stats'.format(
            args.client_node,
            args.node_name,
        )

        if args.heap_used_percent:
            result = getAPI(API_NODES_STATS)
            node = result["nodes"].values()[0]
            check_heap_used_percent(
                node['jvm']['mem']['heap_used_percent'],
                args.perf_data,
                args.only_graph,
            )

        if args.documents_count:
            result = getAPI(API_NODES_STATS)
            node = result["nodes"].values()[0]
            check_documents_count(
                node['indices']['docs']['count'],
                args.perf_data,
                args.only_graph,
            )

        if args.ratio_search_query_time:
            result = getAPI(API_NODES_STATS)
            node = result["nodes"].values()[0]
            query_time_in_millis = float(
                node['indices']['search']['query_time_in_millis']
            )
            query_total = float(
                node['indices']['search']['query_total']
            )
            ratio = round(
                query_time_in_millis/query_total,
                2
            )
            check_ratio_search_query_time(
                ratio,
                args.perf_data,
                args.only_graph,
            )

    if args.subparser_name == 'indices':
        es = Elasticsearch(host=args.client_node)

        if args.last_entry:
            API_ALIASES = 'https://{}:9200/{}/_alias'

            if args.index:
                pattern = args.index
            elif args.prefix:
                pattern = args.prefix + "*"
            else:
                print("Invalid index name or prefix")
                sys.exit(1)

            index = get_indices(
                API_ALIASES.format(
                    args.client_node,
                    pattern,
                )
            )[-1]
            last_timestamp = get_last_timestamp(
                index=index,
            )
            timedelta = (datetime.utcnow() - last_timestamp).seconds
            check_last_entry(
                timedelta,
                args.perf_data,
                args.only_graph,
            )
