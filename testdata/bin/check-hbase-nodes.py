#!/usr/bin/env impala-python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Given a series of hosts and Zookeeper nodes, make sure that each node is accessible.
"""

from __future__ import absolute_import, division, print_function
import argparse
import hdfs
import logging
import pprint
import requests
import sys
import time

from contextlib import closing
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, ConnectionLoss
from kazoo.handlers.threading import KazooTimeoutError

LOGGER = logging.getLogger('hbase_check')
LOGGER.addHandler(logging.StreamHandler())
LOGGER.setLevel(logging.INFO)

TIMEOUT_SECONDS = 30

HDFS_HOST = '127.0.0.1:5070'
ZK_HOSTS = '127.0.0.1:2181'
HBASE_NODES = ['/hbase/master', '/hbase/rs']
ADMIN_USER = 'admin'
MAX_ZOOKEEPER_CONNECTION_RETRIES = 3


def parse_args():
    """Parse and return command line args."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--timeout', '-t', type=int, default=TIMEOUT_SECONDS,
                        help=('Number of seconds to try to get znode before giving up. '
                              'Default is {0} seconds.'.format(TIMEOUT_SECONDS)))

    parser.add_argument('--hdfs_host', '-s', default=HDFS_HOST,
                        help=('Host:port where the HDFS web host is running, '
                              'e.g, 0.0.0.0:5070. Default is {0}.'.format(HDFS_HOST)))

    parser.add_argument('--admin_user', '-u', default=ADMIN_USER,
                        help='Cluster admin username. Default is {0}.'.format(ADMIN_USER))

    parser.add_argument('--zookeeper_hosts', '-z', default=ZK_HOSTS,
                        help=('Comma-delineated string of hosts in host:PORT format, '
                              'e.g, 0.0.0.0:2181. Default is {0}.'.format(ZK_HOSTS)))

    parser.add_argument('-node', '-n', action='append', dest='nodes',
                        default=HBASE_NODES,
                        help=('HBase znode to check. Can be specified multiple times. '
                              'Defaults are -n {0}.'.format(' -n '.join(HBASE_NODES))))
    return parser.parse_args()


def connect_to_zookeeper(host_list, timeout_seconds):
    """Connect to Zookeeper service.

    Args:
        host_list: Comma-separated string of hosts in host:port format
        timeout_seconds: Number of seconds to attempt to connect to host

    Returns:
        KazooClient instance
    """
    zk_client = KazooClient(hosts=host_list)

    try:
        LOGGER.info("Connecting to Zookeeper host(s).")
        zk_client.start(timeout=timeout_seconds)
        LOGGER.info("Success: " + str(zk_client))
        return zk_client
    except KazooTimeoutError as e:
        LOGGER.error("Could not connect to Zookeeper: " + str(e))
        sys.exit(1)


def check_znode(node, zk_client, timeout_seconds):
    """Given a Zookeeper client and a node, check that the node is up.

    Args:
        node: name of a znode as a string, e.g., /hbase/rs
        zk_client: Zookeeper client object
        timeout_seconds: Number of seconds to attempt to get node

    Returns:
        0 success, 1 on failure
    """
    start_time = time.time()
    while (time.time() - start_time) < timeout_seconds:
        LOGGER.info("Waiting for HBase node: " + node)
        try:
            node_info = zk_client.get(node)
            LOGGER.info("Success: " + node)
            LOGGER.debug(pprint.pformat(node_info))
            return 0
        except NoNodeError:
            time.sleep(1)

    LOGGER.error("Failed while checking for HBase node: " + node)
    return 1


def check_znodes_list_for_errors(nodes, zookeeper_hosts, timeout):
    """Confirm that the given list of znodes are responsive.

    Args:
        zk_client: Zookeeper client object
        node: name of a znode as a string, e.g., /hbase/rs
        timeout_seconds: Number of seconds to attempt to get node

    Returns:
        0 success, or else the number of errors
    """
    connection_retries = 0

    while True:
        with closing(connect_to_zookeeper(zookeeper_hosts, timeout)) as zk_client:
            try:
                return sum([check_znode(node, zk_client, timeout) for node in nodes])
            except ConnectionLoss as e:
                connection_retries += 1
                if connection_retries > MAX_ZOOKEEPER_CONNECTION_RETRIES:
                    LOGGER.error("Max connection retries exceeded: {0}".format(str(e)))
                    raise
                else:
                    err_msg = ("Zookeeper connection loss: retrying connection "
                               "({0} of {1} attempts)")
                    LOGGER.warn(err_msg.format(connection_retries,
                                               MAX_ZOOKEEPER_CONNECTION_RETRIES))
                    time.sleep(1)
            except Exception as e:
                LOGGER.error("Unexpected error checking HBase node: {0}".format(str(e)))
                raise
            finally:
                LOGGER.info("Stopping Zookeeper client")
                zk_client.stop()


def is_hdfs_running(host, admin_user):
    """Confirm that HDFS is available.

    There is a pathological case where the HBase master can start up briefly if HDFS is not
    available, and then quit immediately, but that can be long enough to give a false positive
    that the HBase master is running.

    Args:
        host: HDFS host:port
        admin_user: Admin username

    Returns:
        Boolean
    """
    try:
        hdfs_client = hdfs.InsecureClient('http://' + host, user=admin_user)
        LOGGER.info("Contents of HDFS root: {0}".format(hdfs_client.list('/')))
        return True
    except (requests.exceptions.ConnectionError, hdfs.util.HdfsError) as e:
        msg = 'Could not confirm HDFS is running at http://{0} - {1}'.format(host, e)
        LOGGER.error(msg)
        return False


if __name__ == "__main__":
    args = parse_args()

    if is_hdfs_running(args.hdfs_host, args.admin_user):
        errors = check_znodes_list_for_errors(args.nodes, args.zookeeper_hosts, args.timeout)

        if errors > 0:
            msg = "Could not get one or more nodes. Exiting with errors: {0}".format(errors)
            LOGGER.error(msg)
            sys.exit(errors)
    else:
        sys.exit(1)
