#!/usr/bin/env python
# Copyright 2015 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This is a setup script that will downloaded a test warehouse snapshot and
# deploy it on a remote, CM-managed cluster. Once the data is loaded, it is
# possible to run a subset of the Impala core / exhaustive tests on the
# remote cluster.
#
#   * This script should be executed from a machine that has the Impala
#     development environment set up.
#
#   * The cluster needs to be configured appropriately:
#     - The following services need to be installed:
#       HDFS, YARN, HIVE, IMPALA, MAPREDUCE, KUDU, HBASE, ZOOKEEPER
#     - GPL Extras parcel needs to be installed
#     - Metastore DB SERDE properties PARAM_VALUE needs to be altered to
#       allow for wide tables (See HIVE-1364.)
#     - The hive-warehouse path needs to be /test-warehouse
#
# Usage: remote_data_load.py [options] cm_host
#
#  Options:
#    -h, --help           show this help message and exit
#    --cm_user=CM_USER    Cloudera Manager admin user
#    --cm_pass=CM_PASS    Cloudera Manager admin user password
#    --no-load            Do not try to load the snapshot
#    --test               Run end-to-end tests against cluster.
#    --gateway=GATEWAY    Gateway host to upload the data from. If not set, uses
#                         the CM host as gateway.
#    --ssh_user=SSH_USER  System user on the remote machine with passwordless SSH
#                         configured.
#
import fnmatch
import glob
import logging
import os
import sh
import shutil
import sys
import time

from cm_api.api_client import ApiResource
from functools import wraps
from optparse import OptionParser
from sh import ssh
from tempfile import mkdtemp
from urllib import quote as urlquote


REQUIRED_SERVICES = ['HBASE',
                     'HDFS',
                     'HIVE',
                     'IMPALA',
                     'KUDU',
                     'MAPREDUCE',
                     'YARN',
                     'ZOOKEEPER']

# TODO: It's not currently possible to get the version from the cluster.
# It would be nice to generate this dynamically.
# (v14 happens to be the version that ships with CDH 5.9.x)
CM_API_VERSION = 'v14'

# Impala's data loading and test framework assumes this Hive Warehouse Directory.
# Making this configurable would be an invasive change, and therefore, we prefer to
# re-configure the Hive service via the CM API before loading data and running tests.
HIVE_WAREHOUSE_DIR = "/test-warehouse"

logger = logging.getLogger("remote_data_load")
logger.setLevel(logging.DEBUG)

# Goes to the file
fh = logging.FileHandler("remote_data_load.log")
fh.setLevel(logging.DEBUG)

# Goes to stdout
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


def timing(func):
    """
    A decorator for timing how much time a function takes.

    We can modify this later to do something more intelligent than just logging.
    """
    @wraps(func)
    def wrap(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()

        output = 'TIME: {name}() took: {t:.4f} seconds'
        logger.info(output.format(name=func.__name__, t=(t2-t1)))
        return result
    return wrap


def tee(line):
    """Output wrapper function used by sh to send the stdout or stderr to the
    module's logger."""
    logger.debug(line.strip())


class RemoteDataLoad(object):
    """This is an implementation of the process to load a test-warehouse snapshot on
    a remote CM managed cluster. This script assumes that the warehouse snapshot was
    already downloaded and was either passed in as a parameter, or can be found by
    either inspecting the SNAPSHOT_DIR environment variable, or based on the WORKSPACE
    environment variable on a Jenkins build slave.

    The reason for the additional setup code is that in the local development
    environment it is assumed that $USER is HDFS superuser, which is not the case for
    remote deloyments.
    """

    def __init__(self, cm_host, options):
        logger.info("Starting remote data load...")
        self.options = options
        self.cm_host = cm_host

        # Gateway host can be used if the CM host is not configured as a Hadoop gateway
        self.gateway = options.gateway if options.gateway else cm_host
        self.impala_home = os.environ["IMPALA_HOME"]
        self.api = ApiResource(self.cm_host, username=options.cm_user,
                               password=options.cm_pass)

        # The API returns a list of clusters managed by the CM host. We're assuming
        # that this CM host was set up for the purpose of Impala testing on one
        # cluster, so the list should only have one value.
        self.cluster = self.api.get_all_clusters()[0]
        self.services = self.get_services()

        self.config = self.get_service_client_configurations()
        logger.info("Retrieved service configuration")
        logger.info(str(self.config))
        self.prepare()
        logger.info("IMPALA_HOME: {0}".format(self.impala_home))

    def get_hostname_for_ref(self, host_ref):
        """Translate the HostRef instance into the hostname."""
        return self.api.get_host(host_ref.hostId).hostname

    @staticmethod
    def get_or_default(config):
        return config.value if config.value else config.default

    def get_services(self):
        """Confirm that all services are running, and return service dict."""
        services = dict((s.type, s) for s in self.cluster.get_all_services())

        if set(REQUIRED_SERVICES) != set(services.keys()):
            missing_services = set(REQUIRED_SERVICES) - set(services.keys())
            logger.error("Services not installed: {0}".format(list(missing_services)))
            raise RuntimeError("Cluster not ready.")

        if not all(services[s].serviceState == 'STARTED' for s in services):
            stopped = [s for s in services if services[s].serviceState != "STARTED"]
            logger.error("Not all services started: {0}".format(stopped))
            raise RuntimeError("Cluster not ready.")

        return services

    @timing
    def download_client_config(self, cluster, service):
        """Download the client configuration zip for a particular cluster and service.

        Since cm_api does not provide a way to download the archive we build the URL
        manually and download the file. Once it downloaded the file the archive is
        extracted and its content is copied to the Hadoop configuration directories
        defined by Impala.
        """
        logger.info("Downloading client configuration for {0}".format(service.name))
        url = "http://{0}:7180/api/{1}/clusters/{2}/services/{3}/clientConfig".format(
            self.cm_host, CM_API_VERSION, urlquote(cluster.name), urlquote(service.name))
        path = mkdtemp()
        sh.curl(url, o=os.path.join(path, "clientConfig.zip"), _out=tee, _err=tee)
        current = os.getcwd()
        os.chdir(path)
        sh.unzip("clientConfig.zip")
        for root, _, file_names in os.walk("."):
            for filename in fnmatch.filter(file_names, "*.xml"):
                src = os.path.join(root, filename)
                dst = os.path.join(self.impala_home, "fe", "src", "test", "resources")
                logger.debug("Copying {0} to {1}".format(src, dst))
                shutil.copy(src, dst)
        os.chdir(current)

    # TODO: this may be available in tests/comparison/cluster.py
    def set_hive_warehouse_dir(self, cluster, service):
        logger.info("Setting the Hive Warehouse Dir")
        for service in self.api.get_all_clusters()[0].get_all_services():
            logger.info(service)
            if service.type == "HIVE":
              hive_config = { "hive_warehouse_directory" : HIVE_WAREHOUSE_DIR }
              service.update_config(hive_config)

    # TODO: This functionality should be more generally available to other infrastructure
    # code, rather than being quarantined in this script. See IMPALA-4367.
    @timing
    def get_service_client_configurations(self):
        """Download the client configurations necessary to upload data to the remote
        cluster. Unfortunately, the CM API does not allow downloading it so we have to
        iterate over the services and download the config for all of them.

        In addition, returns an options dictionary with settings required for data loading
        like the HS2 server, Impala hosts, Name node etc.

        Returns:
            A client-configuration dictionary, e.g.:

            {
                'hive_warehouse_directory': '/test-warehouse',
                'hs2': 'impala-test-cluster-1.gce.cloudera.com:10000',
                'impalad': ['impala-test-cluster-4.gce.cloudera.com:21000',
                            'impala-test-cluster-2.gce.cloudera.com:21000',
                            'impala-test-cluster-3.gce.cloudera.com:21000'],
                'metastore': 'impala-test-cluster-1.gce.cloudera.com:9083',
                'namenode': 'impala-test-cluster-1.gce.cloudera.com',
                'namenode_http': 'impala-test-cluster-1.gce.cloudera.com:20101',
                'kudu_master': 'impala-test-cluster-1.gce.cloudera.com'
            }
        """
        # Iterate overs services and find the information we need
        result = {}
        for service_type, service in self.services.iteritems():
            if service_type == "IMPALA":
                roles = service.get_roles_by_type("IMPALAD")
                impalads = []
                for r in roles:
                    rc_config = r.get_config("full")
                    hostname = self.get_hostname_for_ref(r.hostRef)
                    hs2_port = self.get_or_default(rc_config["beeswax_port"])
                    impalads.append("{0}:{1}".format(hostname, hs2_port))
                    result["impalad"] = impalads
            elif service_type == "HBASE":
                self.download_client_config(self.cluster, service)
            elif service_type == "HDFS":
                self.download_client_config(self.cluster, service)
                role = service.get_roles_by_type("NAMENODE")
                config = role[0].get_config("full")
                namenode = self.get_hostname_for_ref(role[0].hostRef)
                result["namenode"] = namenode
                result["namenode_http"] = "{0}:{1}".format(
                    namenode,
                    self.get_or_default(config["dfs_http_port"])
                )
            elif service_type == "HIVE":
                self.set_hive_warehouse_dir(self.cluster, service)
                self.download_client_config(self.cluster, service)
                hs2 = service.get_roles_by_type("HIVESERVER2")[0]
                rc_config = hs2.get_config("full")
                result["hive_warehouse_directory"] = self.get_or_default(
                    service.get_config("full")[0]["hive_warehouse_directory"])
                hostname = self.get_hostname_for_ref(hs2.hostRef)
                result["hs2"] = "{0}:{1}".format(hostname, self.get_or_default(
                    rc_config["hs2_thrift_address_port"]))

                # Get Metastore information
                ms = service.get_roles_by_type("HIVEMETASTORE")[0]
                rc_config = ms.get_config("full")
                result["metastore"] = "{0}:{1}".format(
                    self.get_hostname_for_ref(ms.hostRef),
                    self.get_or_default(rc_config["hive_metastore_port"])
                )
            elif service_type == "KUDU":
                # Service KUDU does not require a client configuration
                result["kudu_master"] = self.cm_host

        return result

    # TODO: This functionality should be more generally available to other infrastructure
    # code, rather than being quarantined in this script. See IMPALA-4367.
    @staticmethod
    def find_snapshot_file(snapshot_dir):
        """Given snapshot_directory, walks the directory tree until it finds a file
        matching the test-warehouse archive pattern."""
        for root, _, file_names in os.walk(snapshot_dir):
            for filename in fnmatch.filter(file_names, "test-warehouse-*-SNAPSHOT.tar.gz"):
                logger.info("Found Snapshot file {0}".format(filename))
                return os.path.join(root, filename)

    @timing
    def prepare(self):
        """Populate the environment of the process with the necessary values.

        In addition, it creates helper objects to run shell and SSH processes.
        """
        # Populate environment with required variables
        os.environ["HS2_HOST_PORT"] = self.config["hs2"]
        os.environ["HDFS_NN"] = self.config["namenode"]
        os.environ["IMPALAD"] = self.config["impalad"][0]
        os.environ["REMOTE_LOAD"] = "1"
        os.environ["HADOOP_USER_NAME"] = "hdfs"
        os.environ["TEST_WAREHOUSE_DIR"] = self.config["hive_warehouse_directory"]
        os.environ["KUDU_MASTER"] = self.config["kudu_master"]

        if self.options.snapshot_file is None:
            if "SNAPSHOT_DIR" in os.environ:
                snapshot_dir = os.environ["SNAPSHOT_DIR"]
            else:
                snapshot_dir = "{0}/testdata/test-warehouse-SNAPSHOT".format(
                    os.getenv("WORKSPACE"))
            if not os.path.isdir(snapshot_dir):
                err_msg = 'Snapshot directory "{0}" is not a valid directory'
                logger.error(err_msg.format(snapshot_dir))
                raise OSError("Could not find test-warehouse snapshot file.")

            logger.info("Snapshot directory: {0}".format(snapshot_dir))
            self.snapshot_file = self.find_snapshot_file(snapshot_dir)
        else:
            self.snapshot_file = self.options.snapshot_file

        # Prepare shortcuts for connecting to remote services
        self.gtw_ssh = ssh.bake("{0}@{1}".format(self.options.ssh_user, self.gateway),
                                "-oStrictHostKeyChecking=no",
                                "-oUserKnownHostsFile=/dev/null",
                                t=True, _out=tee, _err=tee)

        self.beeline = sh.beeline.bake(silent=False, outputformat="csv2", n="impala",
                                       u="jdbc:hive2://{0}/default".format(
                                           self.config["hs2"]))

        self.load_test_warehouse = sh.Command(
            "{0}/testdata/bin/load-test-warehouse-snapshot.sh".format(
                self.impala_home)).bake(
            _out=tee, _err=tee)

        self.create_load_data = sh.Command(
            "{0}/testdata/bin/create-load-data.sh".format(self.impala_home))

        self.main_impalad = self.config["impalad"][0]
        self.impala_shell = sh.Command("impala-shell.sh").bake(i=self.main_impalad,
                                                               _out=tee, _err=tee)

        self.python = sh.Command("impala-python").bake(u=True)
        self.compute_stats = sh.Command(
            "{0}/testdata/bin/compute-table-stats.sh".format(self.impala_home)).bake(
            _out=tee, _err=tee)

    @timing
    def load(self):
        """This method performs the actual data load. First it removes any known artifacts
        from the remote location. Next it drops potentially existing database from the
        Hive Metastore. Now, it invokes the load-test-warehouse-snapshot.sh and
        create-load-data.sh scripts with the appropriate parameters. The most important
        paramters are implicitly passed to the scripts as environment variables pointing
        to the remote HDFS, Hive and Impala.
        """
        exploration_strategy = self.options.exploration_strategy

        logger.info("Removing other databases")
        dblist = self.beeline(e="show databases;", _err=tee).stdout
        database_list = dblist.split()[1:]  # The first element is the header string
        for db in database_list:
            if db.strip() != "default":
                logger.debug("Dropping database %s", db)
                self.impala_shell(q="drop database if exists {0} cascade;".format(db))

        logger.info("Invalidating metadata in Impala")
        self.impala_shell(q="invalidate metadata;")

        logger.info("Removing previous remote {0}".format(
            self.config["hive_warehouse_directory"]))
        r = sh.hdfs.dfs("-rm", "-r", "-f", "{0}".format(
            self.config["hive_warehouse_directory"]))

        logger.info("Expunging HDFS trash")
        r = sh.hdfs.dfs("-expunge")

        logger.info("Uploading test warehouse snapshot")
        self.load_test_warehouse(self.snapshot_file)

        # TODO: We need to confirm that if we change any permissions, that we don't
        # affect any running tests. See IMPALA-4375.
        logger.info("Changing warehouse ownership")
        r = sh.hdfs.dfs("-chown", "-R", "impala:hdfs", "{0}".format(
            self.config["hive_warehouse_directory"]))
        sh.hdfs.dfs("-chmod", "-R", "g+rwx", "{0}".format(
            self.config["hive_warehouse_directory"]))
        sh.hdfs.dfs("-chmod", "1777", "{0}".format(
            self.config["hive_warehouse_directory"]))

        logger.info("Calling create_load_data.sh")
        # The $USER variable is used in the create-load-data.sh script for beeline
        # impersonation.
        new_env = os.environ.copy()
        new_env["LOGNAME"] = "impala"
        new_env["USER"] = "impala"
        new_env["USERNAME"] = "impala"

        # Regardless of whether we are in fact skipping the snapshot load or not,
        # we nonetheless always pass -skip_snapshot_load to create-load-data.sh.
        # This is because we have already loaded the snapshot earlier in this
        # script, so we don't want create-load-data.sh to invoke
        # load-test-warehouse-snapshot.sh again.
        #
        # It would actually be nice to be able to skip the snapshot load, but
        # because of the existing messiness of create-load-data.sh, we can't.
        # This invocation...
        #
        #    $ create-load-data.sh -skip_snapshot_load -exploration_strategy core
        #
        # ...results in this error:
        #
        #    Creating /test-warehouse HDFS directory \
        #    (logging to create-test-warehouse-dir.log)... FAILED
        #    'hadoop fs -mkdir /test-warehouse' failed. Tail of log:
        #    Log for command 'hadoop fs -mkdir /test-warehouse'
        #    mkdir: `/test-warehouse': File exists
        #
        # Similarly, even though we might pass in "core" as the exploration strategy,
        # because we aren't loading a metadata snapshot (i.e., -skip_metadata_load is
        # false), an exhaustive dataload will always be done. This again is the result
        # of logic in create-load-data.sh, which itself ignores the value passed in
        # for -exploration_strategy.
        #
        # See IMPALA-4399: "create-load-data.sh has bitrotted to some extent, and needs
        #                   to be cleaned up"
        create_load_data_args = ["-skip_snapshot_load", "-cm_host", self.cm_host,
                                 "-snapshot_file", self.snapshot_file,
                                 "-exploration_strategy", exploration_strategy]

        self.create_load_data(*create_load_data_args, _env=new_env, _out=tee, _err=tee)

        sh.hdfs.dfs("-chown", "-R", "impala:hdfs", "{0}".format(
            self.config["hive_warehouse_directory"]))

        logger.info("Re-load HBase data")
        # Manually load the HBase data last.
        self.python("{0}/bin/load-data.py".format(self.impala_home),
                    "--hive_warehouse_dir={0}".format(
                        self.config["hive_warehouse_directory"]),
                    "--table_formats=hbase/none",
                    "--hive_hs2_hostport={0}".format(self.config["hs2"]),
                    "--hdfs_namenode={0}".format(self.config["namenode"]),
                    "--exploration_strategy={0}".format(exploration_strategy),
                    workloads="functional-query",
                    force=True,
                    impalad=self.main_impalad,
                    _env=new_env,
                    _out=tee,
                    _err=tee)

        self.compute_stats()
        logger.info("Load data finished")

    # TODO: Should this be refactored out of this script? It has nothing to do with
    # data loading per se. If tests rely on the environment on the client being set
    # a certain way -- as in the prepare() method -- we may need to find another way
    # to deal with that. See IMPALA-4376.
    @timing
    def test(self):
        """Execute Impala's end-to-end tests against a remote cluster. All configuration
        paramters are picked from the cluster configuration that was fetched via the
        CM API."""

        # TODO: Running tests via runtest.py is currently not working against a remote
        # cluster (although running directly via py.test seems to work.) This method
        # may be refactored out of this file under IMPALA-4376, so for the time being,
        # raise a NotImplementedError.
        raise NotImplementedError

        # Overwrite the username to match the service user on the remote system and deal
        # with the assumption that in the local development environment the current user
        # is HDFS superuser as well.
        new_env = os.environ.copy()
        new_env["LOGNAME"] = "impala"
        new_env["USER"] = "impala"
        new_env["USERNAME"] = "impala"

        strategy = self.options.exploration_strategy
        logger.info("Running tests with exploration strategy {0}".format(strategy))
        run_tests = sh.Command("{0}/tests/run-tests.py".format(self.impala_home))
        run_tests("--skip_local_tests",
                  "--exploration_strategy={0}".format(strategy),
                  "--workload_exploration_strategy=functional-query:{0}".format(strategy),
                  "--namenode_http_address={0}".format(self.config["namenode_http"]),
                  "--hive_server2={0}".format(self.config["hs2"]),
                  "--metastore_server={0}".format(self.config["metastore"]),
                  "query_test",
                  maxfail=10,
                  impalad=",".join(self.config["impalad"]),
                  _env=new_env,
                  _out=tee,
                  _err=tee)


def parse_args():
    parser = OptionParser()
    parser.add_option("--snapshot-file", default=None,
                      help="Path to the test-warehouse archive")
    parser.add_option("--cm-user", default="admin", help="Cloudera Manager admin user")
    parser.add_option("--cm-pass", default="admin",
                      help="Cloudera Manager admin user password")
    parser.add_option("--gateway", default=None,
                      help=("Gateway host to upload the data from. If not set, uses the "
                            "CM host as gateway."))
    parser.add_option("--ssh-user", default="jenkins",
                      help=("System user on the remote machine with passwordless "
                            "SSH configured."))
    parser.add_option("--no-load", action="store_false", default=True, dest="load",
                      help="Do not try to load the snapshot")
    parser.add_option("--exploration-strategy", default="core")
    parser.add_option("--test", action="store_true", default=False,
                      help="Run end-to-end tests against cluster")
    parser.set_usage("remote_data_load.py [options] cm_host")

    options, args = parser.parse_args()

    try:
        return options, args[0]  # args[0] is the cm_host
    except IndexError:
        logger.error("You must supply the cm_host.")
        parser.print_usage()
        raise

def main(cm_host, options):
    """
    Load data to a remote cluster (and/or run tests) as specified.

    Args:
        cm_host: FQDN or IP of the CM host machine
        options: an optparse 'options' instance containing RemoteDataLoad
                 values (though any object with the correct .attributes, e.g.
                 a collections.namedtuple instance, would also work)
    """
    rd = RemoteDataLoad(cm_host, options)

    if options.load:
        rd.load()
    if options.test:
        rd.test()


if __name__ == "__main__":
    options, cm_host = parse_args()
    main(cm_host=cm_host, options=options)
