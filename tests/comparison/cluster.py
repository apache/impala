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

"""This module provides utilities for interacting with a cluster."""

# This should be moved into the test/util folder eventually. The problem is this
# module depends on db_connection which use some query generator classes.

from __future__ import absolute_import, division, print_function
from builtins import int, range, zip
from future.utils import with_metaclass
import hdfs
import logging
import os
import requests
import shutil
import subprocess
from abc import ABCMeta, abstractproperty
from cm_api.api_client import ApiResource as CmApiResource
from collections import defaultdict
from collections import OrderedDict
from contextlib import contextmanager
from getpass import getuser
from io import BytesIO
from multiprocessing.pool import ThreadPool
from random import choice
from sys import maxsize
from tempfile import mkdtemp
from threading import Lock
from time import mktime, strptime
from xml.etree.ElementTree import parse as parse_xml
from zipfile import ZipFile

try:
  from urllib.parse import urlparse
except ImportError:
  from urlparse import urlparse

from tests.comparison.db_connection import HiveConnection, ImpalaConnection
from tests.common.environ import HIVE_MAJOR_VERSION
from tests.common.errors import Timeout
from tests.util.shell_util import shell as local_shell
from tests.util.parse_util import parse_glog, parse_mem_to_mb

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

DEFAULT_HIVE_HOST = '127.0.0.1'
DEFAULT_HIVE_PORT = 11050
DEFAULT_HIVE_USER = 'hive'
DEFAULT_HIVE_PASSWORD = 'hive'

DEFAULT_TIMEOUT = 300

CM_CLEAR_PORT = 7180
CM_TLS_PORT = 7183


class Cluster(with_metaclass(ABCMeta, object)):
  """This is a base class for clusters. Cluster classes provide various methods for
     interacting with a cluster. Ideally the various cluster implementations provide
     the same set of methods so any cluster implementation can be chosen at runtime.
  """

  def __init__(self):
    self._hadoop_configs = None
    self._local_hadoop_conf_dir = None
    self.hadoop_user_name = getuser()
    self.use_kerberos = False
    self.use_ssl = False
    self.ca_cert = None

    self._hdfs = None
    self._yarn = None
    self._hive = None
    self._impala = None

  def _load_hadoop_config(self):
    if not self._hadoop_configs:
      self._hadoop_configs = dict()
      for file_name in os.listdir(self.local_hadoop_conf_dir):
        if not file_name.lower().endswith(".xml"):
          continue
        xml_doc = parse_xml(os.path.join(self.local_hadoop_conf_dir, file_name))
        for property in xml_doc.getiterator("property"):
          name = property.find("name")
          if name is None or name.text is None:
            continue
          value = property.find("value")
          if value is None or value.text is None:
            continue
          self._hadoop_configs[name.text] = value.text

  def get_hadoop_config(self, key, default=None):
    """Returns the Hadoop Configuration value mapped to the given key. If a default is
       specified, it is returned if the key is cannot be found. If no default is specified
       and the key cannot be found, a 'No Such Key' error will be thrown.
    """
    self._load_hadoop_config()
    result = self._hadoop_configs.get(key, default)
    if result is None:
      raise KeyError
    return result

  @abstractproperty
  def shell(self, cmd, host_name, timeout_secs=DEFAULT_TIMEOUT):
    """Execute the shell command 'cmd' on the host 'host_name' and return the output.
       If the command does not complete before 'timeout_secs' an Timeout exception will
       be raised.
    """
    pass

  @abstractproperty
  def _init_local_hadoop_conf_dir():
    """Prepare a single directory that contains all hadoop configs and set
       '_local_hadoop_conf_dir' to the location of the dir.
    """
    pass

  @property
  def local_hadoop_conf_dir(self):
    if not self._local_hadoop_conf_dir:
      self._init_local_hadoop_conf_dir()
    return self._local_hadoop_conf_dir

  @abstractproperty
  def _init_hdfs():
    pass

  @property
  def hdfs(self):
    if not self._hdfs:
      self._init_hdfs()
    return self._hdfs

  def _init_yarn(self):
    self._yarn = Yarn(self)

  @property
  def yarn(self):
    if not self._yarn:
      self._init_yarn()
    return self._yarn

  @abstractproperty
  def _init_hive():
    pass

  @property
  def hive(self):
    if not self._hive:
      self._init_hive()
    return self._hive

  @abstractproperty
  def _init_impala():
    pass

  @property
  def impala(self):
    if not self._impala:
      self._init_impala()
    return self._impala

  def print_version(self):
    """
    Print the cluster impalad version info to the console sorted by hostname.
    """
    def _sorter(i1, i2):
      return cmp(i1.host_name, i2.host_name)

    version_info = self.impala.get_version_info()
    print("Cluster Impalad Version Info:")
    for impalad in sorted(version_info.keys(), cmp=_sorter):
      print("{0}: {1}".format(impalad.host_name, version_info[impalad]))


class MiniCluster(Cluster):

  def __init__(self, hive_host=DEFAULT_HIVE_HOST, hive_port=DEFAULT_HIVE_PORT,
               num_impalads=3):
    Cluster.__init__(self)
    self.hive_host = hive_host
    self.hive_port = hive_port
    self.num_impalads = num_impalads

  def shell(self, cmd, unused_host_name, timeout_secs=DEFAULT_TIMEOUT):
    return local_shell(cmd, timeout_secs=timeout_secs)

  def _init_local_hadoop_conf_dir(self):
    self._local_hadoop_conf_dir = mkdtemp()

    node_conf_dir = self._get_node_conf_dir()
    for file_name in os.listdir(node_conf_dir):
      shutil.copy(os.path.join(node_conf_dir, file_name), self._local_hadoop_conf_dir)

    other_conf_dir = self._get_other_conf_dir()
    for file_name in ["hive-site.xml"]:
      shutil.copy(os.path.join(other_conf_dir, file_name), self._local_hadoop_conf_dir)

  def _get_node_conf_dir(self):
    return os.path.join(os.environ["IMPALA_CLUSTER_NODES_DIR"],
                        "node-1", "etc", "hadoop", "conf")

  def _get_other_conf_dir(self):
    return os.path.join(os.environ["IMPALA_HOME"], "fe", "src", "test",
                        "resources")

  def _init_hdfs(self):
    self._hdfs = Hdfs(self, self.hadoop_user_name)

  def _init_hive(self):
    self._hive = Hive(self, self.hive_host, self.hive_port)

  def _init_impala(self):
    hs2_base_port = 21050
    web_ui_base_port = 25000
    impalads = [MiniClusterImpalad(hs2_base_port + p, web_ui_base_port + p)
                for p in range(self.num_impalads)]
    self._impala = Impala(self, impalads)

class MiniHiveCluster(MiniCluster):
  """
  A MiniCluster useful for running against Hive. It allows Hadoop configuration files
  to be specified by HADOOP_CONF_DIR and Hive configuration files to be specified by
  HIVE_CONF_DIR.
  """

  def __init__(self, hive_host=DEFAULT_HIVE_HOST, hive_port=DEFAULT_HIVE_PORT):
    MiniCluster.__init__(self)
    self.hive_host = hive_host
    self.hive_port = hive_port

  def _get_node_conf_dir(self):
    return os.environ["HADOOP_CONF_DIR"]

  def _get_other_conf_dir(self):
    return os.environ["HIVE_CONF_DIR"]


class CmCluster(Cluster):

  def __init__(self, host_name, port=None, user="admin", password="admin",
               cluster_name=None, ssh_user=None, ssh_port=None, ssh_key_file=None,
               use_tls=False):
    # Initialize strptime() to workaround https://bugs.python.org/issue7980. Apparently
    # something in the CM API uses strptime().
    strptime("2015", "%Y")

    Cluster.__init__(self)
    # IMPALA-5455: If the caller doesn't specify port, default it based on use_tls
    if port is None:
      if use_tls:
        port = CM_TLS_PORT
      else:
        port = CM_CLEAR_PORT
    self.cm = CmApiResource(host_name, server_port=port, username=user, password=password,
                            use_tls=use_tls)
    clusters = self.cm.get_all_clusters()
    if not clusters:
      raise Exception("No clusters found in CM at %s" % host_name)
    if cluster_name:
      clusters_by_name = dict((c.name, c) for c in clusters)
      if cluster_name not in clusters_by_name:
        raise Exception(("No clusters named %s found in CM at %s."
            "Available clusters are %s.")
            % (cluster_name, host_name, ", ".join(sorted(clusters_by_name.keys()))))
      self.cm_cluster = clusters_by_name[cluster_name]
    else:
      if len(clusters) > 1:
        raise Exception(("Too many clusters found in CM at %s;"
            " a cluster name must be provided")
            % host_name)
      self.cm_cluster = clusters[-1]

    self.ssh_user = ssh_user
    self.ssh_port = ssh_port
    self.ssh_key_file = ssh_key_file
    self._ssh_client_lock = Lock()
    self._ssh_clients_by_host_name = defaultdict(list)

  def shell(self, cmd, host_name, timeout_secs=DEFAULT_TIMEOUT):
    with self._ssh_client(host_name) as client:
      return client.shell(cmd, timeout_secs=timeout_secs)

  @contextmanager
  def _ssh_client(self, host_name):
    """Returns an SSH client for use in a 'with' block. When the 'with' context exits,
       the client will be kept for reuse.
    """
    with self._ssh_client_lock:
      clients = self._ssh_clients_by_host_name[host_name]
      if clients:
        client = clients.pop()
      else:
        # IMPALA-7460: Insulate this import away from the global context so as to avoid
        # requiring Paramiko unless it's absolutely needed.
        from tests.util.ssh_util import SshClient
        LOG.debug("Creating new SSH client for %s", host_name)
        client = SshClient()
        client.connect(host_name, username=self.ssh_user, key_filename=self.ssh_key_file)
    error_occurred = False
    try:
      yield client
    except Exception:
      error_occurred = True
      raise
    finally:
      if not error_occurred:
        with self._ssh_client_lock:
          self._ssh_clients_by_host_name[host_name].append(client)

  def _init_local_hadoop_conf_dir(self):
    self._local_hadoop_conf_dir = mkdtemp()
    data = BytesIO(self.cm.get("/clusters/%s/services/%s/clientConfig"
      % (self.cm_cluster.name, self._find_service("HIVE").name)))
    zip_file = ZipFile(data)
    for name in zip_file.namelist():
      if name.endswith("/"):
        continue
      extract_path = os.path.join(self._local_hadoop_conf_dir, os.path.basename(name))
      with open(extract_path, "w") as conf_file:
        conf_file.write(zip_file.open(name).read())

  def _find_service(self, service_type):
    """Find a service by its CM API service type. An exception will be raised if no
       service is found or multiple services are found. See the CM API documentation for
       more details about the service type.
    """
    services = [s for s in self.cm_cluster.get_all_services() if s.type == service_type]
    if not services:
      raise Exception("No service of type %s found in cluster %s"
          % (service_type, self.cm_cluster.name))
    if len(services) > 1:
      raise Exception("Found %s services in cluster %s; only one is expected."
        % len(services, self.cm_cluster.name))
    return services[0]

  def _find_role(self, role_type, service_type):
    """Find a role by its CM API role and service type. An exception will be raised if
       no roles are found. See the CM API documentation for more details about the
       service and role types.
    """
    service = self._find_service(service_type)
    roles = service.get_roles_by_type(role_type)
    if not roles:
      raise Exception("No roles of type %s found in service %s"
          % (role_type, service.name))
    return roles[0]

  def _init_hdfs(self):
    self._hdfs = Hdfs(self, "hdfs")

  def _init_hive(self):
    hs2 = self._find_role("HIVESERVER2", "HIVE")
    host = self.cm.get_host(hs2.hostRef.hostId)
    config = hs2.get_config(view="full")["hs2_thrift_address_port"]
    self._hive = Hive(self, str(host.hostname), int(config.value or config.default))

  def _init_impala(self):
    self._impala = CmImpala(self, self._find_service("IMPALA"))


class Service(object):
  """This is a base class for cluster services such as HDFS. Service classes will provide
     an interface for interacting with the service.
  """

  def __init__(self, cluster):
    self.cluster = cluster


class Hdfs(Service):

  def __init__(self, cluster, admin_user_name):
    self.cluster = cluster
    self._admin_user_name = admin_user_name

  def create_client(self, as_admin=False):
    """Returns an HdfsClient."""
    endpoint = self.cluster.get_hadoop_config("dfs.namenode.http-address",
                                              "0.0.0.0:50070")
    ip, port = endpoint.split(':')
    if ip == "0.0.0.0":
      ip = "127.0.0.1"
    if self.cluster.use_ssl:
      port = self.cluster.get_hadoop_config("dfs.https.port", 20102)
      scheme = 'https'
    else:
      scheme = 'http'
    endpoint = ':'.join([ip, port])
    return HdfsClient(
        "{scheme}://{endpoint}".format(scheme=scheme, endpoint=endpoint),
        use_kerberos=self.cluster.use_kerberos,
        user_name=(self._admin_user_name if as_admin else self.cluster.hadoop_user_name))

  def ensure_home_dir(self, user=None):
    """Creates the home dir for 'user' if needed. If 'user' is not provided,
       'hadoop_user_name' will be used from the cluster.
    """
    if not user:
      user = self.cluster.hadoop_user_name
    client = self.create_client(as_admin=True)
    hdfs_dir = "/user/%s" % user
    if not client.exists(hdfs_dir):
      client.makedirs(hdfs_dir)
      client.set_owner(hdfs_dir, owner=user)


class HdfsClient(object):

  def __init__(self, url, user_name=None, use_kerberos=False):
    # Set a specific session that doesn't verify SSL certs. This is needed because
    # requests doesn't like self-signed certs.
    # TODO: Support a CA bundle.
    s = requests.Session()
    s.verify = False
    if use_kerberos:
      try:
        from hdfs.ext.kerberos import KerberosClient
      except ImportError as e:
        if "No module named requests_kerberos" not in str(e):
          raise e
        import os
        import subprocess
        LOG.info("kerberos module not found; attempting to install it...")
        pip_path = os.path.join(os.environ["IMPALA_HOME"], "infra", "python", "env",
            "bin", "pip")
        try:
          local_shell(pip_path + " install pykerberos==1.1.14 requests-kerberos==0.11.0",
                      stdout=subprocess.PIPE,
                      stderr=subprocess.STDOUT)
          LOG.info("kerberos installation complete.")
        except Exception as e:
          LOG.error("kerberos installation failed. Try installing libkrb5-dev and"
              " then try again.")
          raise e
      from hdfs.ext.kerberos import KerberosClient
      self._client = KerberosClient(url, session=s)
    else:
      self._client = hdfs.client.InsecureClient(url, user=user_name, session=s)

  def __getattr__(self, name):
    return getattr(self._client, name)

  def exists(self, path):
    """Evaluates to True if the given 'path' exists."""
    return self.status(path, strict=False)


class Yarn(Service):

  @staticmethod
  def find_mr_streaming_jar():
    jar_path = None
    for path, _, file_names in os.walk(os.environ["HADOOP_HOME"]):
      for file_name in file_names:
        lc_file_name = file_name.lower()
        if not lc_file_name.endswith("jar"):
          continue
        if "streaming" not in lc_file_name:
          continue
        if "sources" in lc_file_name:
          continue
        if jar_path:
          raise Exception("Found multiple 'streaming' jars: %s and %s"
              % (jar_path, os.path.join(path, file_name)))
        jar_path = os.path.join(path, file_name)
    return jar_path

  def run_mr_job(self, jar_path, job_args=''):
    """Runs the MR job specified by the 'jar_path' and 'job_args' and blocks until
       completion.
    """
    env = dict(os.environ)
    env['HADOOP_CONF_DIR'] = self.cluster.local_hadoop_conf_dir
    env['CDH_MR2_HOME'] =  os.environ['HADOOP_HOME']
    env['HADOOP_USER_NAME'] =  self.cluster.hadoop_user_name
    local_shell('hadoop jar %s %s' % (jar_path, job_args), stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, env=env)


class Hive(Service):

  def __init__(self, cluster, hs2_host_name, hs2_port):
    Service.__init__(self, cluster)
    self.hs2_host_name = hs2_host_name
    self.hs2_port = hs2_port
    self._warehouse_dir = None

  @property
  def warehouse_dir(self):
    if not self._warehouse_dir:
      # Starting in Hive 3, there is a separate directory for external tables. Since
      # all non-transactional tables are external tables with HIVE-22158, most of the
      # test tables are located here. To avoid disruption, we use this as the warehouse
      # directory. Hive 2 doesn't have this distinction and is unchanged.
      if HIVE_MAJOR_VERSION > 2:
        self._warehouse_dir = urlparse(
          self.cluster.get_hadoop_config("hive.metastore.warehouse.external.dir")).path
      else:
        self._warehouse_dir = urlparse(
          self.cluster.get_hadoop_config("hive.metastore.warehouse.dir")).path
    return self._warehouse_dir

  def connect(self, db_name=None):
    conn = HiveConnection(
        host_name=self.hs2_host_name,
        port=self.hs2_port,
        user_name=self.cluster.hadoop_user_name,
        db_name=db_name,
        use_kerberos=self.cluster.use_kerberos,
        use_ssl=self.cluster.use_ssl,
        ca_cert=self.cluster.ca_cert,
    )
    conn.cluster = self.cluster
    return conn

  @contextmanager
  def cursor(self, db_name=None):
    with self.connect(db_name=db_name) as conn:
      with conn.cursor() as cur:
        yield cur


class Impala(Service):
  """This class represents an Impala service running on a cluster. The class is intended
     to help with basic tasks such as connecting to an impalad or checking if queries
     are running.
  """

  def __init__(self, cluster, impalads):
    Service.__init__(self, cluster)
    self.impalads = impalads
    for i in impalads:
      i.impala = self

    self._thread_pool = ThreadPool()

  @property
  def warehouse_dir(self):
    return self.cluster.hive.warehouse_dir

  def connect(self, db_name=None, impalad=None):
    if not impalad:
      impalad = choice(self.impalads)
    conn = ImpalaConnection(
        host_name=impalad.host_name,
        port=impalad.hs2_port,
        user_name=self.cluster.hadoop_user_name,
        db_name=db_name,
        use_kerberos=self.cluster.use_kerberos,
        use_ssl=self.cluster.use_ssl,
        ca_cert=self.cluster.ca_cert,
    )
    conn.cluster = self.cluster
    return conn

  @contextmanager
  def cursor(self, db_name=None, impalad=None):
    with self.connect(db_name=db_name, impalad=impalad) as conn:
      with conn.cursor() as cur:
        yield cur

  def find_stopped_impalads(self):
    stopped = list()
    for idx, pid in enumerate(self.for_each_impalad(lambda i: i.find_pid())):
      if not pid:
        stopped.append(self.impalads[idx])
    return stopped

  def find_and_set_path_to_running_impalad_binary(self):
    self.for_each_impalad(lambda i: i.find_and_set_path_to_running_binary())

  def cancel_queries(self):
    self.for_each_impalad(lambda i: i.cancel_queries())

  def get_version_info(self):
    return self.for_each_impalad(lambda i: i.get_version_info(), as_dict=True)

  def queries_are_running(self):
    return any(self.for_each_impalad(lambda i: i.queries_are_running()))

  def find_impalad_mem_mb_limit(self):
    return self.for_each_impalad(lambda i: i.find_process_mem_mb_limit())

  def find_impalad_mem_mb_reported_usage(self):
    return self.for_each_impalad(
        lambda i: i.find_reported_mem_mb_usage())

  def find_impalad_mem_mb_actual_usage(self):
    return self.for_each_impalad(lambda i: i.find_actual_mem_mb_usage())

  def find_crashed_impalads(self, start_time):
    """If any impalads are found not running, they will assumed to have crashed. A crash
       info message will be return for each stopped impalad. The return value is a dict
       keyed by impalad. See Impalad.find_last_crash_message() for info about the returned
       messages. 'start_time' is used to filter log messages and core dumps, it should
       be set to the time when the Impala service was started. Impalads that have
       non-generic crash info will be sorted last in the returned dict.
    """
    stopped_impalads = self.find_stopped_impalads()
    if not stopped_impalads:
      return dict.fromkeys(stopped_impalads)
    messages = OrderedDict()
    impalads_with_message = dict()
    for i, message in zip(stopped_impalads, self.for_each_impalad(
        lambda i: i.find_last_crash_message(start_time), impalads=stopped_impalads)):
      if message:
        impalads_with_message[i] = "%s crashed:\n%s" % (i.host_name, message)
      else:
        messages[i] = "%s crashed but no info could be found" % i.host_name
    messages.update(impalads_with_message)
    return messages

  def for_each_impalad(self, func, impalads=None, as_dict=False):
    if impalads is None:
      impalads = self.impalads
    promise = self._thread_pool.map_async(func, impalads)
    # Python doesn't handle ctrl-c well unless a timeout is provided.
    results = promise.get(maxsize)
    if as_dict:
      results = dict(zip(impalads, results))
    return results

  def restart(self):
    raise NotImplementedError()


class CmImpala(Impala):

  def __init__(self, cluster, cm_api):
    super(CmImpala, self).__init__(cluster,
      [CmImpalad(i) for i in cm_api.get_roles_by_type("IMPALAD")])
    self._api = cm_api

  def restart(self):
    LOG.info("Restarting Impala")
    command = self._api.restart()
    command = command.wait(timeout=(60 * 15))
    if command.active:
      raise Timeout("Timeout waiting for Impala to restart")
    if not command.success:
      raise Exception("Failed to restart Impala: %s" % command.resultMessage)


class Impalad(with_metaclass(ABCMeta, object)):

  def __init__(self):
    self.impala = None
    self.bin_path = None

  @property
  def cluster(self):
    return self.impala.cluster

  @abstractproperty
  def host_name(self):
    pass

  @abstractproperty
  def web_ui_port(self):
    pass

  @property
  def label(self):
    return self.host_name

  def is_running(self):
    return self.find_pid() is not None

  def is_stopped(self):
    return not self.is_running

  def find_running_queries(self):
    return self._read_web_page("/queries")["in_flight_queries"]

  def queries_are_running(self):
    return bool(self.find_running_queries())

  def cancel_queries(self):
    for data in self.find_running_queries():
      self.cancel_query(data["query_id"])

  def cancel_query(self, id):
    try:
      self._request_web_page("/cancel_query", params={"query_id": id})
    except requests.exceptions.HTTPError as e:
      # TODO: Handle losing the race
      raise e

  def get_version_info(self):
    with self.cluster.impala.cursor(impalad=self) as cursor:
      cursor.execute("SELECT version()")
      return ''.join(cursor.fetchone()).strip()

  def shell(self, cmd, timeout_secs=DEFAULT_TIMEOUT):
    return self.cluster.shell(cmd, self.host_name, timeout_secs=timeout_secs)

  def find_and_set_path_to_running_binary(self):
    LOG.info("Finding impalad binary location")
    self.bin_path = self.shell("""
        PID=$(pgrep impalad | head -1 || true)
        if [[ -z "$PID" ]]; then
          echo Could not find a running impalad >&2
          exit 1
        fi
        cat /proc/$PID/cmdline""").split(b"\0")[0]

  def find_last_crash_message(self, start_time):
    """Returns a string with various info (backtrace and log messages) if any is found."""
    fatal_log = self.parse_fatal_log(start_time)
    if fatal_log:
      message = "\n".join(fatal_log)
    else:
      message = None
    bt = self.find_last_backtrace(start_time)
    if bt:
      if message:
        message += "\n" + bt
      else:
        message = bt
    return message

  def find_last_backtrace(self, start_time):
    """Finds the most recent core file older than 'start_time' and returns the extracted
       back trace. If no core file could be found, the returned value evaluates to False.
    """
    if self.bin_path is None:
      raise Exception("set_path_to_running_binary() must be called before attempting"
          " to find a backtrace")
    bt = self.shell("""
        LAST_CORE_FILE=$(
            find "{core_dump_dir}" -maxdepth 1 -name "*core*" -printf "%T@ %p\\n" \\
                | sort -n | tail -1 | cut -f 1 -d ' ' --complement)
        if [[ -n "$LAST_CORE_FILE" ]]; then
          MTIME=$(stat -c %Y "$LAST_CORE_FILE")
          if [[ "$MTIME" -ge {start_time_unix} ]]; then
            sudo gdb "{bin_path}" "$LAST_CORE_FILE" --batch --quiet --eval-command=bt
          fi
        fi"""
        .format(core_dump_dir=self.find_core_dump_dir(),
            start_time_unix=int(mktime(start_time.timetuple())),
            bin_path=self.bin_path))
    lines = list()
    prev_line = None
    found_start = False
    found_impala_start = False
    for line in bt.split("\n"):
      if not found_start:
        found_start = line.startswith("#0  0x")
      elif not found_impala_start and "impala" in line:
        found_impala_start = True
        lines.append("[...skipped...]")
        lines.append(prev_line)
      if found_impala_start:
        lines.append(line)
      prev_line = line
    if bt and not found_impala_start:
      return bt
    return "\n".join(lines)

  def parse_fatal_log(self, start_time):
    log = self.shell("""
        if [[ -e /var/log/impalad/impalad.FATAL ]]; then
          cat /var/log/impalad/impalad.FATAL
        fi""")
    return parse_glog(log, start_time)

  def find_reported_mem_mb_usage(self):
    """Returns the amount of memory this impalad is using as reported by the impalad (
       the mem tracker consumption).
    """
    data = self._read_web_page("/memz")["consumption"].split()
    return parse_mem_to_mb(data[0], data[1] if len(data) == 2 else "")

  def find_actual_mem_mb_usage(self):
    """Returns the amount of memory this impalad is using as reported by the operating
       system (resident memory).
    """
    pid = self.find_pid()
    if not pid:
      raise Exception("Impalad at %s is not running" % self.label)
    mem_kb = self.shell("ps --no-header -o rss -p %s" % pid)
    return int(mem_kb) // 1024

  def _read_web_page(self, relative_url, params={}, timeout_secs=DEFAULT_TIMEOUT):
    if "json" not in params:
      params = dict(params)
      params["json"] = "true"
    data = self._request_web_page(relative_url, params=params, timeout_secs=timeout_secs)\
        .json()
    if "__common__" in data:   # Remove the UI navigation stuff.
      del data["__common__"]
    return data

  def _request_web_page(self, relative_url, params={}, timeout_secs=DEFAULT_TIMEOUT):
    if self.cluster.use_ssl:
      scheme = 'https'
    else:
      scheme = 'http'
    url = '{scheme}://{host}:{port}{url}'.format(
        scheme=scheme,
        host=self.host_name,
        port=self.web_ui_port,
        url=relative_url)
    try:
      verify_ca = self.cluster.ca_cert if self.cluster.ca_cert is not None else False
      resp = requests.get(url, params=params, timeout=timeout_secs,
                          verify=verify_ca)
    except requests.exceptions.Timeout as e:
      raise Timeout(underlying_exception=e)
    resp.raise_for_status()
    return resp

  def get_metrics(self):
    return self._read_web_page("/metrics")["metric_group"]["metrics"]

  def get_metric(self, name):
    """Get metric from impalad by name. Raise exception if there is no such metric.
    """
    for metric in self.get_metrics():
      if metric["name"] == name:
        return metric
    raise Exception("Metric '%s' not found" % name);

  def __repr__(self):
    return "<%s host: %s>" % (type(self).__name__, self.label)


class MiniClusterImpalad(Impalad):

  def __init__(self, hs2_port, web_ui_port):
    super(MiniClusterImpalad, self).__init__()
    self._hs2_port = hs2_port
    self._web_ui_port = web_ui_port

  @property
  def label(self):
    return "%s:%s" % (self.host_name, self.hs2_port)

  @property
  def host_name(self):
    return "localhost"

  @property
  def hs2_port(self):
    return self._hs2_port

  @property
  def web_ui_port(self):
    return self._web_ui_port

  def find_pid(self):
    # Need to filter results to avoid pgrep picking up its parent bash script.
    # Test with:
    # sh -c "pgrep -l -f 'impala.*21050' | grep [i]mpalad | grep -o '^[0-9]*' || true"
    pid = self.shell("pgrep -l -f 'impalad.*%s' | grep [i]mpalad | "
                     "grep -o '^[0-9]*' || true" % self.hs2_port)
    if pid:
      return int(pid)

  def find_process_mem_mb_limit(self):
    return int(self.get_metric("mem-tracker.process.limit")["value"]) // 1024 ** 2

  def find_core_dump_dir(self):
    raise NotImplementedError()


class CmImpalad(Impalad):

  def __init__(self, cm_api):
    super(CmImpalad, self).__init__()
    self._api = cm_api
    self._host_name = None
    self._hs2_port = None
    self._web_ui_port = None

  @property
  def host_name(self):
    if not self._host_name:
      self._host_name = str(self.cluster.cm.get_host(self._api.hostRef.hostId).hostname)
    return self._host_name

  @property
  def hs2_port(self):
    if not self._hs2_port:
      self._hs2_port = self._get_cm_config("hs2_port", value_type=int)
    return self._hs2_port

  @property
  def web_ui_port(self):
    if not self._web_ui_port:
      self._web_ui_port = self._get_cm_config("impalad_webserver_port", value_type=int)
    return self._web_ui_port

  def find_pid(self):
    # Get the oldest pid. In a keberized cluster, occasionally two pids could be
    # found if -o isn't used. Presumably the second pid is the kerberos ticket
    # renewer.
    pid = self.shell("pgrep -o impalad || true")
    if pid:
      return int(pid)

  def find_process_mem_mb_limit(self):
    return self._get_cm_config("impalad_memory_limit", value_type=int) // 1024 ** 2

  def find_core_dump_dir(self):
    return self._get_cm_config("core_dump_dir")

  def _get_cm_config(self, config, value_type=None):
    config = self._api.get_config(view="full")[config]
    value = config.value or config.default
    if value_type:
      return value_type(value)
    return value
