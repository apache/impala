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

# Starts up an Impala cluster (ImpalaD + State Store) with the specified number of
# ImpalaD instances. Each ImpalaD runs on a different port allowing this to be run
# on a single machine.
import getpass
import itertools
import json
import logging
import os
import psutil
import shlex
import sys
from datetime import datetime
from getpass import getuser
from time import sleep, time
from optparse import OptionParser, SUPPRESS_HELP
from subprocess import call, check_call
from testdata.common import cgroups
from tests.common.environ import build_flavor_timeout
from tests.common.impala_cluster import (ImpalaCluster, DEFAULT_BEESWAX_PORT,
    DEFAULT_HS2_PORT, DEFAULT_KRPC_PORT, DEFAULT_HS2_HTTP_PORT,
    DEFAULT_STATE_STORE_SUBSCRIBER_PORT, DEFAULT_IMPALAD_WEBSERVER_PORT,
    DEFAULT_STATESTORED_WEBSERVER_PORT, DEFAULT_CATALOGD_WEBSERVER_PORT,
    DEFAULT_ADMISSIOND_WEBSERVER_PORT, DEFAULT_CATALOGD_JVM_DEBUG_PORT,
    DEFAULT_EXTERNAL_FE_PORT, DEFAULT_IMPALAD_JVM_DEBUG_PORT,
    find_user_processes, run_daemon)

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOG.setLevel(level=logging.DEBUG)

KUDU_MASTER_HOSTS = os.getenv("KUDU_MASTER_HOSTS", "127.0.0.1")
DEFAULT_IMPALA_MAX_LOG_FILES = os.environ.get("IMPALA_MAX_LOG_FILES", 10)
INTERNAL_LISTEN_HOST = os.getenv("INTERNAL_LISTEN_HOST", "localhost")

# Options
parser = OptionParser()
parser.add_option("-s", "--cluster_size", type="int", dest="cluster_size", default=3,
                  help="Size of the cluster (number of impalad instances to start).")
parser.add_option("-c", "--num_coordinators", type="int", dest="num_coordinators",
                  default=3, help="Number of coordinators.")
parser.add_option("--use_exclusive_coordinators", dest="use_exclusive_coordinators",
                  action="store_true", default=False, help="If true, coordinators only "
                  "coordinate queries and execute coordinator fragments. If false, "
                  "coordinators also act as executors.")
parser.add_option("--build_type", dest="build_type", default= "latest",
                  help="Build type to use - debug / release / latest")
parser.add_option("--impalad_args", dest="impalad_args", action="append", type="string",
                  default=[],
                  help="Additional arguments to pass to each Impalad during startup")
parser.add_option("--state_store_args", dest="state_store_args", action="append",
                  type="string", default=[],
                  help="Additional arguments to pass to State Store during startup")
parser.add_option("--catalogd_args", dest="catalogd_args", action="append",
                  type="string", default=[],
                  help="Additional arguments to pass to the Catalog Service at startup")
parser.add_option("--admissiond_args", dest="admissiond_args",
                  action="append", type="string", default=[], help="Additional arguments "
                  "to pass to the Admission Control Service at startup")
parser.add_option("--kill", "--kill_only", dest="kill_only", action="store_true",
                  default=False, help="Instead of starting the cluster, just kill all"
                  " the running impalads and the statestored.")
parser.add_option("--force_kill", dest="force_kill", action="store_true", default=False,
                  help="Force kill impalad and statestore processes.")
parser.add_option("-a", "--add_executors", dest="add_executors",
                  action="store_true", default=False,
                  help="Start additional impalad processes. The executor group name must "
                  "be specified using --impalad_args")
parser.add_option("-r", "--restart_impalad_only", dest="restart_impalad_only",
                  action="store_true", default=False,
                  help="Restarts only the impalad processes")
parser.add_option("--restart_catalogd_only", dest="restart_catalogd_only",
                  action="store_true", default=False,
                  help="Restarts only the catalogd process")
parser.add_option("--restart_statestored_only", dest="restart_statestored_only",
                  action="store_true", default=False,
                  help="Restarts only the statestored process")
parser.add_option("--in-process", dest="inprocess", action="store_true", default=False,
                  help="Start all Impala backends and state store in a single process.")
parser.add_option("--log_dir", dest="log_dir",
                  default=os.environ["IMPALA_CLUSTER_LOGS_DIR"],
                  help="Directory to store output logs to.")
parser.add_option("--max_log_files", default=DEFAULT_IMPALA_MAX_LOG_FILES,
                  help="Max number of log files before rotation occurs.")
parser.add_option("--log_level", type="int", dest="log_level", default=1,
                   help="Set the impalad backend logging level")
parser.add_option("--jvm_args", dest="jvm_args", default="",
                  help="Additional arguments to pass to the JVM(s) during startup.")
parser.add_option("--env_vars", dest="env_vars", default="",
                  help="Additional environment variables for Impala to run with")
parser.add_option("--kudu_master_hosts", default=KUDU_MASTER_HOSTS,
                  help="The host name or address of the Kudu master. Multiple masters "
                      "can be specified using a comma separated list.")
parser.add_option("--docker_network", dest="docker_network", default=None,
                  help="If set, the cluster processes run inside docker containers "
                      "(which must be already built, e.g. with 'make docker_images'. "
                      "The containers are connected to the virtual network specified by "
                      "the argument value. This is currently experimental and not all "
                      "actions work. This mode only works on python 2.7+")
parser.add_option("--docker_auto_ports", dest="docker_auto_ports",
                  action="store_true", default=False,
                  help="Only has an effect if --docker_network is set. If true, Docker "
                       "will automatically allocate ports for client-facing endpoints "
                       "(Beewax, HS2, Web UIs, etc), which avoids collisions with other "
                       "running processes. If false, ports are mapped to the same ports "
                       "on localhost as the non-docker impala cluster.")
parser.add_option("--data_cache_dir", dest="data_cache_dir", default=None,
                  help="This specifies a base directory in which the IO data cache will "
                       "use.")
parser.add_option("--data_cache_size", dest="data_cache_size", default=0,
                  help="This specifies the maximum storage usage of the IO data cache "
                       "each Impala daemon can use.")
parser.add_option("--data_cache_eviction_policy", dest="data_cache_eviction_policy",
                  default="LRU", help="This specifies the cache eviction policy to use "
                  "for the data cache")
parser.add_option("--data_cache_enable_tracing", dest="data_cache_enable_tracing",
                  action="store_true", default=False,
                  help="If the data cache is enabled, this enables tracing accesses.")
parser.add_option("--enable_admission_service", dest="enable_admission_service",
                  action="store_true", default=False,
                  help="If true, enables the Admissison Control Service - the cluster "
                  "will be launched with an admissiond and all coordinators configured "
                  "to use it for admission control.")
parser.add_option("--enable_external_fe_support", dest="enable_external_fe_support",
                  action="store_true", default=False,
                  help="If true, impalads will start with the external_fe_port defined.")

# For testing: list of comma-separated delays, in milliseconds, that delay impalad catalog
# replica initialization. The ith delay is applied to the ith impalad.
parser.add_option("--catalog_init_delays", dest="catalog_init_delays", default="",
                  help=SUPPRESS_HELP)
# For testing: Semi-colon separated list of startup arguments to be passed per impalad.
# The ith group of options is applied to the ith impalad.
parser.add_option("--per_impalad_args", dest="per_impalad_args", type="string"
                  ,default="", help=SUPPRESS_HELP)

options, args = parser.parse_args()

IMPALA_HOME = os.environ["IMPALA_HOME"]
CORE_SITE_PATH = os.path.join(IMPALA_HOME, "fe/src/test/resources/core-site.xml")
KNOWN_BUILD_TYPES = ["debug", "release", "latest"]
# The location in the container where the cache is always mounted.
DATA_CACHE_CONTAINER_PATH = "/opt/impala/cache"

# Kills have a timeout to prevent automated scripts from hanging indefinitely.
# It is set to a high value to avoid failing if processes are slow to shut down.
KILL_TIMEOUT_IN_SECONDS = 240
# For build types like ASAN, modify the default Kudu rpc timeout.
KUDU_RPC_TIMEOUT = build_flavor_timeout(0, slow_build_timeout=60000)

# HTTP connections don't keep alive their associated sessions. We increase the timeout
# during builds to make spurious session expiration less likely.
DISCONNECTED_SESSION_TIMEOUT = 60 * 60 * 6

def check_process_exists(binary, attempts=1):
  """Checks if a process exists given the binary name. The `attempts` count allows us to
  control the time a process needs to settle until it becomes available. After each try
  the script will sleep for one second and retry. Returns True if it exists and False
  otherwise.
  """
  for _ in range(attempts):
    for _ in find_user_processes([binary]):
      return True
    sleep(1)
  return False


def run_daemon_with_options(daemon_binary, args, output_file, jvm_debug_port=None):
  """Wrapper around run_daemon() with options determined from command-line options."""
  env_vars = {"JAVA_TOOL_OPTIONS": build_java_tool_options(jvm_debug_port)}
  if options.env_vars is not None:
    for kv in options.env_vars.split():
      k, v = kv.split('=')
      env_vars[k] = v
  run_daemon(daemon_binary, args, build_type=options.build_type, env_vars=env_vars,
      output_file=output_file)


def build_java_tool_options(jvm_debug_port=None):
  """Construct the value of the JAVA_TOOL_OPTIONS environment variable to pass to
  daemons."""
  java_tool_options = ""
  if jvm_debug_port is not None:
    java_tool_options = ("-agentlib:jdwp=transport=dt_socket,address={debug_port}," +
        "server=y,suspend=n ").format(debug_port=jvm_debug_port) + java_tool_options
  if options.jvm_args is not None:
    java_tool_options += " " + options.jvm_args
  return java_tool_options

def kill_matching_processes(binary_names, force=False):
  """Kills all processes with the given binary name, waiting for them to exit"""
  # Send all the signals before waiting so that processes can clean up in parallel.
  processes = [proc for _, proc in find_user_processes(binary_names)]
  for process in processes:
    try:
      if force:
        process.kill()
      else:
        process.terminate()
    except psutil.NoSuchProcess:
      pass

  for process in processes:
    try:
      process.wait(KILL_TIMEOUT_IN_SECONDS)
    except psutil.TimeoutExpired:
      raise RuntimeError(("Unable to kill {process_name} (pid {process_pid}) "
          "after {num_seconds} seconds.").format(
              process_name=process.name,
              process_pid=process.pid,
              num_seconds=KILL_TIMEOUT_IN_SECONDS))


def choose_impalad_ports(instance_num):
  """Compute the ports for impalad instance num 'instance_num', returning as a map
  from the argument name to the port number."""
  return {'beeswax_port': DEFAULT_BEESWAX_PORT + instance_num,
          'hs2_port': DEFAULT_HS2_PORT + instance_num,
          'hs2_http_port': DEFAULT_HS2_HTTP_PORT + instance_num,
          'krpc_port': DEFAULT_KRPC_PORT + instance_num,
          'external_fe_port': DEFAULT_EXTERNAL_FE_PORT + instance_num,
          'state_store_subscriber_port':
              DEFAULT_STATE_STORE_SUBSCRIBER_PORT + instance_num,
          'webserver_port': DEFAULT_IMPALAD_WEBSERVER_PORT + instance_num}


def build_impalad_port_args(instance_num):
  IMPALAD_PORTS = (
      "-beeswax_port={beeswax_port} "
      "-hs2_port={hs2_port} "
      "-hs2_http_port={hs2_http_port} "
      "-krpc_port={krpc_port} "
      "-state_store_subscriber_port={state_store_subscriber_port} "
      "-webserver_port={webserver_port}")
  if options.enable_external_fe_support:
    IMPALAD_PORTS += " -external_fe_port={external_fe_port}"
  return IMPALAD_PORTS.format(**choose_impalad_ports(instance_num))


def build_logging_args(service_name):
  """Return a list of command line arguments to pass to daemon processes to configure
  logging"""
  result = ["-logbufsecs=5", "-v={0}".format(options.log_level),
      "-max_log_files={0}".format(options.max_log_files)]
  if options.docker_network is None:
    # Impala inside a docker container should always log to the same location.
    result += ["-log_filename={0}".format(service_name),
               "-log_dir={0}".format(options.log_dir)]
  return result


def impalad_service_name(i):
  """Return the name to use for the ith impala daemon in the cluster."""
  if i == 0:
    # The first impalad always logs to impalad.INFO
    return "impalad"
  else:
    return "impalad_node{node_num}".format(node_num=i)


def combine_arg_list_opts(opt_args):
  """Helper for processing arguments like impalad_args. The input is a list of strings,
  each of which is the string passed into one instance of the argument, e.g. for
  --impalad_args="-foo -bar" --impalad_args="-baz", the input to this function is
  ["-foo -bar", "-baz"]. This function combines the argument lists by tokenised each
  string into separate arguments, if needed, e.g. to produce the output
  ["-foo", "-bar", "-baz"]"""
  return list(itertools.chain(*[shlex.split(arg) for arg in opt_args]))


def build_statestored_arg_list():
  """Build a list of command line arguments to pass to the statestored."""
  return (build_logging_args("statestored") + build_kerberos_args("statestored") +
      combine_arg_list_opts(options.state_store_args))


def build_catalogd_arg_list():
  """Build a list of command line arguments to pass to the catalogd."""
  return (build_logging_args("catalogd") +
      ["-kudu_master_hosts", options.kudu_master_hosts] +
      build_kerberos_args("catalogd") +
      combine_arg_list_opts(options.catalogd_args))


def build_admissiond_arg_list():
  """Build a list of command line arguments to pass to the admissiond."""
  return (build_logging_args("admissiond") + build_kerberos_args("admissiond") +
      combine_arg_list_opts(options.admissiond_args))


def build_impalad_arg_lists(cluster_size, num_coordinators, use_exclusive_coordinators,
    remap_ports, start_idx=0, admissiond_host=INTERNAL_LISTEN_HOST):
  """Build the argument lists for impala daemons in the cluster. Returns a list of
  argument lists, one for each impala daemon in the cluster. Each argument list is
  a list of strings. 'num_coordinators' and 'use_exclusive_coordinators' allow setting
  up the cluster with dedicated coordinators.  If 'remap_ports' is true, the impalad
  ports are changed from their default values to avoid port conflicts. If the admission
  service is enabled, 'admissiond_host' is the hostname for the admissiond."""
  # TODO: currently we build a big string blob then split it. It would be better to
  # build up the lists directly.

  mem_limit_arg = ""
  if options.docker_network is None:
    mem_limit_arg = "-mem_limit={0}".format(compute_impalad_mem_limit(cluster_size))
  else:
    # For containerised impalads, set a memory limit via docker instead of directly,
    # to emulate what would happen in a production container. JVM heap is included,
    # so we should be able to use 100% of the detected mem_limit.
    mem_limit_arg = "-mem_limit=100%"

  delay_list = []
  if options.catalog_init_delays != "":
    delay_list = [delay.strip() for delay in options.catalog_init_delays.split(",")]

  per_impalad_args = []
  if options.per_impalad_args != "":
    per_impalad_args = [args.strip() for args in options.per_impalad_args.split(";")]

  # Build args for each each impalad instance.
  impalad_args = []
  for i in range(start_idx, start_idx + cluster_size):
    service_name = impalad_service_name(i)

    impala_port_args = ""
    if remap_ports:
      impala_port_args = build_impalad_port_args(i)
    # impalad args from the --impalad_args flag. Also replacing '#ID' with the instance.
    param_args = (" ".join(options.impalad_args)).replace("#ID", str(i))
    args = ("{mem_limit_arg} "
        "{impala_logging_args} "
        "{impala_port_args} "
        "{impala_kerberos_args} "
        "{param_args}").format(
            mem_limit_arg=mem_limit_arg,  # Goes first so --impalad_args will override it.
            impala_logging_args=" ".join(build_logging_args(service_name)),
            impala_port_args=impala_port_args,
            impala_kerberos_args=" ".join(build_kerberos_args("impalad")),
            param_args=param_args)
    if options.kudu_master_hosts:
      # Must be prepended, otherwise the java options interfere.
      args = "-kudu_master_hosts {kudu_master_hosts} {args}".format(
          kudu_master_hosts=options.kudu_master_hosts,
          args=args)

    if "kudu_client_rpc_timeout" not in args:
      args = "-kudu_client_rpc_timeout_ms {kudu_rpc_timeout} {args}".format(
          kudu_rpc_timeout=KUDU_RPC_TIMEOUT,
          args=args)

    if "disconnected_session_timeout" not in args:
      args = "-disconnected_session_timeout {timeout} {args}".format(
          timeout=DISCONNECTED_SESSION_TIMEOUT,
          args=args)

    if i >= num_coordinators:
      args = "-is_coordinator=false {args}".format(args=args)
    elif use_exclusive_coordinators:
      # Coordinator instance that doesn't execute non-coordinator fragments
      args = "-is_executor=false {args}".format(args=args)

    if i < len(delay_list):
      args = "-stress_catalog_init_delay_ms={delay} {args}".format(
          delay=delay_list[i],
          args=args)

    if options.data_cache_dir:
      # create the base directory
      assert options.data_cache_size != 0, "--data_cache_dir must be used along " \
          "with --data_cache_size"
      data_cache_path = \
          os.path.join(options.data_cache_dir, "impala-datacache-{0}".format(str(i)))
      # Try creating the directory if it doesn't exist already. May raise exception.
      if not os.path.exists(data_cache_path):
        os.mkdir(data_cache_path)
      if options.docker_network is None:
        data_cache_path_arg = data_cache_path
      else:
        # The data cache directory will always be mounted at the same path inside the
        # container.
        data_cache_path_arg = DATA_CACHE_CONTAINER_PATH

      args = "-data_cache={dir}:{quota} {args}".format(
          dir=data_cache_path_arg, quota=options.data_cache_size, args=args)

      # Add the eviction policy
      args = "-data_cache_eviction_policy={policy} {args}".format(
          policy=options.data_cache_eviction_policy, args=args)

      # Add access tracing arguments if requested
      if options.data_cache_enable_tracing:
        tracing_args = ""
        if options.docker_network is None:
          # To avoid collisions in log files, use different data_cache_trace_dir values
          # for different Impalads. The default directory is fine for the docker-based
          # tests.
          data_cache_trace_dir = "{log_dir}/data_cache_traces_{impalad_num}".format(
              log_dir=options.log_dir, impalad_num=i)
          tracing_args = "-data_cache_trace_dir={trace_dir} {tracing_args}".format(
              trace_dir=data_cache_trace_dir, tracing_args=tracing_args)

        tracing_args = "-data_cache_enable_tracing=true {tracing_args}".format(
            tracing_args=tracing_args)
        args = "{tracing_args} {args}".format(tracing_args=tracing_args, args=args)

    if options.enable_admission_service:
      args = "{args} -admission_service_host={host}".format(
          args=args, host=admissiond_host)

    # Appended at the end so they can override previous args.
    if i < len(per_impalad_args):
      args = "{args} {per_impalad_args}".format(
          args=args, per_impalad_args=per_impalad_args[i])
    impalad_args.append(shlex.split(args))
  return impalad_args


def build_kerberos_args(daemon):
  """If the cluster is kerberized, returns arguments to pass to daemon process.
  daemon should either be "impalad", "catalogd", "statestored", or "admissiond"."""
  # Note: this code has probably bit-rotted but is preserved in case someone needs to
  # revive the kerberized minicluster.
  assert daemon in ("impalad", "catalogd", "statestored", "admissiond")
  if call([os.path.join(IMPALA_HOME, "testdata/cluster/admin"), "is_kerberized"]) != 0:
    return []
  args = ["-keytab_file={0}".format(os.getenv("KRB5_KTNAME")),
          "-krb5_conf={0}".format(os.getenv("KRB5_CONFIG"))]
  if daemon == "impalad":
    args += ["-principal={0}".format(os.getenv("MINIKDC_PRINC_IMPALA")),
             "-be_principal={0}".format(os.getenv("MINIKDC_PRINC_IMPALA_BE"))]
  else:
    args.append("-principal={0}".format(os.getenv("MINIKDC_PRINC_IMPALA_BE")))
  if os.getenv("MINIKDC_DEBUG", "") == "true":
    args.append("-krb5_debug_file=/tmp/{0}.krb5_debug".format(daemon))
  return args


def compute_impalad_mem_limit(cluster_size):
  # Set mem_limit of each impalad to the smaller of 12GB or
  # 1/cluster_size (typically 1/3) of 70% of available memory.
  #
  # The default memory limit for an impalad is 80% of the total system memory. On a
  # mini-cluster with 3 impalads that means 240%. Since having an impalad be OOM killed
  # is very annoying, the mem limit will be reduced. This can be overridden using the
  # --impalad_args flag. virtual_memory().total returns the total physical memory.
  # The exact ratio to use is somewhat arbitrary. Peak memory usage during
  # tests depends on the concurrency of parallel tests as well as their ordering.
  # On the other hand, to avoid using too much memory, we limit the
  # memory choice here to max out at 12GB. This should be sufficient for tests.
  #
  # Beware that ASAN builds use more memory than regular builds.
  physical_mem_gb = psutil.virtual_memory().total / 1024 / 1024 / 1024
  available_mem = int(os.getenv("IMPALA_CLUSTER_MAX_MEM_GB", str(physical_mem_gb)))
  mem_limit = int(0.7 * available_mem * 1024 * 1024 * 1024 / cluster_size)
  return min(12 * 1024 * 1024 * 1024, mem_limit)

class MiniClusterOperations(object):
  """Implementations of operations for the non-containerized minicluster
  implementation.
  TODO: much of this logic could be moved into ImpalaCluster.
  """
  def get_cluster(self):
    """Return an ImpalaCluster instance."""
    return ImpalaCluster(use_admission_service=options.enable_admission_service)

  def kill_all_daemons(self, force=False):
    kill_matching_processes(["catalogd", "impalad", "statestored", "admissiond"], force)

  def kill_all_impalads(self, force=False):
    kill_matching_processes(["impalad"], force=force)

  def kill_catalogd(self, force=False):
    kill_matching_processes(["catalogd"], force=force)

  def kill_statestored(self, force=False):
    kill_matching_processes(["statestored"], force=force)

  def kill_admissiond(self, force=False):
    kill_matching_processes(["admissiond"], force=force)

  def start_statestore(self):
    LOG.info("Starting State Store logging to {log_dir}/statestored.INFO".format(
        log_dir=options.log_dir))
    output_file = os.path.join(options.log_dir, "statestore-out.log")
    run_daemon_with_options("statestored", build_statestored_arg_list(), output_file)
    if not check_process_exists("statestored", 10):
      raise RuntimeError("Unable to start statestored. Check log or file permissions"
                         " for more details.")

  def start_catalogd(self):
    LOG.info("Starting Catalog Service logging to {log_dir}/catalogd.INFO".format(
        log_dir=options.log_dir))
    output_file = os.path.join(options.log_dir, "catalogd-out.log")
    run_daemon_with_options("catalogd", build_catalogd_arg_list(), output_file,
        jvm_debug_port=DEFAULT_CATALOGD_JVM_DEBUG_PORT)
    if not check_process_exists("catalogd", 10):
      raise RuntimeError("Unable to start catalogd. Check log or file permissions"
                         " for more details.")

  def start_admissiond(self):
    LOG.info("Starting Admission Control Service logging to {log_dir}/admissiond.INFO"
        .format(log_dir=options.log_dir))
    output_file = os.path.join(options.log_dir, "admissiond-out.log")
    run_daemon_with_options("admissiond", build_admissiond_arg_list(), output_file)
    if not check_process_exists("admissiond", 10):
      raise RuntimeError("Unable to start admissiond. Check log or file permissions"
                         " for more details.")

  def start_impalads(self, cluster_size, num_coordinators, use_exclusive_coordinators,
                     start_idx=0):
    """Start 'cluster_size' impalad instances. The first 'num_coordinator' instances will
      act as coordinators. 'use_exclusive_coordinators' specifies whether the coordinators
      will only execute coordinator fragments."""
    if cluster_size == 0:
      # No impalad instances should be started.
      return

    # The current TCP port allocation of the minicluster allows up to 10 impalads before
    # the backend port (25000 + idx) will collide with the statestore (25010).
    assert start_idx + cluster_size <= 10, "Must not start more than 10 impalads"

    impalad_arg_lists = build_impalad_arg_lists(
        cluster_size, num_coordinators, use_exclusive_coordinators, remap_ports=True,
        start_idx=start_idx)
    assert cluster_size == len(impalad_arg_lists)
    for i in xrange(start_idx, start_idx + cluster_size):
      service_name = impalad_service_name(i)
      LOG.info("Starting Impala Daemon logging to {log_dir}/{service_name}.INFO".format(
          log_dir=options.log_dir, service_name=service_name))
      output_file = os.path.join(
          options.log_dir, "{service_name}-out.log".format(service_name=service_name))
      run_daemon_with_options("impalad", impalad_arg_lists[i - start_idx],
          jvm_debug_port=DEFAULT_IMPALAD_JVM_DEBUG_PORT + i, output_file=output_file)


class DockerMiniClusterOperations(object):
  """Implementations of operations for the containerized minicluster implementation
  with all processes attached to a user-defined docker bridge network.

  We assume that only one impala cluster is running on the network - existing containers
  created by this script (or with names that collide with those generated by this script)
  can be destroyed if present.

  We use a naming convention for the created docker containers so that we can easily
  refer to them with docker commands:
    impala-test-cluster-<network_name>-<daemon_name>[-<instance_num>],
  e.g. impala-test-cluster-impala_network-catalogd or
  impala-test-cluster-impala_network-impalad-0.
  """
  def __init__(self, network_name):
    self.network_name = network_name
    # Make sure that the network actually exists.
    check_call(["docker", "network", "inspect", network_name])

  def get_cluster(self):
    """Return an ImpalaCluster instance."""
    return ImpalaCluster(docker_network=self.network_name,
        use_admission_service=options.enable_admission_service)

  def kill_all_daemons(self, force=False):
    self.kill_statestored(force=force)
    self.kill_catalogd(force=force)
    self.kill_admissiond(force=force)
    self.kill_all_impalads(force=force)

  def kill_all_impalads(self, force=False):
    # List all running containers on the network and kill those with the impalad name
    # prefix to make sure that no running container are left over from previous clusters.
    container_name_prefix = self.__gen_container_name__("impalad")
    for container_id, info in self.__get_network_info__()["Containers"].iteritems():
      container_name = info["Name"]
      if container_name.startswith(container_name_prefix):
        LOG.info("Stopping container {0}".format(container_name))
        check_call(["docker", "stop", container_name])

  def kill_catalogd(self, force=False):
    self.__stop_container__("catalogd")

  def kill_statestored(self, force=False):
    self.__stop_container__("statestored")

  def kill_admissiond(self, force=False):
    self.__stop_container__("admissiond")

  def start_statestore(self):
    self.__run_container__("statestored", build_statestored_arg_list(),
        {DEFAULT_STATESTORED_WEBSERVER_PORT: DEFAULT_STATESTORED_WEBSERVER_PORT})

  def start_catalogd(self):
    self.__run_container__("catalogd", build_catalogd_arg_list(),
          {DEFAULT_CATALOGD_WEBSERVER_PORT: DEFAULT_CATALOGD_WEBSERVER_PORT})

  def start_admissiond(self):
    self.__run_container__("admissiond", build_admissiond_arg_list(),
          {DEFAULT_ADMISSIOND_WEBSERVER_PORT: DEFAULT_ADMISSIOND_WEBSERVER_PORT})

  def start_impalads(self, cluster_size, num_coordinators, use_exclusive_coordinators):
    impalad_arg_lists = build_impalad_arg_lists(cluster_size, num_coordinators,
        use_exclusive_coordinators, remap_ports=False, admissiond_host="admissiond")
    assert cluster_size == len(impalad_arg_lists)
    mem_limit = compute_impalad_mem_limit(cluster_size)
    for i in xrange(cluster_size):
      chosen_ports = choose_impalad_ports(i)
      port_map = {DEFAULT_BEESWAX_PORT: chosen_ports['beeswax_port'],
                  DEFAULT_HS2_PORT: chosen_ports['hs2_port'],
                  DEFAULT_HS2_HTTP_PORT: chosen_ports['hs2_http_port'],
                  DEFAULT_IMPALAD_WEBSERVER_PORT: chosen_ports['webserver_port'],
                  DEFAULT_EXTERNAL_FE_PORT: chosen_ports['external_fe_port']}
      self.__run_container__("impalad_coord_exec", impalad_arg_lists[i], port_map, i,
          mem_limit=mem_limit, supports_data_cache=True)

  def __gen_container_name__(self, daemon, instance=None):
    """Generate the name for the container, which should be unique among containers
    managed by this script."""
    return "impala-test-cluster-{0}-{1}".format(
        self.network_name, self.__gen_host_name__(daemon, instance))

  def __gen_host_name__(self, daemon, instance=None):
    """Generate the host name for the daemon inside the network, e.g. catalogd or
    impalad-1."""
    if instance is None:
      return daemon
    return "{0}-{1}".format(daemon, instance)

  def __run_container__(self, daemon, args, port_map, instance=None, mem_limit=None,
      supports_data_cache=False):
    """Launch a container with the daemon - impalad, catalogd, or statestored. If there
    are multiple impalads in the cluster, a unique instance number must be specified.
    'args' are command-line arguments to be appended to the end of the daemon command
    line. 'port_map' determines a mapping from container ports to ports on localhost. If
    --docker_auto_ports was set on the command line, 'port_map' is ignored and Docker
    will automatically choose the mapping. If there is an existing running or stopped
    container with the same name, it will be destroyed. If provided, mem_limit is
    passed to "docker run" as a string to set the memory limit for the container.
    If 'supports_data_cache' is true and the data cache is enabled via --data_cache_dir,
    mount the data cache inside the container."""
    self.__destroy_container__(daemon, instance)
    if options.docker_auto_ports:
      port_args = ["-P"]
    else:
      port_args = ["-p{dst}:{src}".format(src=src, dst=dst)
                   for src, dst in port_map.iteritems()]
    # Impersonate the current user for operations against the minicluster. This is
    # necessary because the user name inside the container is "root".
    # TODO: pass in the actual options
    env_args = ["-e", "HADOOP_USER_NAME={0}".format(getpass.getuser()),
                "-e", "JAVA_TOOL_OPTIONS={0}".format(
                    build_java_tool_options(DEFAULT_IMPALAD_JVM_DEBUG_PORT))]
    # The container build processes tags the generated image with the daemon name.
    debug_build = options.build_type == "debug" or (options.build_type == "latest" and
        os.path.basename(os.path.dirname(os.readlink("be/build/latest"))) == "debug")
    if debug_build:
      image_tag = daemon + "_debug"
    else:
      image_tag = daemon
    host_name = self.__gen_host_name__(daemon, instance)
    container_name = self.__gen_container_name__(daemon, instance)
    # Mount configuration into container so that we don't need to rebuild container
    # for config changes to take effect.
    conf_dir = os.path.join(IMPALA_HOME, "fe/src/test/resources/")
    mount_args = ["--mount", "type=bind,src={0},dst=/opt/impala/conf".format(conf_dir)]

    # Collect container logs in a unique subdirectory per daemon to avoid any potential
    # interaction between containers, which should be isolated.
    log_dir = os.path.join(IMPALA_HOME, options.log_dir, host_name)
    if not os.path.isdir(log_dir):
      os.makedirs(log_dir)
    mount_args += ["--mount", "type=bind,src={0},dst=/opt/impala/logs".format(log_dir)]

    # Create a data cache subdirectory for each daemon and mount at /opt/impala/cache
    # in the container.
    if options.data_cache_dir and supports_data_cache:
      data_cache_dir = os.path.join(options.data_cache_dir, host_name + "_cache")
      if not os.path.isdir(data_cache_dir):
        os.makedirs(data_cache_dir)
      mount_args += ["--mount", "type=bind,src={0},dst={1}".format(
                     data_cache_dir, DATA_CACHE_CONTAINER_PATH)]

    # Run the container as the current user.
    user_args = ["--user", "{0}:{1}".format(os.getuid(), os.getgid())]

    mem_limit_args = []
    if mem_limit is not None:
      mem_limit_args = ["--memory", str(mem_limit)]
    LOG.info("Running container {0}".format(container_name))
    run_cmd = (["docker", "run", "-d"] + env_args + port_args + user_args + ["--network",
      self.network_name, "--name", container_name, "--network-alias", host_name] +
      mount_args + mem_limit_args + [image_tag] + args)
    LOG.info("Running command {0}".format(run_cmd))
    check_call(run_cmd)
    port_mapping = check_output(["docker", "port", container_name])
    LOG.info("Launched container {0} with port mapping:\n{1}".format(
        container_name, port_mapping))

  def __stop_container__(self, daemon, instance=None):
    """Stop a container that was created by __run_container__()."""
    container_name = self.__gen_container_name__(daemon, instance)
    if call(["docker", "stop", container_name]) == 0:
      LOG.info("Stopped container {0}".format(container_name))

  def __destroy_container__(self, daemon, instance=None):
    """Destroy a container that was created by __run_container__()."""
    container_name = self.__gen_container_name__(daemon, instance)
    if call(["docker", "rm", "-f", container_name]) == 0:
      LOG.info("Destroyed container {0}".format(container_name))

  def __get_network_info__(self):
    """Get the output of "docker network inspect" as a python data structure."""
    output = check_output(["docker", "network", "inspect", self.network_name])
    # Only one network should be present in the top level array.
    return json.loads(output)[0]


def validate_options():
  if options.build_type not in KNOWN_BUILD_TYPES:
    LOG.error("Invalid build type {0}".format(options.build_type))
    LOG.error("Valid values: {0}".format(", ".join(KNOWN_BUILD_TYPES)))
    sys.exit(1)

  if options.cluster_size < 0:
    LOG.error("Please specify a cluster size >= 0")
    sys.exit(1)

  if (options.use_exclusive_coordinators and
      options.num_coordinators >= options.cluster_size):
    LOG.info("Starting impala cluster without executors")

  if not os.path.isdir(options.log_dir):
    LOG.error("Log dir does not exist or is not a directory: {log_dir}".format(
        log_dir=options.log_dir))
    sys.exit(1)

  restart_only_count = len([opt for opt in [options.restart_impalad_only,
                                            options.restart_statestored_only,
                                            options.restart_catalogd_only,
                                            options.add_executors] if opt])
  if restart_only_count > 1:
    LOG.error("--restart_impalad_only, --restart_catalogd_only, "
              "--restart_statestored_only, and --add_executors options are mutually "
              "exclusive")
    sys.exit(1)
  elif restart_only_count == 1:
    if options.inprocess:
      LOG.error(
        "Cannot perform individual component restarts using an in-process cluster")
      sys.exit(1)


if __name__ == "__main__":
  logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(threadName)s: %(message)s",
    datefmt="%H:%M:%S")
  validate_options()
  if options.docker_network is None:
    cluster_ops = MiniClusterOperations()
  else:
    if sys.version_info < (2, 7):
      raise Exception("Docker minicluster only supported on Python 2.7+")
    # We use some functions in the docker code that don't exist in Python 2.6.
    from subprocess import check_output
    cluster_ops = DockerMiniClusterOperations(options.docker_network)

  # If core-site.xml is missing, it likely means that we are missing config
  # files and should try regenerating them.
  if not os.path.exists(CORE_SITE_PATH):
    LOG.info("{0} is missing, regenerating cluster configs".format(CORE_SITE_PATH))
    check_call(os.path.join(IMPALA_HOME, "bin/create-test-configuration.sh"))

  # Kill existing cluster processes based on the current configuration.
  if options.restart_impalad_only:
    cluster_ops.kill_all_impalads(force=options.force_kill)
  elif options.restart_catalogd_only:
    cluster_ops.kill_catalogd(force=options.force_kill)
  elif options.restart_statestored_only:
    cluster_ops.kill_statestored(force=options.force_kill)
  elif options.add_executors:
    pass
  else:
    cluster_ops.kill_all_daemons(force=options.force_kill)

  if options.kill_only:
    sys.exit(0)

  if options.restart_impalad_only:
    impala_cluster = ImpalaCluster()
    if not impala_cluster.statestored or not impala_cluster.catalogd:
      LOG.info("No running statestored or catalogd detected. "
          "Restarting entire cluster.")
      options.restart_impalad_only = False

  existing_cluster_size = len(cluster_ops.get_cluster().impalads)
  expected_cluster_size = options.cluster_size
  num_coordinators = options.num_coordinators
  try:
    if options.restart_catalogd_only:
      cluster_ops.start_catalogd()
    elif options.restart_statestored_only:
      cluster_ops.start_statestore()
    elif options.restart_impalad_only:
      cluster_ops.start_impalads(options.cluster_size, options.num_coordinators,
                                 options.use_exclusive_coordinators)
    elif options.add_executors:
      num_coordinators = 0
      use_exclusive_coordinators = False
      cluster_ops.start_impalads(options.cluster_size, num_coordinators,
                                 use_exclusive_coordinators, existing_cluster_size)
      expected_cluster_size += existing_cluster_size
    else:
      cluster_ops.start_statestore()
      cluster_ops.start_catalogd()
      if options.enable_admission_service:
        cluster_ops.start_admissiond()
      cluster_ops.start_impalads(options.cluster_size, options.num_coordinators,
                                 options.use_exclusive_coordinators)
    # Sleep briefly to reduce log spam: the cluster takes some time to start up.
    sleep(3)

    impala_cluster = cluster_ops.get_cluster()
    expected_catalog_delays = 0
    if options.catalog_init_delays != "":
      for delay in options.catalog_init_delays.split(","):
        if int(delay.strip()) != 0: expected_catalog_delays += 1
    # Check for the cluster to be ready.
    impala_cluster.wait_until_ready(expected_cluster_size,
        expected_cluster_size - expected_catalog_delays)
  except Exception, e:
    LOG.exception("Error starting cluster")
    sys.exit(1)

  if options.use_exclusive_coordinators == True:
    executors = options.cluster_size - options.num_coordinators
  else:
    executors = options.cluster_size
  LOG.info(("Impala Cluster Running with {num_nodes} nodes "
      "({num_coordinators} coordinators, {num_executors} executors).").format(
          num_nodes=options.cluster_size,
          num_coordinators=min(options.cluster_size, num_coordinators),
          num_executors=executors))
