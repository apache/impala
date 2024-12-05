#!/usr/bin/env ambari-python-wrap
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
#

# We do not use Impala's python environment here, nor do we depend on
# non-standard python libraries to avoid needing extra build steps before
# triggering this.
from __future__ import absolute_import, division, print_function
import argparse
import datetime
import itertools
import logging
import multiprocessing
import multiprocessing.pool
import os
import re
import subprocess
import sys
import tempfile
import threading
import time

CLI_HELP = """\
Runs tests inside of docker containers, parallelizing different types of
tests. This script first creates a docker container, checks out this repo
into it, bootstraps the container with Impala dependencies, and builds Impala
and its test data.  Then, it saves the resulting container, and launches new
containers to run tests in parallel.  An HTML, visual timeline is generated
as part of the build, in logs/docker/*/timeline.html.
"""

# To execute run:
#   docker/test-with-docker.py
# After waiting for that to finish, inspect results in logs/docker.
#
# Visually, the timeline looks as follows, produced on a 32-core, 100GB RAM
# machine:
# .......                      1h05m Checkout, setup machine, build (8m with ccache),
#                                    generate testdata (52m); missing ccache
#                                    adds about 7 minutes (very sensitive to number
#                                    of available cores)
#        ...                     11m Commit the Docker container
#           .                    10m FE tests
#           .                    10m JDBC tests
#           ....                 45m serial EE tests
#           ......             1h02m cluster tests
#           ...                  31m BE (C++) tests
#           ....                 36m parallel EE tests
# Total time: 2h25m.
#
# CPU usage is sustained high for the parallel EE tests and for
# the C++ compile (when it's not ccache'd), but is otherwise low.
# Because every parallel track consumes memory (a cluster),
# increasing parallelism and memory must be balanced.
#
# Memory usage is thorny. The minicluster memory can
# be tweaked somewhat by how much memory to give to the JVM
# and what to set --mem_limit too. Furthermore, parallel
# cluster tests use more memory when more parallelism happens.
#
# The code that runs inside of the containers is in entrypoint.sh,
# whereas the code that invokes docker is here.
#
# We avoid using Dockerfile and "docker build": they make it hard or impossible
# to cross-mount host directories into containers or use --privileged, and using
# them would require generating them dynamically. They're more trouble than
# they're worth for this use case.
#
# In practice, the containers are about 100GB (with 45GB
# being test data and ~40GB being the tests).
#
# Requirements:
#  * Docker
#    This has been tested on Ubuntu16.04 with Docker
#    from the Ubuntu repos, i.e., Docker 1.13.1.
#  * About 150 GB of disk space available to Docker.
#  * 75GB of RAM.
#
# This script tries to clean up images and containers created by this process, though
# this can be disabled for debugging.
#
# To clean up containers and images manually, you can use:
#   for x in $(docker ps -aq --filter label=pwd=$IMPALA_HOME); do
#       docker stop $x; docker rm $x; done
#   for x in $(docker images -q --filter label=pwd=$IMPALA_HOME); do docker rmi $x; done
#
# Core dumps:
# On an Ubuntu host, core dumps and Docker don't mix by default, because apport is not
# running inside of the container. See https://github.com/moby/moby/issues/11740
# To enable core dumps, run the following command on the host:
#   $echo 'core.%e.%p' | sudo tee /proc/sys/kernel/core_pattern
#
# TODOs:
#  - Support for executing other flavors, like exhaustive, or file systems,
#    like S3.
#
# Suggested speed improvement TODOs:
#   - Speed up testdata generation
#   - Skip generating test data for variants not being run
#   - Make container image smaller
#   - Analyze .xml junit files to find slow tests; eradicate
#     or move to different suite.
#   - Run BE tests earlier (during data load)

if __name__ == '__main__' and __package__ is None:
  sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
  import monitor

base = os.path.dirname(os.path.abspath(__file__))

LOG_FORMAT="%(asctime)s %(threadName)s: %(message)s"


def main():
  logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

  default_parallel_test_concurrency, default_suite_concurrency, default_memlimit_gb = \
      _compute_defaults()
  parser = argparse.ArgumentParser(
      description=CLI_HELP, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  group = parser.add_mutually_exclusive_group()
  group.add_argument('--cleanup-containers', dest="cleanup_containers",
                     action='store_true', default=True,
                     help='Removes containers when finished.')
  group.add_argument('--no-cleanup-containers',
                     dest="cleanup_containers", action='store_false')
  group = parser.add_mutually_exclusive_group()
  parser.add_argument(
      '--parallel-test-concurrency', type=int,
      default=default_parallel_test_concurrency,
      help='For the ee-test-parallel suite, how many tests to run concurrently.')
  parser.add_argument(
      '--suite-concurrency', type=int, default=default_suite_concurrency,
      help='Number of concurrent suites to run in parallel.')
  parser.add_argument(
      '--impalad-mem-limit-bytes', type=int, default=default_memlimit_gb,
      help='Memlimit to pass to impalad for miniclusters.')
  group.add_argument(
      '--cleanup-image', dest="cleanup_image",
      action='store_true', default=True,
      help="Whether to remove image when done.")
  group.add_argument('--no-cleanup-image', dest="cleanup_image", action='store_false')
  parser.add_argument('--base-image', dest="base_image", default="ubuntu:16.04",
      help="Base OS image to use. ubuntu:16.04 and centos:6 are known to work.")
  parser.add_argument(
      '--build-image', metavar='IMAGE',
      help='Skip building, and run tests on pre-existing image.')

  suite_group = parser.add_mutually_exclusive_group()
  suite_group.add_argument(
      '--suite', metavar='VARIANT', action='append',
      help="""
        Run specific test suites; can be specified multiple times.
        Test-with-docker may shard some suites to improve parallelism.
        If not specified, default tests are run.
        Default: %s, All Choices: %s
        """ % (",".join([ s.name for s in DEFAULT_SUITES]),
          ",".join([ s.name for s in ALL_SUITES ])))
  suite_group.add_argument('--all-suites', action='store_true', default=False,
      help="If set, run all available suites.")
  parser.add_argument(
      '--name', metavar='NAME',
      help="Use a specific name for the test run. The name is used " +
      "as a prefix for the container and image names, and " +
      "as part of the log directory naming. Defaults to include a timestamp.",
      default=datetime.datetime.now().strftime("i-%Y%m%d-%H%M%S"))
  parser.add_argument('--ccache-dir', metavar='DIR',
                      help="CCache directory to use",
                      default=os.path.expanduser("~/.ccache"))
  parser.add_argument('--tail', action="store_true",
      help="Run tail on all container log files.")
  parser.add_argument('--env', metavar='K=V', default=[], action='append',
      help="""Passes given environment variables (expressed as KEY=VALUE)
           through containers.
           """)
  parser.add_argument('--test', action="store_true")
  args = parser.parse_args()

  if not args.suite:
    if args.all_suites:
      # Ignore "NOOP" tasks, as they are just for testing.
      args.suite = [ s.name for s in ALL_SUITES if not s.name.startswith("NOOP") ]
    else:
      args.suite = [ s.name for s in DEFAULT_SUITES ]
  t = TestWithDocker(
      build_image=args.build_image, suite_names=args.suite,
      name=args.name, cleanup_containers=args.cleanup_containers,
      cleanup_image=args.cleanup_image, ccache_dir=args.ccache_dir, test_mode=args.test,
      parallel_test_concurrency=args.parallel_test_concurrency,
      suite_concurrency=args.suite_concurrency,
      impalad_mem_limit_bytes=args.impalad_mem_limit_bytes,
      tail=args.tail,
      env=args.env, base_image=args.base_image)

  fh = logging.FileHandler(os.path.join(_make_dir_if_not_exist(t.log_dir), "log.txt"))
  fh.setFormatter(logging.Formatter(LOG_FORMAT))
  logging.getLogger('').addHandler(fh)

  logging.info("Arguments: %s", args)

  ret = t.run()
  t.create_timeline()
  t.log_summary()

  if not ret:
    sys.exit(1)


def _compute_defaults():
  """Compute default config options based on memory.

  The goal is to work reasonably on machines with
  about 60GB of memory, like Amazon's c4.8xlarge (36 CPUs, 60GB)
  or c5.9xlarge (36 CPUs, 72GB) or m4.4xlarge (16 CPUs, 64 GB).

  Based on some experiments, we set up defaults for different
  machine sizes based on memory, with an eye towards
  having reasonable runtimes as well.

  Experiments on memory usage:

  suite               parallelism usage
                    Xmx    memlimit
  ee-test-parallel  4GB  8  5GB   33GB
  ee-test-parallel  4GB 16  7GB   37GB
  ee-test-serial    4GB  -  5GB   18GB
  cluster-test      4GB  -    -   13GB
  be-test           4GB  - 10GB   19GB
  fe-test           4GB  - 10GB    9GB
  """
  total_memory_gb = monitor.total_memory()
  cpus = multiprocessing.cpu_count()
  logging.info("CPUs: %s Memory (GB): %s", cpus, total_memory_gb)

  parallel_test_concurrency = min(cpus, 8)
  memlimit_gb = 8

  if total_memory_gb >= 140:
    suite_concurrency = 6
    memlimit_gb = 11
    parallel_test_concurrency = min(cpus, 12)
  elif total_memory_gb >= 95:
    suite_concurrency = 5
    memlimit_gb = 11
    parallel_test_concurrency = min(cpus, 12)
  elif total_memory_gb >= 65:
    suite_concurrency = 3
  elif total_memory_gb >= 35:
    suite_concurrency = 2
  else:
    logging.warning("This tool should be run on a machine with more memory.")
    suite_concurrency = 1

  return parallel_test_concurrency, suite_concurrency, memlimit_gb * 1024 * 1024 * 1024

class Suite(object):
  """Encapsulates a test suite.

  A test suite is a named thing that the user can select to run,
  and it runs in its own container, in parallel with other suites.
  The actual running happens from entrypoint.sh and is controlled
  mostly by environment variables. When complexity is easier
  to handle in Python (with its richer data types), we prefer
  it here.
  """
  def __init__(self, name, **envs):
    """Create suite with given name and environment."""
    self.name = name
    self.envs = dict(
        FE_TEST="false",
        BE_TEST="false",
        EE_TEST="false",
        JDBC_TEST="false",
        CLUSTER_TEST="false")
    # If set, this suite is sharded past a certain suite concurrency threshold.
    self.shard_at_concurrency = None
    # Variable to which to append --shard_tests
    self.sharding_variable = None
    self.envs[name] = "true"
    self.envs.update(envs)
    self.timeout_minutes = 120

  def copy(self, name, **envs):
    """Duplicates current suite allowing for environment updates."""
    v = dict()
    v.update(self.envs)
    v.update(envs)
    ret = Suite(name, **v)
    ret.shard_at_concurrency = self.shard_at_concurrency
    ret.sharding_variable = self.sharding_variable
    ret.timeout_minutes = self.timeout_minutes
    return ret

  def exhaustive(self):
    """Returns an "exhaustive" copy of the suite."""
    r = self.copy(self.name + "_EXHAUSTIVE", EXPLORATION_STRATEGY="exhaustive")
    r.timeout_minutes = 240
    return r

  def asan(self):
    """Returns an ASAN copy of this suite."""
    r = self.copy(self.name + "_ASAN", REBUILD_ASAN="true")
    r.timeout_minutes = self.timeout_minutes * 2.0 + 10
    return r

  def sharded(self, shards):
    """Returns a list of sharded copies of the list.

    key is the name of the variable which needs to be appended with "--shard-tests=..."
    """
    # RUN_TESTS_ARGS
    ret = []
    for i in range(1, shards + 1):
      s = self.copy("%s_%d_of_%d" % (self.name, i, shards))
      s.envs[self.sharding_variable] = self.envs.get(self.sharding_variable, "") \
          + " --shard_tests=%s/%s" % (i, shards)
      ret.append(s)
    return ret

# Definitions of all known suites:
be_test = Suite("BE_TEST")
ee_test_serial = Suite("EE_TEST_SERIAL", EE_TEST="true",
    RUN_TESTS_ARGS="--skip-parallel --skip-stress")
ee_test_serial.shard_at_concurrency = 4
ee_test_serial.sharding_variable = "RUN_TESTS_ARGS"
ee_test_serial_exhaustive = ee_test_serial.exhaustive()
ee_test_parallel = Suite("EE_TEST_PARALLEL", EE_TEST="true",
    RUN_TESTS_ARGS="--skip-serial")
ee_test_parallel_exhaustive = ee_test_parallel.exhaustive()
cluster_test = Suite("CLUSTER_TEST")
cluster_test.shard_at_concurrency = 4
cluster_test.sharding_variable = "RUN_CUSTOM_CLUSTER_TESTS_ARGS"
cluster_test_exhaustive = cluster_test.exhaustive()

# Default supported suites. These are organized slowest-to-fastest, so that,
# when parallelism is limited, the total time is least impacted.
DEFAULT_SUITES = [
    cluster_test,
    Suite("FE_TEST"),
    ee_test_parallel,
    ee_test_serial,
    Suite("BE_TEST"),
    Suite("JDBC_TEST")
]

OTHER_SUITES = [
    ee_test_parallel_exhaustive,
    ee_test_serial_exhaustive,
    cluster_test_exhaustive,

    # ASAN
    be_test.asan(),
    cluster_test.asan(),
    ee_test_parallel.asan(),
    ee_test_serial.asan(),

    Suite("RAT_CHECK"),
    # These are used for testing this script
    Suite("NOOP"),
    Suite("NOOP_FAIL"),
    Suite("NOOP_SLEEP_FOREVER")
]
ALL_SUITES = DEFAULT_SUITES + OTHER_SUITES

def _call(args, check=True):
  """Wrapper for calling a subprocess.

  args is the first argument of subprocess.Popen, typically
  an array, e.g., ["echo", "hi"].

  If check is set, raise an exception on failure.
  """
  logging.info("Calling: %s", args)
  if check:
    subprocess.check_call(args, stdin=None)
  else:
    return subprocess.call(args, stdin=None)


def _check_output(*args, **kwargs):
  """Wrapper for subprocess.check_output, with logging."""
  logging.info("Running: %s, %s; cmdline: %s.", args, kwargs, " ".join(*args))
  return subprocess.check_output(*args, universal_newlines=True, **kwargs)


def _make_dir_if_not_exist(*parts):
  d = os.path.join(*parts)
  if not os.path.exists(d):
    os.makedirs(d)
  return d


class Container(object):
  """Encapsulates a container, with some metadata."""

  def __init__(self, id_, name, logfile, exitcode=None, running=None):
    self.id = id_
    self.name = name
    self.logfile = logfile
    self.exitcode = exitcode
    self.running = running
    self.start = None
    self.end = None
    self.removed = False

    # Protects multiple calls to "docker rm <self.id>"
    self.lock = threading.Lock()

    # Updated by Timeline class
    self.total_user_cpu = -1
    self.total_system_cpu = -1
    self.peak_total_rss = -1

  def runtime_seconds(self):
    if self.start and self.end:
      return self.end - self.start

  def __str__(self):
    return "Container<" + \
        ",".join(["%s=%s" % (k, v) for k, v in self.__dict__.items()]) \
        + ">"


class TestWithDocker(object):
  """Tests Impala using Docker containers for parallelism."""

  def __init__(self, build_image, suite_names, name, cleanup_containers,
               cleanup_image, ccache_dir, test_mode,
               suite_concurrency, parallel_test_concurrency,
               impalad_mem_limit_bytes, tail,
               env, base_image):
    self.build_image = build_image
    self.name = name
    self.containers = []
    self.git_root = _check_output(["git", "rev-parse", "--show-toplevel"]).strip()
    # Protects multiple concurrent calls to "docker create"
    self.docker_lock = threading.Lock()

    # If using worktrees, we need to find $GIT_COMMON_DIR; rev-parse
    # supports finding it as of vesion 2.5.0; for older versions, we
    # use $GIT_DIR.
    git_common_dir = _check_output(["git", "rev-parse", "--git-common-dir"]).strip()
    if git_common_dir == "--git-common-dir":
      git_common_dir = _check_output(["git", "rev-parse", "--git-dir"]).strip()
    self.git_common_dir = os.path.realpath(git_common_dir)
    assert os.path.exists(self.git_common_dir)

    self.git_head_rev = _check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip()
    assert self.git_head_rev, \
        "Could not get reference to HEAD using git rev-parse --abbrev-ref HEAD."
    self.cleanup_containers = cleanup_containers
    self.cleanup_image = cleanup_image
    self.image = None
    if build_image and cleanup_image:
      # Refuse to clean up external image.
      raise Exception("cleanup_image and build_image cannot be both specified")
    self.ccache_dir = ccache_dir
    self.log_dir = os.path.join(self.git_root, "logs", "docker", self.name)
    self.monitoring_output_file = os.path.join(self.log_dir, "metrics.txt")
    self.monitor = monitor.ContainerMonitor(self.monitoring_output_file)
    self.test_mode = test_mode
    self.suite_concurrency = suite_concurrency
    self.parallel_test_concurrency = parallel_test_concurrency
    self.impalad_mem_limit_bytes = impalad_mem_limit_bytes
    self.tail = tail
    self.env = env
    self.base_image = base_image

    # Map suites back into objects; we ignore case for this mapping.
    suites = []
    suites_by_name = {}
    for suite in ALL_SUITES:
      suites_by_name[suite.name.lower()] = suite
    for suite_name in suite_names:
      suites.append(suites_by_name[suite_name.lower()])

    # If we have enough concurrency, shard some suites into two halves.
    suites2 = []
    for suite in suites:
      if suite.shard_at_concurrency is not None and \
          suite_concurrency >= suite.shard_at_concurrency:
        suites2.extend(suite.sharded(2))
      else:
        suites2.append(suite)
    suites = suites2

    self.suite_runners = [TestSuiteRunner(self, suite) for suite in suites]

  def _create_container(self, image, name, logdir, logname, entrypoint, extras=None):
    """Returns a new container.

    logdir - subdirectory to create under self.log_dir,
      which will get mounted to /logs
    logname - name of file in logdir that will be created
    extras - extra arguments to pass to docker
    entrypoint - entrypoint arguments, as a list.
    """
    if extras is None:
      extras = []
    if self.test_mode:
      extras = ["-e", "TEST_TEST_WITH_DOCKER=true"] + extras

    # According to localtime(5), /etc/localtime is supposed
    # to be a symlink to somewhere inside /usr/share/zoneinfo.
    # Note that sometimes the symlink tree may be
    # complicated, e.g.:
    #  /etc/localtime ->
    #    /usr/share/zoneinfo/America/Los_Angeles ->  (readlink)
    #      ../US/Pacific-New                         (realpath)
    # Using both readlink and realpath should work, but we've
    # encountered one scenario (centos:6) where the Java tzdata
    # database doesn't have US/Pacific-New, but has America/Los_Angeles.
    # This is deemed sufficient to tip the scales to using readlink.
    assert os.path.islink("/etc/localtime")
    localtime_link_target = os.readlink("/etc/localtime")
    assert localtime_link_target.startswith("/usr/share/zoneinfo")

    # Workaround for what appears to be https://github.com/moby/moby/issues/13885
    # Namely, if we run too many "docker create" at the same time, one
    # of them hangs forever. To avoid the issue, we serialize the invocations
    # of "docker create".
    with self.docker_lock:
      container_id = _check_output([
          "docker", "create",
          # Required for some of the ntp handling in bootstrap and Kudu;
          # requirement may be lifted in newer Docker versions.
          "--privileged",
          "--name", name,
          # Whereas the container names vary across containers, we use the same
          # hostname repeatedly, so that the build container and the test
          # containers have the same hostnames. Kudu errors out with "Remote
          # error: Service unavailable: Timed out: could not wait for desired
          # snapshot timestamp to be consistent: Tablet is lagging too much to be
          # able to serve snapshot scan." if reading with READ_AT_SNAPSHOT
          # if the hostnames change underneath it.
          "--hostname", self.name,
          # Label with the git root directory for easier cleanup
          "--label=pwd=" + self.git_root,
          # Consistent locales
          "-e", "LC_ALL=C.UTF-8",
          "-e", "IMPALAD_MEM_LIMIT_BYTES=" +
          str(self.impalad_mem_limit_bytes),
          # Mount the git directory so that clones can be local.
          # We use /repo to have access to certain scripts,
          # and we use /git_common_dir to have local clones,
          # even when "git worktree" is being used.
          "-v", self.git_root + ":/repo:ro",
          "-v", self.git_common_dir + ":/git_common_dir:ro",
          "-e", "GIT_HEAD_REV=" + self.git_head_rev,
          # Share timezone between host and container
          "-e", "LOCALTIME_LINK_TARGET=" + localtime_link_target,
          "-v", self.ccache_dir + ":/ccache",
          "-e", "CCACHE_TEMPDIR=" + "/ccache/" + name,
          "-v", _make_dir_if_not_exist(self.log_dir,
                                       logdir) + ":/logs",
          "-v", base + ":/mnt/base:ro"]
          + list(itertools.chain(*[["-e", env] for env in self.env]))
          + extras
          + [image]
          + entrypoint).strip()
      ctr = Container(name=name, id_=container_id,
                       logfile=os.path.join(self.log_dir, logdir, logname))
      logging.info("Created container %s", ctr)
      return ctr

  def _run_container(self, container):
    """Runs container, and returns True if the container had a successful exit value.

    This blocks while the container is running. The container output is
    run through annotate.py to add timestamps and saved into the container's log file.
    """
    container.running = True
    tailer = None

    with open(container.logfile, "aw") as log_output:
      if self.tail:
        tailer = subprocess.Popen(
            ["tail", "-f", "--pid", str(os.getpid()), "-v", container.logfile])

      container.start = time.time()
      # Sets up a "docker start ... | annotate.py > logfile" pipeline using
      # subprocess.
      annotate = subprocess.Popen(
          [os.path.join(self.git_root, "docker", "annotate.py")],
          stdin=subprocess.PIPE,
          stdout=log_output,
          stderr=log_output)

      logging.info("Starting container %s; logging to %s", container.name,
                   container.logfile)
      docker = subprocess.Popen(["docker", "start", "--attach", container.id],
                                stdin=None, stdout=annotate.stdin, stderr=annotate.stdin)

      ret = docker.wait()
      annotate.stdin.close()
      annotate.wait()

      logging.info("Container %s returned %s", container, ret)
      container.exitcode = ret
      container.running = False
      container.end = time.time()
      if tailer:
        tailer.kill()
      return ret == 0

  @staticmethod
  def _stop_container(container):
    """Stops container. Ignores errors (e.g., if it's already exited)."""
    if container.running:
      _call(["docker", "stop", container.id], check=False)
      container.end = time.time()
      container.running = False

  @staticmethod
  def _rm_container(container):
    """Removes container."""
    # We can have multiple threads trying to call "docker rm" on a container.
    # Docker will fail one of those with "already running", but we actually
    # want to block until it's removed. Using a lock to serialize the "docker # rm"
    # calls handles that.
    with container.lock:
      if not container.removed:
        _call(["docker", "rm", container.id], check=False)
      container.removed = True

  def _create_build_image(self):
    """Creates the "build image", with Impala compiled and data loaded."""
    container = self._create_container(
        image=self.base_image, name=self.name + "-build",
        logdir="build",
        logname="log-build.txt",
        # entrypoint.sh will create a user with our uid; this
        # allows the shared file systems to work seamlessly
        entrypoint=["/mnt/base/entrypoint.sh", "build", str(os.getuid())])
    self.containers.append(container)
    self.monitor.add(container)
    try:
      logging.info("Docker container for build: %s", container)
      _check_output(["docker", "start", container.id])
      if not self._run_container(container):
        raise Exception("Build container failed.")
      logging.info("Committing docker container.")
      self.image = _check_output(
          ["docker", "commit",
           "-c", "LABEL pwd=" + self.git_root,
           "-c", "USER impdev",
           "-c", "WORKDIR /home/impdev/Impala",
           "-c", 'CMD ["/home/impdev/Impala/docker/entrypoint.sh", "shell"]',
           container.id, "impala:built-" + self.name]).strip()
      logging.info("Committed docker image: %s", self.image)
    finally:
      if self.cleanup_containers:
        self._stop_container(container)
        self._rm_container(container)

  def _run_tests(self):
    pool = multiprocessing.pool.ThreadPool(processes=self.suite_concurrency)
    outstanding_suites = []
    for suite in self.suite_runners:
      suite.task = pool.apply_async(suite.run)
      outstanding_suites.append(suite)

    ret = True
    try:
      while len(outstanding_suites) > 0:
        for suite in list(outstanding_suites):
          if suite.timed_out():
            msg = "Task %s not finished within timeout %s" % (suite.name,
                suite.suite.timeout_minutes,)
            logging.error(msg)
            raise Exception(msg)
          task = suite.task
          if task.ready():
            this_task_ret = task.get()
            outstanding_suites.remove(suite)
            if this_task_ret:
              logging.info("Suite %s succeeded.", suite.name)
            else:
              logging.info("Suite %s failed.", suite.name)
              ret = False
        time.sleep(5)
    except KeyboardInterrupt:
      logging.info("\n\nDetected KeyboardInterrupt; shutting down!\n\n")
      raise
    finally:
      pool.terminate()
    return ret

  def run(self):
    # Create logs directories and ccache dir.
    _make_dir_if_not_exist(self.ccache_dir)
    _make_dir_if_not_exist(self.log_dir)

    self.monitor.start()
    try:
      if not self.build_image:
        self._create_build_image()
      else:
        self.image = self.build_image
      ret = self._run_tests()
      return ret
    finally:
      self.monitor.stop()
      if self.cleanup_containers:
        for c in self.containers:
          self._stop_container(c)
          self._rm_container(c)
      if self.cleanup_image and self.image:
        _call(["docker", "rmi", self.image], check=False)
      logging.info("Memory usage: %s GB min, %s GB max",
                   self.monitor.min_memory_usage_gb,
                   self.monitor.max_memory_usage_gb)

  # Strings (really, regular expressions) pulled out into to the visual timeline.
  _INTERESTING_STRINGS = [
      ">>> ",
  ]
  _INTERESTING_RE = re.compile("|".join("(%s)" % (s,) for s in _INTERESTING_STRINGS))

  def create_timeline(self):
    """Creates timeline into log directory."""
    timeline = monitor.Timeline(
        monitor_file=self.monitoring_output_file,
        containers=self.containers,
        interesting_re=self._INTERESTING_RE,
        buildname=self.name)
    timeline.create(os.path.join(self.log_dir, "timeline.html"))

  def log_summary(self):
    logging.info("Containers:")
    def to_success_string(exitcode):
      if exitcode == 0:
        return "SUCCESS"
      return "FAILURE"

    for c in self.containers:
      logging.info("%s %s %s %0.1fm wall, %0.1fm user, %0.1fm system, " +
            "%0.1fx parallelism, %0.1f GB peak RSS",
          to_success_string(c.exitcode), c.name, c.logfile,
          c.runtime_seconds() / 60.0,
          c.total_user_cpu / 60.0,
          c.total_system_cpu / 60.0,
          (c.total_user_cpu + c.total_system_cpu) / max(c.runtime_seconds(), 0.0001),
          c.peak_total_rss / 1024.0 / 1024.0 / 1024.0)


class TestSuiteRunner(object):
  """Runs a single test suite."""

  def __init__(self, test_with_docker, suite):
    self.test_with_docker = test_with_docker
    self.suite = suite
    self.task = None
    self.name = suite.name.lower()
    # Set at the beginning of run and facilitates enforcing timeouts
    # for individual suites.
    self.deadline = None

  def timed_out(self):
    return self.deadline is not None and time.time() > self.deadline

  def run(self):
    """Runs given test. Returns true on success, based on exit code."""
    self.deadline = time.time() + self.suite.timeout_minutes * 60
    test_with_docker = self.test_with_docker
    suite = self.suite
    envs = ["-e", "NUM_CONCURRENT_TESTS=" + str(test_with_docker.parallel_test_concurrency)]
    for k, v in sorted(suite.envs.items()):
      envs.append("-e")
      envs.append("%s=%s" % (k, v))

    self.start = time.time()

    # io-file-mgr-test expects a real-ish file system at /tmp;
    # we mount a temporary directory into the container to appease it.
    tmpdir = tempfile.mkdtemp(prefix=test_with_docker.name + "-" + self.name)
    os.chmod(tmpdir, 0o1777)
    # Container names are sometimes used as hostnames, and DNS names shouldn't
    # have underscores.
    container_name = test_with_docker.name + "-" + self.name.replace("_", "-")

    container = test_with_docker._create_container(
        image=test_with_docker.image,
        name=container_name,
        extras=[
            "-v", tmpdir + ":/tmp",
            "-u", str(os.getuid())
        ] + envs,
        logdir=self.name,
        logname="log-test-" + self.suite.name + ".txt",
        entrypoint=["/mnt/base/entrypoint.sh", "test_suite", suite.name])

    test_with_docker.containers.append(container)
    test_with_docker.monitor.add(container)
    try:
      return test_with_docker._run_container(container)
    except:
      return False
    finally:
      logging.info("Cleaning up containers for %s" % (suite.name,))
      test_with_docker._stop_container(container)
      if test_with_docker.cleanup_containers:
        test_with_docker._rm_container(container)


if __name__ == "__main__":
  main()
