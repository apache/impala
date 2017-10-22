#!/usr/bin/python
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
#   - Make container image smaller; perhaps make BE test binaries
#     smaller
#   - Split up cluster tests into two groups
#   - Analyze .xml junit files to find slow tests; eradicate
#     or move to different suite.
#   - Avoid building BE tests, and build them during execution,
#     saving on container space as well as baseline build
#     time.

# We do not use Impala's python environment here, nor do we depend on
# non-standard python libraries to avoid needing extra build steps before
# triggering this.
import argparse
import datetime
import logging
import multiprocessing
import multiprocessing.pool
import os
import re
import subprocess
import sys
import tempfile
import time

if __name__ == '__main__' and __package__ is None:
  sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
  import monitor

base = os.path.dirname(os.path.abspath(__file__))


def main():

  logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s %(threadName)s: %(message)s')

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
  parser.add_argument(
      '--build-image', metavar='IMAGE',
      help='Skip building, and run tests on pre-existing image.')
  parser.add_argument(
      '--suite', metavar='VARIANT', action='append',
      help="Run specific test suites; can be specified multiple times. \
          If not specified, all tests are run. Choices: " + ",".join(ALL_SUITES))
  parser.add_argument(
      '--name', metavar='NAME',
      help="Use a specific name for the test run. The name is used " +
      "as a prefix for the container and image names, and " +
      "as part of the log directory naming. Defaults to include a timestamp.",
      default=datetime.datetime.now().strftime("i-%Y%m%d-%H%M%S"))
  parser.add_argument('--timeout', metavar='MINUTES',
                      help="Timeout for test suites, in minutes.",
                      type=int,
                      default=60*2)
  parser.add_argument('--ccache-dir', metavar='DIR',
                      help="CCache directory to use",
                      default=os.path.expanduser("~/.ccache"))
  parser.add_argument('--test', action="store_true")
  args = parser.parse_args()

  if not args.suite:
    args.suite = ALL_SUITES
  t = TestWithDocker(
      build_image=args.build_image, suites=args.suite,
      name=args.name, timeout=args.timeout, cleanup_containers=args.cleanup_containers,
      cleanup_image=args.cleanup_image, ccache_dir=args.ccache_dir, test_mode=args.test,
      parallel_test_concurrency=args.parallel_test_concurrency,
      suite_concurrency=args.suite_concurrency,
      impalad_mem_limit_bytes=args.impalad_mem_limit_bytes)

  logging.getLogger('').addHandler(
      logging.FileHandler(os.path.join(_make_dir_if_not_exist(t.log_dir), "log.txt")))

  logging.info("Arguments: %s", args)

  ret = t.run()
  t.create_timeline()

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
  memlimit_gb = 7

  if total_memory_gb >= 95:
    suite_concurrency = 4
    parallel_test_concurrency = min(cpus, 12)
  elif total_memory_gb >= 65:
    suite_concurrency = 3
  elif total_memory_gb >= 35:
    suite_concurrency = 2
  else:
    logging.warning("This tool should be run on a machine with more memory.")
    suite_concurrency = 1

  return parallel_test_concurrency, suite_concurrency, memlimit_gb * 1024 * 1024 * 1024


# The names of all the test tracks supported.  NOOP isn't included here, but is
# handy for testing.  These are organized slowest-to-fastest, so that, when
# parallelism of suites is limited, the total time is not impacted.
ALL_SUITES = [
    "EE_TEST_SERIAL",
    "EE_TEST_PARALLEL",
    "CLUSTER_TEST",
    "BE_TEST",
    "FE_TEST",
    "JDBC_TEST",
]


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
  return subprocess.check_output(*args, **kwargs)


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

  def runtime_seconds(self):
    if self.start and self.end:
      return self.end - self.start

  def __str__(self):
    return "Container<" + \
        ",".join(["%s=%s" % (k, v) for k, v in self.__dict__.items()]) \
        + ">"


class TestWithDocker(object):
  """Tests Impala using Docker containers for parallelism."""

  def __init__(self, build_image, suites, name, timeout, cleanup_containers,
               cleanup_image, ccache_dir, test_mode,
               suite_concurrency, parallel_test_concurrency,
               impalad_mem_limit_bytes):
    self.build_image = build_image
    self.suites = [TestSuiteRunner(self, suite) for suite in suites]
    self.name = name
    self.containers = []
    self.timeout_minutes = timeout
    self.git_root = _check_output(["git", "rev-parse", "--show-toplevel"]).strip()
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

    container_id = _check_output([
        "docker", "create",
        # Required for some of the ntp handling in bootstrap and Kudu;
        # requirement may be lifted in newer Docker versions.
        "--privileged",
        "--name", name,
        "--hostname", name,
        # Label with the git root directory for easier cleanup
        "--label=pwd=" + self.git_root,
        # Consistent locales
        "-e", "LC_ALL=C",
        "-e", "IMPALAD_MEM_LIMIT_BYTES=" +
        str(self.impalad_mem_limit_bytes),
        # Mount the git directory so that clones can be local
        "-v", self.git_root + ":/repo:ro",
        "-v", self.ccache_dir + ":/ccache",
        # Share timezone between host and container
        "-v", "/etc/localtime:/mnt/localtime",
        "-v", _make_dir_if_not_exist(self.log_dir,
                                     logdir) + ":/logs",
        "-v", base + ":/mnt/base:ro"]
        + extras
        + [image]
        + entrypoint).strip()
    return Container(name=name, id_=container_id,
                     logfile=os.path.join(self.log_dir, logdir, logname))

  def _run_container(self, container):
    """Runs container, and returns True if the container had a successful exit value.

    This blocks while the container is running. The container output is
    run through annotate.py to add timestamps and saved into the container's log file.
    """
    container.running = True

    with file(container.logfile, "w") as log_output:
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
      return ret == 0

  @staticmethod
  def _stop_container(container):
    """Stops container. Ignores errors (e.g., if it's already exited)."""
    _call(["docker", "stop", container.id], check=False)
    if container.running:
      container.end = time.time()
      container.running = False

  @staticmethod
  def _rm_container(container):
    """Removes container."""
    _call(["docker", "rm", container.id], check=False)

  def _create_build_image(self):
    """Creates the "build image", with Impala compiled and data loaded."""
    container = self._create_container(
        image="ubuntu:16.04", name=self.name,
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
           container.id, "impala:built-" + self.name]).strip()
      logging.info("Committed docker image: %s", self.image)
    finally:
      if self.cleanup_containers:
        self._stop_container(container)
        self._rm_container(container)

  def _run_tests(self):
    start_time = time.time()
    timeout_seconds = self.timeout_minutes * 60
    deadline = start_time + timeout_seconds
    pool = multiprocessing.pool.ThreadPool(processes=self.suite_concurrency)
    outstanding_suites = []
    for suite in self.suites:
      suite.task = pool.apply_async(suite.run)
      outstanding_suites.append(suite)

    ret = True
    while time.time() < deadline and len(outstanding_suites) > 0:
      for suite in list(outstanding_suites):
        task = suite.task
        if task.ready():
          this_task_ret = task.get()
          outstanding_suites.remove(suite)
          if this_task_ret:
            logging.info("Suite %s succeeded.", suite.name)
          else:
            logging.info("Suite %s failed.", suite.name)
            ret = False
      time.sleep(10)
    if len(outstanding_suites) > 0:
      for container in self.containers:
        self._stop_container(container)
      for suite in outstanding_suites:
        suite.task.get()
      raise Exception("Tasks not finished within timeout (%s minutes): %s" %
                      (self.timeout_minutes, ",".join([
                          suite.name for suite in outstanding_suites])))
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
      logging.info("Containers:")
      for c in self.containers:
        def to_success_string(exitcode):
          if exitcode == 0:
            return "SUCCESS"
          return "FAILURE"
        logging.info("%s %s %s %s", to_success_string(c.exitcode), c.name, c.logfile,
                     c.runtime_seconds())
      return ret
    finally:
      self.monitor.stop()
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
        interesting_re=self._INTERESTING_RE)
    timeline.create(os.path.join(self.log_dir, "timeline.html"))


class TestSuiteRunner(object):
  """Runs a single test suite."""

  def __init__(self, test_with_docker, suite):
    self.test_with_docker = test_with_docker
    self.suite = suite
    self.task = None
    self.name = self.suite.lower()

  def run(self):
    """Runs given test. Returns true on success, based on exit code."""
    test_with_docker = self.test_with_docker
    suite = self.suite
    self.start = time.time()

    # io-file-mgr-test expects a real-ish file system at /tmp;
    # we mount a temporary directory into the container to appease it.
    tmpdir = tempfile.mkdtemp(prefix=test_with_docker.name + "-" + self.name)
    # Container names are sometimes used as hostnames, and DNS names shouldn't
    # have underscores.
    container_name = test_with_docker.name + "-" + self.name.replace("_", "-")

    container = test_with_docker._create_container(
        image=test_with_docker.image,
        name=container_name,
        extras=[
            "-v", tmpdir + ":/tmp",
            "-u", str(os.getuid()),
            "-e", "NUM_CONCURRENT_TESTS=" +
            str(test_with_docker.parallel_test_concurrency),
        ],
        logdir=self.name,
        logname="log-test-" + self.suite + ".txt",
        entrypoint=["/mnt/base/entrypoint.sh", "test_suite", suite])

    test_with_docker.containers.append(container)
    test_with_docker.monitor.add(container)
    try:
      return test_with_docker._run_container(container)
    except:
      return False
    finally:
      logging.info("Cleaning up containers for %s" % (suite,))
      test_with_docker._stop_container(container)
      if test_with_docker.cleanup_containers:
        test_with_docker._rm_container(container)


if __name__ == "__main__":
  main()
