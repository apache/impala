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

from __future__ import absolute_import, division, print_function
import argparse
import time
import logging
import os
import pipes
from subprocess import check_call
from tests.common.impala_cluster import ImpalaCluster
from tests.util.filesystem_utils import IS_EC
from threading import Event, Thread

IMPALA_HOME = os.environ["IMPALA_HOME"]


class AutoScaler(object):
  """This class implements a simple autoscaling algorithm: if queries queue up for a
  configurable duration, a new executor group is started. Likewise, if the number of
  concurrently running queries indicated that an executor group can be removed, such
  measure is taken.

  Users of this class can start an auto scaler by calling start() and must call stop()
  before exiting (see main() below for an example).

  This class only uses the default admission control pool.
  """
  DEFAULT_POOL_NAME = "default-pool"

  def __init__(self, executor_slots, group_size, start_batch_size=0, max_groups=0,
               wait_up_s=0, wait_down_s=0, coordinator_slots=128):
    # Number of queries that can run concurrently on each executor
    self.executor_slots = executor_slots
    self.coordinator_slots = coordinator_slots
    # Number of executors per executor group
    self.group_size = group_size
    # New executor groups will be started in increments of this size
    self.start_batch_size = group_size
    if start_batch_size > 0:
      self.start_batch_size = start_batch_size
    # Maximum number of executor groups. We only have 10 TCP ports free on our
    # miniclusters and we need one for the dedicated coordinator.
    self.max_groups = 9 // self.group_size
    # max_groups can further bound the maximum number of groups we are going to start,
    # but we won't start more than possible.
    if max_groups > 0 and max_groups < self.max_groups:
      self.max_groups = max_groups
    # Number of seconds to wait before scaling up/down
    self.scale_wait_up_s = 5
    if wait_up_s > 0:
      self.scale_wait_up_s = wait_up_s
    self.scale_wait_down_s = 5
    if wait_down_s > 0:
      self.scale_wait_down_s = wait_down_s
    self.groups = []
    self.num_groups = 0
    # Stopwatches to track how long the conditions for scaling up/down have been met.
    self.scale_up_sw = time.time()
    self.scale_down_sw = time.time()

    self.loop_thread = None
    # Event to signal that the control loop should exit
    self.stop_ev = Event()

  def get_cluster(self):
    return ImpalaCluster.get_e2e_test_cluster()

  def get_coordinator(self):
    cluster = self.get_cluster()
    assert len(cluster.impalads) > 0
    return cluster.get_first_impalad()

  def get_service(self):
    return self.get_coordinator().service

  def get_client(self):
    return self.get_coordinator().service.create_hs2_client()

  def group_name(self, idx):
    # By convention, group names must start with their associated resource pool name
    # followed by a "-".
    return "%s-group-%s" % (self.DEFAULT_POOL_NAME, idx)

  def start_base_cluster(self):
    """Starts the base cluster consisting of an exclusive coordinator, catalog, and
    statestore. Does not add any executors."""
    logging.info("Starting base cluster (coordinator, catalog, statestore)")
    cluster_args = ["--impalad_args=-executor_groups=coordinator"]
    self._start_impala_cluster(cluster_args, cluster_size=1,
                               executor_slots=self.coordinator_slots,
                               expected_num_executors=0, add_executors=False)
    logging.info("Done, number of running executor groups: %s" % self.num_groups)

  def start_group(self):
    """Starts an executor group. The name of the group is automatically determined based
    on the current number of total executor groups. Executors in the group will be started
    in batches."""
    self.num_groups += 1
    name = self.group_name(self.num_groups)
    desc = "%s:%s" % (name, self.group_size)
    logging.info("Starting executor group %s with %s members" % (name, self.group_size))
    cluster_args = ["--impalad_args=-executor_groups=%s" % desc]
    batch_size = self.start_batch_size
    num_started = 0
    num_expected = (self.num_groups - 1) * self.group_size
    while (num_started < self.group_size):
      to_start = min(batch_size, self.group_size - num_started)
      num_expected += to_start
      if to_start == 1:
        start_msg = "Starting executor %s" % (num_started + 1)
      else:
        start_msg = "Starting executors %s-%s" % (num_started + 1,
                                                  num_started + to_start)
      logging.info(start_msg)
      self._start_impala_cluster(cluster_args, cluster_size=to_start,
                                 executor_slots=self.executor_slots,
                                 expected_num_executors=num_expected, add_executors=True)
      num_started += to_start
    logging.info("Done, number of running executor groups: %s" % self.num_groups)

  def stop_group(self):
    """Stops the executor group that was added last."""
    name = self.group_name(self.num_groups)
    group_hosts = self.get_groups()[name]
    logging.info("Stopping executor group %s" % name)
    for host in group_hosts:
      logging.debug("Stopping host %s" % host)
      query = ":shutdown('%s');" % host
      self.execute(query)
    self.wait_for_group_gone(name)
    self.num_groups -= 1
    logging.info("Done, number of running executor groups: %s" % self.num_groups)

  def wait_for_group_gone(self, group_name, timeout=120):
    """Waits until all executors in group 'group_name' have unregistered themselves from
    the coordinator's cluster membership view."""
    end = time.time() + timeout
    while time.time() < end:
      groups = self.get_groups()
      if group_name not in groups:
        return
      time.sleep(0.5)
    assert False, "Timeout waiting for group %s to shut down" % group_name

  def get_groups(self):
    return self.get_service().get_executor_groups()

  def execute(self, query):
    return self.get_client().execute(query)

  def get_num_queued_queries(self):
    """Returns the number of queries currently queued in the default pool on the
    coordinator."""
    return self.get_service().get_num_queued_queries(pool_name=self.DEFAULT_POOL_NAME)

  def get_num_running_queries(self):
    """Returns the number of queries currently queued in the default pool on the
    coordinator."""
    return self.get_service().get_num_running_queries(self.DEFAULT_POOL_NAME)

  def loop(self):
    """Controls whether new executor groups need to be started or existing ones need to be
    stopped, based on the number of queries that are currently queued and running.
    """
    while not self.stop_ev.is_set():
      now = time.time()
      num_queued = self.get_num_queued_queries()
      num_running = self.get_num_running_queries()
      capacity = self.executor_slots * self.num_groups

      logging.debug("queued: %s, running: %s, capacity: %s" % (num_queued, num_running,
                                                               capacity))

      if num_queued == 0:
        self.scale_up_sw = now

      scale_up = self.scale_up_sw < now - self.scale_wait_up_s
      if scale_up and self.num_groups < self.max_groups:
        self.start_group()
        self.scale_up_sw = time.time()
        self.scale_down_sw = self.scale_up_sw
        continue

      surplus = capacity - num_running
      if surplus < self.executor_slots:
        self.scale_down_sw = now

      if self.scale_down_sw < now - self.scale_wait_down_s:
        self.stop_group()
        self.scale_up_sw = time.time()
        self.scale_down_sw = self.scale_up_sw
        continue

      time.sleep(1)

  def start(self):
    """Starts a base cluster with coordinator and statestore and the control loop to start
    and stop additional executor groups."""
    self.start_base_cluster()
    assert self.loop_thread is None
    self.loop_thread = Thread(target=self.loop)
    self.loop_thread.start()

  def stop(self):
    """Stops the AutoScaler and its cluster."""
    if self.stop_ev.is_set():
      return
    self.stop_ev.set()
    if self.loop_thread:
      self.loop_thread.join()
      self.loop_thread = None
    self._kill_whole_cluster()

  def _start_impala_cluster(self, options, cluster_size, executor_slots,
                            expected_num_executors, add_executors):
    """Starts an Impala cluster and waits for all impalads to come online.

    If 'add_executors' is True, new executors will be added to the cluster and the
    existing daemons will not be restarted. In that case 'cluster_size' must specify the
    number of nodes that will be added and 'expected_num_executors' must be the total
    expected number of executors after the additional ones have started.

    If 'add_executors' is false, 'cluster_size' must be 1 and a single exclusive
    coordinator will be started (together with catalog and statestore).
    """
    assert cluster_size > 0, "cluster_size cannot be 0"
    impala_log_dir = os.getenv("LOG_DIR", "/tmp/")
    cmd = [os.path.join(IMPALA_HOME, "bin/start-impala-cluster.py"),
           "--cluster_size=%d" % cluster_size,
           "--log_dir=%s" % impala_log_dir,
           "--log_level=1"]
    if add_executors:
      cmd.append("--add_executors")
    else:
      assert expected_num_executors == 0
      assert cluster_size == 1
      cmd.append("--use_exclusive_coordinators")

    impalad_args = [
        "-vmodule=admission-controller=3,cluster-membership-mgr=3",
        "-admission_control_slots=%s" % executor_slots,
        "-shutdown_grace_period_s=2"]

    options += ["--impalad_args=%s" % a for a in impalad_args]

    logging.debug("Starting cluster with command: %s" %
                 " ".join(pipes.quote(arg) for arg in cmd + options))
    log_debug = logging.getLogger().getEffectiveLevel() == logging.DEBUG
    log_file = None
    if not log_debug:
      log_file = open("/dev/null", "w")

    check_call(cmd + options, close_fds=True, stdout=log_file, stderr=log_file)

    # The number of statestore subscribers is
    # cluster_size (# of impalad) + 1 (for catalogd).
    if expected_num_executors > 0:
      expected_subscribers = expected_num_executors + 2
      expected_backends = expected_num_executors + 1
    else:
      expected_subscribers = cluster_size + 1
      expected_backends = 1

    cluster = self.get_cluster()
    statestored = cluster.statestored
    if statestored is None:
      raise Exception("statestored was not found")

    logging.debug("Waiting for %s subscribers to come online" % expected_subscribers)
    statestored.service.wait_for_live_subscribers(expected_subscribers, timeout=60)
    for impalad in cluster.impalads:
      logging.debug("Waiting for %s executors to come online" % expected_backends)
      impalad.service.wait_for_num_known_live_backends(expected_backends, timeout=60)

  def _kill_whole_cluster(self):
    """Terminates the whole cluster, i.e. all impalads, catalogd, and statestored."""
    logging.info("terminating cluster")
    check_call([os.path.join(IMPALA_HOME, "bin/start-impala-cluster.py"), "--kill_only"])


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-n", "--executor_slots", help="Concurrent queries per executor "
                      "group", type=int, default=3)
  parser.add_argument("-g", "--group_size", help="Number of executors per group",
                      type=int, default=2)
  parser.add_argument("-b", "--batch_size", help="Start executors of a group "
                      "in batches instead of all at once", type=int, default=0)
  parser.add_argument("-m", "--max_groups", help="Maximum number of groups to start",
                      type=int, default=0)
  parser.add_argument("-d", "--wait_down", help="Time to wait before scaling down (s)",
                      type=int, default=5)
  parser.add_argument("-u", "--wait_up", help="Time to wait before scaling up (s)",
                      type=int, default=5)
  parser.add_argument("-v", "--verbose", help="Verbose logging", action="store_true")
  args = parser.parse_args()

  # Restrict some logging for command line usage
  logging.getLogger("impala_cluster").setLevel(logging.INFO)
  logging.getLogger("requests").setLevel(logging.WARNING)
  if args.verbose:
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("impala.hiveserver2").setLevel(logging.INFO)
  else:
    logging.basicConfig(level=logging.INFO)
    # Also restrict other modules' debug output
    logging.getLogger("impala_connection").setLevel(logging.WARNING)
    logging.getLogger("impala_service").setLevel(logging.WARNING)
    logging.getLogger("impala.hiveserver2").setLevel(logging.WARNING)

  a = AutoScaler(executor_slots=args.executor_slots, group_size=args.group_size,
                 start_batch_size=args.batch_size, max_groups=args.max_groups,
                 wait_up_s=args.wait_up, wait_down_s=args.wait_down)
  a.start()
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    logging.debug("Caught KeyboardInterrupt, stopping autoscaler")
    a.stop()


if __name__ == "__main__":
  main()
