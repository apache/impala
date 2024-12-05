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
# Monitors Docker containers for CPU and memory usage, and
# prepares an HTML timeline based on said monitoring.
#
# Usage example:
#   mon = monitor.ContainerMonitor("monitoring.txt")
#   mon.start()
#   # container1 is an object with attributes id, name, and logfile.
#   mon.add(container1)
#   mon.add(container2)
#   mon.stop()
#   timeline = monitor.Timeline("monitoring.txt",
#       [container1, container2],
#       re.compile(">>> "))
#   timeline.create("output.html")

from __future__ import absolute_import, division, print_function
import datetime
import json
import logging
import os
import shutil
import subprocess
import threading
import time


# Unit for reporting user/system CPU seconds in cpuacct.stat.
# See https://www.kernel.org/doc/Documentation/cgroup-v1/cpuacct.txt and time(7).
USER_HZ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])


def total_memory():
  """Returns total RAM on system, in GB."""
  return _memory()[0]


def used_memory():
  """Returns total used RAM on system, in GB."""
  return _memory()[1]


def _memory():
  """Returns (total, used) memory on system, in GB.

  Used is computed as total - available.

  Calls "free" and parses output. Sample output for reference:

                total        used        free      shared     buffers       cache   available
  Mem:    126747197440 26363965440 56618553344    31678464  2091614208 41673064448 99384889344
  Swap:             0           0           0
  """

  free_lines = subprocess.check_output(["free", "-b", "-w"],
      universal_newlines=True).split('\n')
  free_grid = [x.split() for x in free_lines]
  # Identify columns for "total" and "available"
  total_idx = free_grid[0].index("total")
  available_idx = free_grid[0].index("available")
  total = int(free_grid[1][1 + total_idx])
  available = int(free_grid[1][1 + available_idx])
  used = total - available
  total_gb = total / (1024.0 * 1024.0 * 1024.0)
  used_gb = used / (1024.0 * 1024.0 * 1024.0)
  return (total_gb, used_gb)


def datetime_to_seconds_since_epoch(dt):
  """Converts a Python datetime to seconds since the epoch."""
  return time.mktime(dt.timetuple())


def split_timestamp(line):
  """Parses timestamp at beginning of a line.

  Returns a tuple of seconds since the epoch and the rest
  of the line. Returns None on parse failures.
  """
  LENGTH = 26
  FORMAT = "%Y-%m-%d %H:%M:%S.%f"
  t = line[:LENGTH]
  return (datetime_to_seconds_since_epoch(datetime.datetime.strptime(t, FORMAT)),
          line[LENGTH + 1:])


class ContainerMonitor(object):
  """Monitors Docker containers.

  Monitoring data is written to a file. An example is:

  2018-02-02 09:01:37.143591 d8f640989524be3939a70557a7bf7c015ba62ea5a105a64c94472d4ebca93c50 cpu user 2 system 5
  2018-02-02 09:01:37.143591 d8f640989524be3939a70557a7bf7c015ba62ea5a105a64c94472d4ebca93c50 memory cache 11481088 rss 4009984 rss_huge 0 mapped_file 8605696 dirty 24576 writeback 0 pgpgin 4406 pgpgout 624 pgfault 3739 pgmajfault 99 inactive_anon 0 active_anon 3891200 inactive_file 7614464 active_file 3747840 unevictable 0 hierarchical_memory_limit 9223372036854771712 total_cache 11481088 total_rss 4009984 total_rss_huge 0 total_mapped_file 8605696 total_dirty 24576 total_writeback 0 total_pgpgin 4406 total_pgpgout 624 total_pgfault 3739 total_pgmajfault 99 total_inactive_anon 0 total_active_anon 3891200 total_inactive_file 7614464 total_active_file 3747840 total_unevictable 0

  That is, the format is:

  <timestamp> <container> cpu user <usercpu> system <systemcpu>
  <timestamp> <container> memory <contents of memory.stat without newlines>

  <usercpu> and <systemcpu> are in the units of USER_HZ.
  See https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt for documentation
  on memory.stat; it's in the "memory" cgroup, often mounted at
  /sys/fs/cgroup/memory/<cgroup>/memory.stat.

  This format is parsed back by the Timeline class below and should
  not be considered an API.
  """

  def __init__(self, output_path, frequency_seconds=1):
    """frequency_seconds is how often metrics are gathered"""
    self.containers = []
    self.output_path = output_path
    self.keep_monitoring = None
    self.monitor_thread = None
    self.frequency_seconds = frequency_seconds

  def start(self):
    self.keep_monitoring = True
    self.monitor_thread = threading.Thread(target=self._monitor)
    self.monitor_thread.setDaemon(True)
    self.monitor_thread.start()

  def stop(self):
    self.keep_monitoring = False
    self.monitor_thread.join()

  def add(self, container):
    """Adds monitoring for container, which is an object with property 'id'."""
    self.containers.append(container)

  @staticmethod
  def _metrics_from_stat_file(root, container, stat):
    """Returns metrics stat file contents.

    root: a cgroups root (a path as a string)
    container: an object with string attribute id
    stat: a string filename

    Returns contents of <root>/<container.id>/<stat>
    with newlines replaced with spaces.
    Returns None on errors.
    """
    dirname = os.path.join(root, "docker", container.id)
    if not os.path.isdir(dirname):
      # Container may no longer exist.
      return None
    try:
      statcontents = open(os.path.join(dirname, stat)).read()
      return statcontents.replace("\n", " ").strip()
    except IOError as e:
      # Ignore errors; cgroup can disappear on us.
      logging.warning("Ignoring exception reading cgroup. " +
                      "This can happen if container just exited. " + str(e))
      return None

  def _monitor(self):
    """Monitors CPU usage of containers.

    Otput is stored in self.output_path.
    Also, keeps track of minimum and maximum memory usage (for the machine).
    """
    # Ubuntu systems typically mount cpuacct cgroup in /sys/fs/cgroup/cpu,cpuacct,
    # but this can vary by OS distribution.
    all_cgroups = subprocess.check_output(
        "findmnt -n -o TARGET -t cgroup --source cgroup".split(), universal_newlines=True
    ).split("\n")
    cpuacct_root = [c for c in all_cgroups if "cpuacct" in c][0]
    memory_root = [c for c in all_cgroups if "memory" in c][0]
    logging.info("Using cgroups: cpuacct %s, memory %s", cpuacct_root, memory_root)
    self.min_memory_usage_gb = None
    self.max_memory_usage_gb = None

    with open(self.output_path, "w") as output:
      while self.keep_monitoring:
        # Use a single timestamp for a given round of monitoring.
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        for c in self.containers:
          cpu = self._metrics_from_stat_file(cpuacct_root, c, "cpuacct.stat")
          memory = self._metrics_from_stat_file(memory_root, c, "memory.stat")
          if cpu:
            output.write("%s %s cpu %s\n" % (now, c.id, cpu))
          if memory:
            output.write("%s %s memory %s\n" % (now, c.id, memory))
        output.flush()

        # Machine-wide memory usage
        m = used_memory()
        if self.min_memory_usage_gb is None:
          self.min_memory_usage_gb, self.max_memory_usage_gb = m, m
        else:
          self.min_memory_usage_gb = min(self.min_memory_usage_gb, m)
          self.max_memory_usage_gb = max(self.max_memory_usage_gb, m)
        time.sleep(self.frequency_seconds)


class Timeline(object):
  """Given metric and log data for containers, creates a timeline report.

  This is a standalone HTML file with a timeline for the log files and CPU charts for
  the containers. The HTML uses https://developers.google.com/chart/ for rendering
  the charts, which happens in the browser.
  """

  def __init__(self, monitor_file, containers, interesting_re, buildname):
    self.monitor_file = monitor_file
    self.containers = containers
    self.interesting_re = interesting_re
    self.buildname = buildname

  def logfile_timeline(self, container):
    """Returns a list of (name, timestamp, line) tuples for interesting lines in
    the container's logfile. container is expected to have name and logfile attributes.
    """
    interesting_lines = [
        line.strip()
        for line in open(container.logfile)
        if self.interesting_re.search(line)]
    return [(container.name,) + split_timestamp(line) for line in interesting_lines]

  def parse_metrics(self, f):
    """Parses timestamped metric lines.

    Given metrics lines like:

    2017-10-25 10:08:30.961510 87d5562a5fe0ea075ebb2efb0300d10d23bfa474645bb464d222976ed872df2a cpu user 33 system 15

    Returns an iterable of (ts, container, user_cpu, system_cpu). It also updates
    container.peak_total_rss and container.total_user_cpu and container.total_system_cpu.
    """
    prev_by_container = {}
    peak_rss_by_container = {}
    for line in f:
      ts, rest = split_timestamp(line.rstrip())
      total_rss = None
      try:
        container, metric_type, rest2 = rest.split(" ", 2)
        if metric_type == "cpu":
          _, user_cpu_s, _, system_cpu_s = rest2.split(" ", 3)
        elif metric_type == "memory":
          memory_metrics = rest2.split(" ")
          total_rss = int(memory_metrics[memory_metrics.index("total_rss") + 1 ])
      except:
        logging.warning("Skipping metric line: %s", line)
        continue

      if total_rss is not None:
        peak_rss_by_container[container] = max(peak_rss_by_container.get(container, 0),
            total_rss)
        continue

      prev_ts, prev_user, prev_system = prev_by_container.get(
          container, (None, None, None))
      user_cpu = int(user_cpu_s)
      system_cpu = int(system_cpu_s)
      if prev_ts is not None:
        # Timestamps are seconds since the epoch and are floats.
        dt = ts - prev_ts
        assert type(dt) == float
        if dt != 0:
          yield ts, container, (user_cpu - prev_user) // dt // USER_HZ,\
              (system_cpu - prev_system) // dt // USER_HZ
      prev_by_container[container] = ts, user_cpu, system_cpu

    # Now update container totals
    for c in self.containers:
      if c.id in prev_by_container:
        _, u, s = prev_by_container[c.id]
        c.total_user_cpu, c.total_system_cpu = u // USER_HZ, s // USER_HZ
      if c.id in peak_rss_by_container:
        c.peak_total_rss = peak_rss_by_container[c.id]

  def create(self, output):
    # Read logfiles
    timelines = []
    for c in self.containers:
      if not os.path.exists(c.logfile):
        logging.warning("Missing log file: %s", c.logfile)
        continue
      timelines.append(self.logfile_timeline(c))

    # Convert timelines to JSON
    min_ts = None
    timeline_json = []
    for timeline in timelines:
      for current_line, next_line in zip(timeline, timeline[1:]):
        name, ts_current, msg = current_line
        _, ts_next, _ = next_line
        timeline_json.append(
            [name, msg, ts_current, ts_next]
        )
    if not timeline_json:
      logging.warning("No timeline data; skipping timeline")
      return

    min_ts = min(x[2] for x in timeline_json)

    for row in timeline_json:
      row[2] = row[2] - min_ts
      row[3] = row[3] - min_ts

    # metrics_by_container: container -> [ ts, user, system ]
    metrics_by_container = dict()
    max_metric_ts = 0
    container_by_id = dict()
    for c in self.containers:
      container_by_id[c.id] = c

    for ts, container_id, user, system in self.parse_metrics(open(self.monitor_file)):
      container = container_by_id.get(container_id)
      if not container:
        continue

      if ts > max_metric_ts:
        max_metric_ts = ts
      if ts < min_ts:
        # We ignore metrics that show up before the timeline's
        # first messages. This largely avoids a bug in the
        # Google Charts visualization code wherein one of the series seems
        # to wrap around.
        continue
      metrics_by_container.setdefault(
          container.name, []).append((ts - min_ts, user, system))

    with open(output, "w") as o:
      template_path = os.path.join(os.path.dirname(__file__), "timeline.html.template")
      shutil.copyfileobj(open(template_path), o)
      o.write("\n<script>\nvar data = \n")
      json.dump(dict(buildname=self.buildname, timeline=timeline_json,
          metrics=metrics_by_container, max_ts=(max_metric_ts - min_ts)), o, indent=2)
      o.write("</script>")
      o.close()
