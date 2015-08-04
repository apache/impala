#!/usr/bin/env impala-python
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

# Utility code for creating cgroups for the Impala development environment.
# May be used as a library or as a command-line utility for manual testing.
import os
import sys
import errno

from optparse import OptionParser

# Options
parser = OptionParser()
parser.add_option("-s", "--cluster_size", type="int", dest="cluster_size", default=3,
                  help="Size of the cluster (number of impalad instances to start).")

def get_cpu_controller_root():
  """Returns the filesystem path of the CPU cgroup controller.
     Currently assumes the CPU controller is mounted in the standard location.
     TODO: Read /etc/mounts to find where cpu controller is mounted.
  """
  CGROUP_CPU_ROOT = "/sys/fs/cgroup/cpu"
  if not os.path.isdir(CGROUP_CPU_ROOT):
    raise Exception("Cgroup CPU controller is not mounted at %s" % (CGROUP_CPU_ROOT))
  return CGROUP_CPU_ROOT

def get_session_cpu_path():
  """Returns the path of the CPU cgroup hierarchy for this session, which is writable
     by the impalad processes. The cgroup hierarchy is specified as an absolute path
     under the CPU controller root.
  """
  PROC_SELF_CGROUP = '/proc/self/cgroup'
  cgroup_paths = open(PROC_SELF_CGROUP)
  try:
    for line in cgroup_paths:
      parts = line.strip().split(':')
      if len(parts) == 3 and parts[1] == 'cpu':
        return parts[2]
  finally:
    cgroup_paths.close()
  raise Exception("Process cgroup CPU hierarchy not found in %s" % (PROC_SELF_CGROUP))

def create_impala_cgroup_path(instance_num):
  """Returns the full filesystem path of a CPU controller cgroup hierarchy which is
     writeable by an impalad. The base cgroup path is read from the environment variable
     IMPALA_CGROUP_BASE_PATH if it is set, otherwise it is set to a child of the path of
     the cgroup for this process.

     instance_num is used to provide different (sibiling) cgroups for each impalad
     instance. The returned cgroup is created if necessary.
  """
  parent_cgroup = os.getenv('IMPALA_CGROUP_BASE_PATH')
  if parent_cgroup is None:
    # Join root path with the cpu hierarchy path by concatenting the strings. Can't use
    # path.join() because the session cpu hierarchy path looks like an absolute FS path.
    parent_cgroup = "%s%s" % (get_cpu_controller_root(), get_session_cpu_path())
  cgroup_path = os.path.join(parent_cgroup, ("impala-%s" % instance_num))
  try:
    os.makedirs(cgroup_path)
  except OSError, ex:
    if ex.errno == errno.EEXIST and os.path.isdir(cgroup_path):
        pass
    else: raise
  return cgroup_path

if __name__ == "__main__":
  if options.cluster_size < 0:
    print 'Please specify a cluster size >= 0'
    sys.exit(1)
  for i in range(options.cluster_size):
    create_impala_cgroup_path(i)
