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

from __future__ import absolute_import, division, print_function
import os
import sys

kerberize = os.environ.get('IMPALA_KERBERIZE') == 'true'
hive_major_version = int(os.environ['IMPALA_HIVE_VERSION'][0])


def _get_system_ram_mb():
  lines = open("/proc/meminfo").readlines()
  memtotal_line = [l for l in lines if l.startswith('MemTotal')][0]
  mem_kb = int(memtotal_line.split()[1])
  return mem_kb // 1024


def _get_yarn_nm_ram_mb():
  sys_ram = _get_system_ram_mb()
  available_ram_gb = int(os.getenv("IMPALA_CLUSTER_MAX_MEM_GB", str(sys_ram // 1024)))
  # Fit into the following envelope:
  # - need 4GB at a bare minimum
  # - leave at least 20G for other services
  # - don't need more than 48G
  ret = min(max(available_ram_gb * 1024 - 20 * 1024, 4096), 48 * 1024)
  print("Configuring Yarn NM to use {0}MB RAM".format(ret), file=sys.stderr)
  return ret


CONFIG = {
  # Host/port configs
  'yarn.resourcemanager.webapp.address': '${EXTERNAL_LISTEN_HOST}:${YARN_WEBUI_PORT}',
  'yarn.nodemanager.address': '${INTERNAL_LISTEN_HOST}:${NODEMANAGER_PORT}',
  'yarn.nodemanager.localizer.address': '${INTERNAL_LISTEN_HOST}:${NODEMANAGER_LOCALIZER_PORT}',
  'yarn.nodemanager.webapp.address': '${INTERNAL_LISTEN_HOST}:${NODEMANAGER_WEBUI_PORT}',

  # Directories
  'yarn.nodemanager.local-dirs': '${NODE_DIR}/var/lib/hadoop-yarn/cache/${USER}/nm-local-dir',
  'yarn.nodemanager.log-dirs': '${NODE_DIR}/var/log/hadoop-yarn/containers',

  # Set it to a large enough value so that the logs of all the containers ever created in
  # a Jenkins run will be retained.
  'yarn.nodemanager.log.retain-seconds': 86400,

  # Enable the MR shuffle service, which is also used by Tez.
  'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
  'yarn.nodemanager.aux-services.mapreduce_shuffle.class': 'org.apache.hadoop.mapred.ShuffleHandler',
  # Disable vmem checking, since vmem is essentially free, and tasks
  # fail with vmem limit errors otherwise.
  'yarn.nodemanager.vmem-check-enabled': 'false',

  # Limit memory used by the NM to 8GB.
  # TODO(todd): auto-configure this based on the memory available on the machine
  # to speed up data-loading.
  'yarn.nodemanager.resource.memory-mb': _get_yarn_nm_ram_mb(),

  # Allow YARN to run with at least 3GB disk free. Otherwise it hangs completely.
  # Avoids disabling YARN disk monitoring completely because otherwise multiple jobs might
  # use up all the disk in a scenario where otherwise they could complete sequentially.
  'yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage': 99,

  # Increase YARN container resources to 2GB to avoid dataload failures
  'yarn.app.mapreduce.am.resource.mb': 2048,

  # Increase YARN minimum container size to 2GB to avoid dataload failures
  'yarn.scheduler.minimum-allocation-mb': 2048
}

app_classpath = [
  # Default classpath as provided by Hadoop: these environment variables are not
  # expanded by our config templating, but rather evaluated and expanded by
  # YARN itself, in a context where the various _HOMEs have been defined.
  '$HADOOP_CONF_DIR',
  '$HADOOP_COMMON_HOME/share/hadoop/common/*',
  '$HADOOP_COMMON_HOME/share/hadoop/common/lib/*',
  '$HADOOP_HDFS_HOME/share/hadoop/hdfs/*',
  '$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*',
  '$HADOOP_YARN_HOME/share/hadoop/yarn/*',
  '$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*']

# Hive 3 needs Tez on the classpath.
if hive_major_version == 3:
  app_classpath += [
      '${TEZ_HOME}/*',
      '${TEZ_HOME}/lib/*']

CONFIG['yarn.application.classpath'] = ",".join(app_classpath)

if kerberize:
  CONFIG.update({
    'yarn.resourcemanager.keytab': '${KRB5_KTNAME}',
    'yarn.resourcemanager.principal': '${MINIKDC_PRINC_USER}',
    'yarn.nodemanager.keytab': '${KRB5_KTNAME}',
    'yarn.nodemanager.principal': '${MINIKDC_PRINC_USER}',
  })
