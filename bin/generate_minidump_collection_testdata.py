#!/usr/bin/env impala-python
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

# This script generates testdata for collect_minidumps.py. 3 text files will be created
# containing statup flags for each of the services in (conf_dir)/impalad_flags,
# (conf_dir)/state_store_flags, and (conf_dir)/catalogsever_flags. Each of those files
# will have a parameter -minidump_path. Each path will look like (minidump_dir)/impalad,
# (minidump_dir)/catalogd, (minidump_dir)/statestored. Sample minidump files will be
# generated and placed into each of those directories. It is possible to control the
# minidump file timestamps by specifying the start_time and end_time. The timestamps will
# be spaced evenly in the interval. Alternatively, duration can be specified which will
# create the files in the interval [now - duration, now]. Minidumps are simulated by
# making the files easily compressible by having some repeated data.

from __future__ import absolute_import, division, print_function
from builtins import range
import errno
import os
import random
import shutil
import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option('--conf_dir', default='/tmp/impala-conf')
parser.add_option('--log_dir', default='/tmp/impala-logs')
parser.add_option('--minidump_dir', default='minidumps')
parser.add_option('--start_time', default=None, type='int')
parser.add_option('--end_time', default=None, type='int')
parser.add_option('--duration', default=3600, type='int',
    help="if start and end time are not set, they will be calculated based on this value")
parser.add_option('--num_minidumps', default=20, type='int')

options, args = parser.parse_args()

CONFIG_FILE = '''-beeswax_port=21000
-fe_port=21000
-hs2_port=21050
-enable_webserver=true
-mem_limit=108232130560
-max_log_files=10
-webserver_port=25000
-max_result_cache_size=100000
-state_store_subscriber_port=23000
-statestore_subscriber_timeout_seconds=30
-scratch_dirs=/data/1/impala/impalad,/data/10/impala/impalad,/data/11/impala/impalad
-default_query_options
-log_filename=impalad
-audit_event_log_dir=/var/log/impalad/audit
-max_audit_event_log_file_size=5000
-abort_on_failed_audit_event=false
-lineage_event_log_dir=/var/log/impalad/lineage
-log_dir={0}
-minidump_path={1}
-max_lineage_log_file_size=5000
-hostname=host1.example.com
-state_store_host=host2.example.com
-state_store_port=24000
-catalog_service_host=host2.example.com
-catalog_service_port=26000
-local_library_dir=/var/lib/impala/udfs
-disk_spill_encryption=false
-abort_on_config_error=true'''

ROLE_NAMES = {'impalad': 'impalad_flags',
    'statestored': 'state_store_flags',
    'catalogd':  'catalogserver_flags'}

def generate_conf_files():
  try:
    os.makedirs(options.conf_dir)
  except OSError as e:
    if e.errno == errno.EEXIST and os.path.isdir(options.conf_dir):
      pass
    else:
      raise e
  for role_name in ROLE_NAMES:
    with open(os.path.join(options.conf_dir, ROLE_NAMES[role_name]), 'w') as f:
      f.write(CONFIG_FILE.format(options.log_dir, options.minidump_dir))

def random_bytes(num):
  return ''.join(chr(random.randint(0, 255)) for _ in range(num))

def write_minidump(common_data, timestamp, target_dir):
  '''Generate and write the minidump into the target_dir. atime and mtime of the minidump
  will be set to timestamp.'''
  file_name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(10))
  with open(os.path.join(target_dir, file_name), 'wb') as f:
    # We want the minidump to be pretty similar to each other. The number 8192 was chosen
    # arbitratily and seemed like a reasonable guess.
    unique_data = random_bytes(8192)
    f.write(unique_data)
    f.write(common_data)
  os.utime(os.path.join(target_dir, file_name), (timestamp, timestamp))

def generate_minidumps():
  if options.start_time is None or options.end_time is None:
    start_timestamp = time.time() - options.duration
    end_timestamp = time.time()
  else:
    start_timestamp = options.start_time
    end_timestamp = options.end_time
  minidump_dir = options.minidump_dir
  if not os.path.isabs(minidump_dir):
    minidump_dir = os.path.join(options.log_dir, minidump_dir)
  if os.path.exists(minidump_dir):
    shutil.rmtree(minidump_dir)
  for role_name in ROLE_NAMES:
    os.makedirs(os.path.join(minidump_dir, role_name))
    # We want the files to have a high compression ratio and be several megabytes in size.
    # The parameters below should accomplish this.
    repeated_token = random_bytes(256)
    common_data = repeated_token * 40000
    if options.num_minidumps == 1:
      interval = 0
    else:
      interval = (end_timestamp - start_timestamp) // (options.num_minidumps - 1)
    for i in range(options.num_minidumps):
      write_minidump(common_data,
          start_timestamp + interval * i,
          os.path.join(minidump_dir, role_name))

def main():
  generate_conf_files()
  generate_minidumps()

if __name__ == '__main__':
  main()
