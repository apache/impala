#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
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

# This script will kill the FE processes (hive and hbase) if they are running.
import os,signal

# Java Process names to kill
fe_processes = ['Launcher', 'HMaster']

# Run jps.  The format of the output is <pid> <java proc name>
processes = os.popen("jps", "r")
while 1:
  line = processes.readline()
  if not line: break
  splits = line.split(' ')
  if len(splits) != 2: continue;
  pid = int(splits[0].strip())
  proc_name = splits[1].strip()
  if proc_name in fe_processes:
    os.kill(pid, 9)
