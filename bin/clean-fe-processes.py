#!/usr/bin/env python

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
