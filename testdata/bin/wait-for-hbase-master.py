#!/usr/bin/env impala-python
# Copyright (c) Cloudera 2012.

from subprocess import Popen, PIPE
import os
import re
import sys
import time

now = time.time()
TIMEOUT_SECONDS = 30.0
ZK_CLASS="org.apache.zookeeper.ZooKeeperMain"

print "Waiting for HBase Master"

while time.time() - now < TIMEOUT_SECONDS:
  sys.stdout.write(".")
  sys.stdout.flush()

  p = Popen([os.environ["JAVA"],
             ZK_CLASS,
             "-server",
             "localhost:2181",
             "get",
             "/hbase/rs"], stderr=PIPE, stdout=PIPE)
  out, err = p.communicate()
  if re.match(".*" + ZK_CLASS + "\w*$", err):
    print "Failure"
    print err
    print "CLASSPATH does not contain " + ZK_CLASS
    print "Please check your CLASSPATH"
    exit(1)

  if "numChildren" in err:
    print "Success"
    print "HBase master is up, found in %2.1fs" % (time.time() - now,)
    exit(0)

  time.sleep(0.5)

print "Failure"
print "Hbase master did NOT write /hbase/rs in %2.1fs" % (time.time() - now,)
exit(1)
