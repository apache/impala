# Copyright (c) Cloudera 2012.

from subprocess import Popen, PIPE
import time

now = time.time()
TIMEOUT_SECONDS = 30.0

while time.time() - now < TIMEOUT_SECONDS:
  print "Polling /hbase/rs"
  p = Popen(["java",
             "org.apache.zookeeper.ZooKeeperMain",
             "-server",
             "localhost:2181",
             "get",
             "/hbase/rs"], stderr=PIPE, stdout=PIPE)
  out, err = p.communicate()
  if "Could not find the main class: org.apache.zookeeper.ZooKeeperMain" in err:
    print """CLASSPATH does not contain org.apache.zookeeper.ZooKeeperMain.
          Please check your CLASSPATH"""
    exit(1)

  if "numChildren" in err:
    print "HBase master is up, found in %2.1fs" % (time.time() - now,)
    exit(0)

  time.sleep(0.5)

print "Hbase master did NOT write /hbase/rs in %2.1fs" % (time.time() - now,)
exit(1)
