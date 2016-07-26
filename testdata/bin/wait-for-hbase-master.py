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
