#!/usr/bin/env ambari-python-wrap

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

# Apache RAT is a tool for checking license compliance. This is a script that uses Apache
# RAT to check licenses in Impala.
#
# It takes as command line parameters two file names - the first is the name of a file
# containing globs of files to ignore, and the second is the XML output of RAT.
#
# I tested this with
#
#    git archive --prefix=foo/ -o test-impala.zip HEAD
#    java -jar apache-rat-0.12.jar -x test-impala.zip >rat.xml
#    bin/check-rat-report.py bin/rat_exclude_files.txt rat.xml
#
# This is copied from a similar file in Apache Kudu. Only RAT 0.12 is supported at this
# time, and the RAT JAR is not included in the Impala repo; it must be downloaded
# separately.

from __future__ import absolute_import, division, print_function
import fnmatch
import re
import sys
import xml.etree.ElementTree as ET

if len(sys.argv) != 3:
  sys.stderr.write("Usage: %s exclude_globs.lst rat_report.xml\n" % (sys.argv[0],))
  sys.exit(1)

exclude_globs_filename = sys.argv[1]
xml_filename = sys.argv[2]

globs = [line.strip() for line in open(exclude_globs_filename, "r") if "# " != line[0:2]]

tree = ET.parse(xml_filename)
root = tree.getroot()
all_ok = True

resources = root.findall('resource')
for r in resources:
  approvals = r.findall('license-approval')
  if approvals and approvals[0].attrib['name'] == 'true':
    continue
  clean_name = re.sub('^[^/]+/', '', r.attrib['name'])
  excluded = False
  for g in globs:
    if fnmatch.fnmatch(clean_name, g):
      excluded = True
      break
  if not excluded:
    typename = r.findall('type')[0].attrib['name']
    if not (clean_name[0:9] == 'testdata/' and typename in ['archive', 'binary']
            and clean_name[-4:] != '.jar'):
      sys.stderr.write(
          "%s: %s\n" %
          ('UNAPPROVED' if approvals else "NO APPROVALS; " + typename, clean_name))
      all_ok = False

if not all_ok:
  sys.exit(1)

print('OK')
sys.exit(0)
