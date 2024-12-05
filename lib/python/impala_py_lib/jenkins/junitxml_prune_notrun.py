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

"""
Some tests that produce JUnitXML include tests that did not run (i.e. status="notrun").
This script walks through the JUnitXML and removes these elements.
"""
from __future__ import absolute_import, division, print_function
from optparse import OptionParser
from xml.etree import ElementTree as ET
from xml.dom import minidom


def junitxml_prune_notrun(junitxml_filename):
    tree = ET.ElementTree()
    root = tree.parse(junitxml_filename)

    for testsuite in root.findall("testsuite"):
        # Get the list of notrun tests
        notrun_testcases = []
        for testcase in testsuite:
            status = testcase.attrib["status"]
            if status == 'notrun':
                notrun_testcases.append(testcase)
        # Get the total number of tests
        num_tests = int(testsuite.attrib["tests"])

        # There are two cases.
        # 1. No test from the testsuite ran. The whole testsuite can be pruned.
        # 2. Some test from the testsuite ran. The individual testcases can be pruned.
        if len(notrun_testcases) == num_tests:
            # Remove whole testsuite
            root.remove(testsuite)
        else:
            # Remote individual testcases.
            for testcase in notrun_testcases:
                testsuite.remove(testcase)
            # Fixup the total number of tests
            testsuite.attrib["tests"] = str(num_tests - len(notrun_testcases))
    # Write out the pruned JUnitXML
    # An XML declaration is optional, but it is nice to have. ElementTree.write() does
    # not support an XML declaration on Python 2.6, so use minidom to write the XML.
    root_node_minidom = minidom.parseString(ET.tostring(root))
    junitxml_string = root_node_minidom.toxml(encoding="utf-8")
    with open(junitxml_filename, "wb") as f:
        f.write(junitxml_string)


def main():
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename", help="JUnitXML file to prune")
    options, args = parser.parse_args()
    junitxml_prune_notrun(options.filename)


if __name__ == "__main__": main()
