#!/usr/bin/env python
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
A script for generating arbitrary junit XML reports while building Impala.
These files will be consumed by jenkins.impala.io to generate reports for
easier triaging of build and setup errors.
"""
import argparse
import errno
import os
import pytz
import textwrap

from datetime import datetime as dt
from junit_xml import TestSuite, TestCase

IMPALA_HOME = os.getenv('IMPALA_HOME', '.')


def get_xml_content(file_or_string=None):
  """
  Derive content for the XML report.

  Args:
    file_or_string: a path to a file, or a plain string

  If the supplied parameter is the path to a file, the contents will be inserted
  into the XML report. If the parameter is just plain string, use that as the
  content for the report.
  """
  if file_or_string is None:
    content = ''
  elif os.path.exists(file_or_string):
    with open(file_or_string, 'r') as f:
      content = f.read()
  else:
      content = file_or_string
  return content


def generate_xml_file(testsuite, junitxml_logdir='.'):
  """
  Create a timestamped XML report file.

  Args:
    testsuite: junit_xml.TestSuite object
    junitxml_logdir: path to directory where the file will be created

  Return:
    junit_log_file: path to the generated file
  """
  ts_string = testsuite.timestamp.strftime('%Y%m%d_%H_%M_%S')
  junit_log_file = os.path.join(junitxml_logdir,
                                '{}.{}.xml'.format(testsuite.name, ts_string))

  with open(junit_log_file, 'w') as f:
    TestSuite.to_file(f, [testsuite], prettyprint=True)

  return junit_log_file


def get_options():
  """Parse and return command line options."""
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  # Required options
  parser.add_argument("--phase",
                      default="buildall",
                      help="General build phase or script.")
  parser.add_argument("--step",
                      required=True,
                      help=textwrap.dedent(
                          """Specific build step or child script being run.
                          Each step must be unique for the given build phase.""")
                      )
  parser.add_argument("-t", "--time",
                      type=float,
                      default=0,
                      help="If known, the elapsed time in seconds for this step.")
  parser.add_argument("--stdout",
                      help=textwrap.dedent(
                          """Standard output to include in the XML report. Can be
                          either a string or the path to a file..""")
                      )
  parser.add_argument("--stderr",
                      help=textwrap.dedent(
                          """Standard error to include in the XML report. Can be
                          either a string or the path to a file.""")
                      )
  parser.add_argument("--error",
                      help=textwrap.dedent(
                          """If specified, the XML report will mark this as an error.
                          This should be a brief explanation for the error.""")
                      )

  return parser.parse_args()


def main():
  """
  Create a "testcase" for each invocation of the script, and output the results
  of the test case to an XML file within $IMPALA_HOME/logs/extra_junit_xml_logs.
  The log file name will use "phase" and "step" values provided on the command
  line to structure the report. The XML report filename will follow the form:

    junitxml_logger.<phase>.<step>.<time_stamp>.xml

  Phase can be repeated in a given test run, but the step leaf node, which is
  equivalent to a "test case", must be unique within each phase.
  """
  junitxml_logdir = os.path.join(IMPALA_HOME, 'logs', 'extra_junit_xml_logs')

  # The equivalent of mkdir -p
  try:
    os.makedirs(junitxml_logdir)
  except OSError as e:
    if e.errno == errno.EEXIST and os.path.isdir(junitxml_logdir):
      pass
    else:
      raise

  options = get_options()
  root_name, _ = os.path.splitext(os.path.basename(__file__))

  tc = TestCase(classname='{}.{}'.format(root_name, options.phase),
                name=options.step,
                elapsed_sec=options.time,
                stdout=get_xml_content(options.stdout),
                stderr=get_xml_content(options.stderr))

  # Specifying an error message for any step causes the buid to be marked as invalid.
  if options.error:
    tc.add_error_info(get_xml_content(options.error))
    assert tc.is_error()

  testsuite = TestSuite(name='{}.{}.{}'.format(root_name, options.phase, options.step),
                        timestamp=dt.utcnow().replace(tzinfo=pytz.UTC),
                        test_cases=[tc])

  xml_report = generate_xml_file(testsuite, junitxml_logdir)
  print("Generated: {}".format(xml_report))


if "__main__" == __name__:
  main()
