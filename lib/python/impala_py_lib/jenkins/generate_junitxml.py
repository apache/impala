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
import textwrap
from xml.dom import minidom
from xml.etree import ElementTree as ET

from datetime import datetime as dt

IMPALA_HOME = os.getenv('IMPALA_HOME', '.')
SCRIPT_NAME, _ = os.path.splitext(os.path.basename(__file__))
JUNITXML_LOGDIR = os.path.join(os.getenv("IMPALA_LOGS_DIR", "."), 'extra_junit_xml_logs')


class JunitReport(object):
  """A Junit XML style report parseable by Jenkins for reporting build status.

  Generally, a caller who invokes this script from bash doesn't need to do
  more than supply the necessary command line parameters. The JunitReport
  class is instantiated using those initial inputs, and a timestamped XML
  file is output to the $IMPALA_HOME/logs/extra_junit_xml_logs/.

  Log files are timestamped, so they will not overwrite previous files containing
  output of the same step.

  For use from within a python script (must be invoked with impala-python), an
  example might look like:

  >>> from impala_py_lib.jenkins.generate_junitxml import JunitReport
  >>> report = JunitReport(phase='load_data', step='load_hbase', error_msg='oops')
  >>> report.tofile()

  For now, the class does not support adding more than one step (analogous to a
  test case) to the same phase (analogous to a test suite). Each report should
  be unique for a given junit XML file. This may be enhanced at some point.
  """

  def __init__(self, phase, step, error_msg=None, stdout=None, stderr=None,
               elapsed_time=0):

    self.root_element = None
    self.testsuite_element = None
    self.testcase_element = None

    self.phase = phase
    self.step = step
    self.error_msg = error_msg
    self.stdout = stdout
    self.stderr = stderr
    self.elapsed_time = elapsed_time
    self.utc_time = dt.utcnow()

    self.create_root_element()
    self.add_testsuite_element()
    self.add_testcase_element()

    if self.error_msg is not None:
      self.set_error()

    if self.stdout is not None:
      self.add_output('out', self.stdout)

    if self.stderr is not None:
      self.add_output('err', self.stderr)

  def create_root_element(self):
    """Create the testsuites root element."""
    self.root_element = ET.Element("testsuites")
    self.root_element.set("time", "{0:.1f}".format(float(self.elapsed_time)))
    self.root_element.set("tests", "1")
    self.root_element.set("failures", "0")
    self.root_element.set("errors", "0")

  def add_testsuite_element(self):
    """Create the testsuite element."""
    self.testsuite_element = ET.SubElement(self.root_element, "testsuite")
    self.testsuite_element.set("name", "{name}.{phase}.{step}".format(
      name=SCRIPT_NAME, phase=self.phase, step=self.step))
    self.testsuite_element.set(
      "timestamp", "{ts}+00:00".format(ts=self.utc_time.strftime('%Y-%m-%d %H:%M:%S')))
    self.testsuite_element.set("disabled", "0")
    self.testsuite_element.set("errors", "0")
    self.testsuite_element.set("failures", "0")
    self.testsuite_element.set("skipped", "0")
    self.testsuite_element.set("tests", "1")
    self.testsuite_element.set("time", "0")
    self.testsuite_element.set("file", "None")
    self.testsuite_element.set("log", "None")
    self.testsuite_element.set("url", "None")

  def add_testcase_element(self):
    """Create the testcase element."""
    self.testcase_element = ET.SubElement(self.testsuite_element, "testcase")
    self.testcase_element.set("classname", "{name}.{phase}".format(
      name=SCRIPT_NAME, phase=self.phase))
    self.testcase_element.set("name", self.step)

  def set_error(self):
    """Set an error msg if the step failed, and increment necessary error attributes."""
    error = ET.SubElement(self.testcase_element, "error")
    error.set("message", self.error_msg)
    error.set("type", "error")
    self.testsuite_element.set("errors", "1")
    self.root_element.set("errors", "1")

  def add_output(self, output_type, file_or_string):
    """
    Add stdout or stderr content to testcase element.

    Args:
      output_type: [string] either out or err
      file_or_string: a path to a file containing the content, or a plain string
    """
    output = ET.SubElement(self.testcase_element,
        "system-{output_type}".format(output_type=output_type))
    output.text = JunitReport.get_xml_content(file_or_string)

  def to_file(self, junitxml_logdir=JUNITXML_LOGDIR):
    """
    Create a timestamped XML report file.

    Args:
      junitxml_logdir: path to directory where the file will be created

    Return:
      junit_log_file: path to the generated file
    """
    # The equivalent of mkdir -p
    try:
      os.makedirs(junitxml_logdir)
    except OSError as e:
      if e.errno == errno.EEXIST and os.path.isdir(junitxml_logdir):
        pass
      else:
        raise

    filename = '{name}.{ts}.xml'.format(
        name=self.testsuite_element.attrib['name'],
        ts=self.utc_time.strftime('%Y%m%d_%H_%M_%S')
    )
    junit_log_file = os.path.join(junitxml_logdir, filename)

    with open(junit_log_file, 'w') as f:
      f.write(str(self))

    return junit_log_file

  @staticmethod
  def get_xml_content(file_or_string=None):
    """
    Derive additional content for the XML report.

    If the supplied parameter is the path to a file, the contents will be inserted
    into the XML report. If the parameter is just plain string, use that as the
    content for the report.

    Args:
      file_or_string: a path to a file, or a plain string

    Returns:
      content as a string
    """
    if file_or_string is None:
      content = ''
    elif os.path.exists(file_or_string):
      with open(file_or_string, 'r') as f:
        content = f.read()
    else:
      content = file_or_string
    return content

  def __str__(self):
    """
    Generate and return a pretty-printable XML string.
    """
    root_node_str = minidom.parseString(ET.tostring(self.root_element))
    return root_node_str.toprettyxml(indent=' ' * 4)


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
  Create a report for each invocation of the script, and output the results
  of the test case to an XML file within $IMPALA_HOME/logs/extra_junit_xml_logs.
  The log file name will use "phase" and "step" values provided on the command
  line to structure the report. The XML report filename will follow the form:

    junitxml_logger.<phase>.<step>.<time_stamp>.xml

  Phase can be repeated in a given test run, but the step leaf node, which is
  equivalent to a "test case", must be unique within each phase.
  """
  options = get_options()

  junit_report = JunitReport(phase=options.phase,
                             step=options.step,
                             error_msg=options.error,
                             stdout=options.stdout,
                             stderr=options.stderr,
                             elapsed_time=options.time)

  junit_log_file = junit_report.to_file()
  print("Generated: {0}".format(junit_log_file))


if "__main__" == __name__:
  main()
