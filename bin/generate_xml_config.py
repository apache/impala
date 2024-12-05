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
Script which uses a Python "template" to generate a Hadoop-style XML
configuration file.

The "template" is a Python module which should export a global variable called
'CONFIG'. This variable should be a dictionary of keys/values. The values may
use the special syntax '${FOO}' to substitute an environment variable (as a
convenience over manually implementing the same).

If you have an existing XML configuration and want to see it in convenient
python form, you can use a snippet like the following from within the Python
REPL:

  import xml.etree.ElementTree as ET
  import pprint
  def convert(path):
    e = ET.parse(path)
    c = dict([(property.findtext('name'), property.findtext('value'))
              for property in e.getroot()])
    pprint.pprint(c, stream=file(path + ".py", "w"))

"""

from __future__ import absolute_import, division, print_function
import imp
import os
import re
import sys
from textwrap import dedent
from xml.sax.saxutils import escape as xmlescape

ENV_VAR_RE = re.compile(r'\${(.+?)\}')


def _substitute_env_vars(s):
  """ Substitute ${FOO} with the $FOO environment variable in 's' """
  def lookup_func(match):
    return os.environ[match.group(1)]
  return ENV_VAR_RE.sub(lookup_func, s)


def dump_config(d, source_path, out):
  """
  Dump a Hadoop-style XML configuration file.

  'd': a dictionary of name/value pairs.
  'source_path': the path where 'd' was parsed from.
  'out': stream to write to
  """
  header = """\
      <?xml version="1.0"?>
      <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      <!--

      NOTE: THIS FILE IS AUTO-GENERATED FROM:
        {source_path}

      EDITS BY HAND WILL BE LOST!

      -->
      <configuration>""".format(source_path=os.path.abspath(source_path))
  print(dedent(header), file=out)
  for k, v in sorted(d.items()):
    try:
      k_new = _substitute_env_vars(k)
      if isinstance(v, int):
        v = str(v)
      v_new = _substitute_env_vars(v)
    except KeyError as e:
      raise Exception("failed environment variable substitution for value {k}: {e}"
                      .format(k=k, e=e))
    print("""\
      <property>
        <name>{name}</name>
        <value>{value}</value>
      </property>""".format(name=xmlescape(k_new), value=xmlescape(v_new)), file=out)
  print("</configuration>", file=out)


def main():
  if len(sys.argv) != 3:
    print("usage: {prog} <template> <out>".format(prog=sys.argv[0]), file=sys.stderr)
    sys.exit(1)

  _, in_path, out_path = sys.argv
  try:
    mod = imp.load_source('template', in_path)
  except:  # noqa
    print("Unable to load template: %s" % in_path, file=sys.stderr)
    raise
  conf = mod.__dict__.get('CONFIG')
  if not isinstance(conf, dict):
    raise Exception("module in '{path}' should define a dict named CONFIG"
                    .format(path=in_path))

  tmp_path = out_path + ".tmp"
  with open(tmp_path, "w") as out:
    try:
      dump_config(conf, in_path, out)
    except:  # noqa
      os.unlink(tmp_path)
      raise
  os.rename(tmp_path, out_path)


if __name__ == "__main__":
  main()
