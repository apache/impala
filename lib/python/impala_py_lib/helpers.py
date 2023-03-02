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

from __future__ import absolute_import, division, print_function
import fnmatch
import logging
import os
import re
import subprocess

LOG = logging.getLogger('impala_lib_python_helpers')


def exec_local_command(cmd):
  """
  Executes a command for the local bash shell and return stdout as a string.

  Raise CalledProcessError in case of  non-zero return code.

  Args:
    cmd: command as a string

  Return:
    STDOUT
  """
  proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = proc.communicate()
  retcode = proc.poll()
  if retcode:
    LOG.error("{0} returned status {1}: {2}".format(cmd, retcode, error))
    raise subprocess.CalledProcessError()
  else:
    return output


def find_all_files(fname_pattern, base_dir=os.getenv('IMPALA_HOME', '.')):
  """
  General utility to recursively find files matching a certain unix-like file pattern.

  Args:
    fname_pattern: Unix glob
    base_dir: the root directory where searching should start

  Returns:
    A list of full paths relative to the give base_dir
  """
  file_glob = fnmatch.translate(fname_pattern)
  matching_files = []

  for root, dirs, files in os.walk(base_dir):
    matching_files += [os.path.join(root, f) for f in files if re.match(file_glob, f)]

  return matching_files


def is_core_dump(file_path):
  """
  Determine whether given file is a core file. Works on CentOS and Ubuntu.

  Args:
    file_path: full path to a possible core file
  """
  file_std_out = exec_local_command("file %s" % file_path)
  return "core file" in file_std_out and 'ELF' in file_std_out
