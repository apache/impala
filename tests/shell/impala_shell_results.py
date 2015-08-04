# encoding=utf-8
# Copyright 2014 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

PY_CMD = "%s/shell/impala_shell.py" % os.environ['IMPALA_HOME']

class ImpalaShellResult(object):
  def __init__(self):
    self.rc = 0
    self.stdout = str()
    self.stderr = str()

def get_shell_cmd_result(process, stdin_input=None):
  result = ImpalaShellResult()
  result.stdout, result.stderr = process.communicate(input=stdin_input)
  # We need to close STDIN if we gave it an input, in order to send an EOF that will
  # allow the subprocess to exit.
  if stdin_input is not None: process.stdin.close()
  result.rc = process.returncode
  return result
