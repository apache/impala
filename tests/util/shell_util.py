#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
#
# Utility functions related to executing shell commands.
import logging
import shlex
from subprocess import Popen, PIPE

logging.basicConfig(level=logging.ERROR, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('shell_util')
LOG.setLevel(level=logging.DEBUG)

def exec_shell_cmd(cmd, run_in_background=False):
  """Executes a command in the shell, pipes the output to local variables"""
  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.  The first element is the command,
  # with the rest being arguments.
  p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)
  if not run_in_background:
    stdout, stderr = p.communicate()
    rc = p.returncode
    return rc, stdout, stderr
