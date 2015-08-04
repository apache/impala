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

def exec_process(cmd):
  """Executes a subprocess, waiting for completion. The process exit code, stdout and
  stderr are returned as a tuple."""
  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.  The first element is the command,
  # with the rest being arguments.
  p = exec_process_async(cmd)
  stdout, stderr = p.communicate()
  rc = p.returncode
  return rc, stdout, stderr

def exec_process_async(cmd):
  """Executes a subprocess, returning immediately. The process object is returned for
  later retrieval of the exit code etc. """
  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.  The first element is the command,
  # with the rest being arguments.
  return Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)
