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

# Utility functions used by the stress test that don't fit in the other modules.

from __future__ import absolute_import, division, print_function

import sys
import threading
import traceback


def create_and_start_daemon_thread(fn, name):
  thread = threading.Thread(target=fn, name=name)
  thread.error = None
  thread.daemon = True
  thread.start()
  return thread


def increment(counter):
  """Increment a multiprocessing.Value object."""
  with counter.get_lock():
    counter.value += 1


def print_stacks(*_):
  """Print the stacks of all threads from this script to stderr."""
  thread_names = dict([(t.ident, t.name) for t in threading.enumerate()])
  stacks = list()
  for thread_id, stack in sys._current_frames().items():
    stacks.append(
        "\n# Thread: %s(%d)"
        % (thread_names.get(thread_id, "No name"), thread_id))
    for filename, lineno, name, line in traceback.extract_stack(stack):
      stacks.append('File: "%s", line %d, in %s' % (filename, lineno, name))
      if line:
        stacks.append("  %s" % (line.strip(), ))
  print("\n".join(stacks), file=sys.stderr)
