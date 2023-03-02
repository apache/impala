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
import time
import traceback

from multiprocessing.pool import ThreadPool


class Task:
  """Helper class for parallel execution. Constructor stores a function with its
  parameters."""
  def __init__(self, func, *args, **kwargs):
    self.func = func
    self.args = args
    self.kwargs = kwargs

  def run(self):
    """Executes the function with the given parameters."""
    try:
      return self.func(*self.args, **self.kwargs)
    except Exception:
      traceback.print_exc()
      raise


def run_tasks(tasks, timeout_seconds=600):
  """Runs a list of Tasks in parallel in a thread pool."""
  start = time.time()
  pool = ThreadPool(processes=len(tasks))
  pool.map_async(Task.run, tasks).get(timeout_seconds)
  return time.time() - start
