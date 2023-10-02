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

from time import sleep


def retry(func, max_attempts=3, sleep_time_s=1, backoff=2, raise_immediately=False):
  """
  Repeatedly executes a provided function until the function either returns True or the
  maximum number of iterations is it.

  Inputs:
    func             - A function to execute, this function must take a single boolean
                       parameter which indicates if the current attempt is the last
                       attempt, the purpose of this parameter is to enable assertions to
                       run during the lastattempt so that better error messages a
                       provided.
    max_attempts      - The maximum number of times to run the function.
    sleep_time_s      - The number of seconds to sleep before retrying.
    backoff           - Defines the amount the the sleep time will be multiplied by after
                        each attempt. Specify a value of `1` if the same amount of time
                        should be used as the sleep between each attempt.
    raise_immediately - Controls the behavior of this function when an exception is raised
                        by the provided function to be executed. If an exception is raised
                        out of that function and this parameter is `True`, the exception
                        will be immediately raised up the stack. If this parameter is
                        `False`, then the exception is only raised if it is the last
                        attempt.

  Return:
    `True`  - Indicates the provided function returned `True`.
    `False` - Indicates the provided function returned `False` for each attempt.

  Raises:
    See the doc for the `raise_immediately` parameter.

  To use this function, code a separate named function to return `True` if it succeeds or
  `False` if it fails. If the named function returns True, this function will also return
  `True`.  If the named function returns `False`, this function will try again. If, at the
  last iteration, the named function returns `False`, this function returns `False`.

  If the named function raises an exception, the behavior of this function depends on the
  input parameter `raise_immediately`.  If set to `True`, the exception is
  immediately raised from this function.  If set to `False`, the exception is ignored,
  unless the maximum number of attempts is hit at which point the exception will be
  raised.
  """
  success = False
  iterations = 0
  e = None

  while(iterations < max_attempts):
    try:
      success = func(iterations == max_attempts - 1)

      if success:
        return True
    except Exception as func_e:
      e = func_e
      if raise_immediately:
        raise e

    sleep(sleep_time_s)
    iterations += 1
    sleep_time_s *= backoff

  print("max retries '{0}' exceeded".format(max_attempts))
  if e is not None:
    raise e

  return False
