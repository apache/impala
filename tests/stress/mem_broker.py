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

from __future__ import absolute_import, division, print_function
import logging
import os
from contextlib import contextmanager
from multiprocessing import Value
from time import sleep

from tests.stress.util import increment

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


class MemBroker(object):
  """Provides memory usage coordination for clients running in different processes.
  The broker fulfills reservation requests by blocking as needed so total memory
  used by clients never exceeds the total available memory (including an
  'overcommitable' amount).

  The lock built in to _available is also used to protect access to other members.

  The state stored in this class is actually an encapsulation of part of the state
  of the StressRunner class below. The state here is separated for clarity.
  """

  def __init__(self, real_mem_mb, overcommitable_mem_mb):
    """'real_mem_mb' memory should be the amount of memory that each impalad is able
    to use. 'overcommitable_mem_mb' is the amount of memory that will be dispensed
    over the 'real' amount.
    """
    self._total_mem_mb = real_mem_mb + overcommitable_mem_mb
    self._available = Value("i", self._total_mem_mb)
    self._max_overcommitment = overcommitable_mem_mb

    # Each reservation will be assigned an id. Ids are monotonically increasing. When
    # a reservation crosses the overcommitment threshold, the corresponding reservation
    # id will be stored in '_last_overcommitted_reservation_id' so clients can check
    # to see if memory was overcommitted since their reservation was made (this is a race
    # but an incorrect result will be on the conservative side).
    self._next_reservation_id = Value("L", 0)
    self._last_overcommitted_reservation_id = Value("L", 0)

  @property
  def total_mem_mb(self):
    return self._total_mem_mb

  @property
  def overcommitted_mem_mb(self):
    return max(self._max_overcommitment - self._available.value, 0)

  @property
  def available_mem_mb(self):
    return self._available.value

  @property
  def last_overcommitted_reservation_id(self):
    return self._last_overcommitted_reservation_id.value

  @contextmanager
  def reserve_mem_mb(self, mem_mb):
    """Blocks until the requested amount of memory is available and taken for the caller.
    This function should be used in a 'with' block. The taken memory will
    automatically be released when the 'with' context exits. A numeric id is returned
    so clients can compare against 'last_overcommitted_reservation_id' to see if
    memory was overcommitted since the reservation was obtained.

    with broker.reserve_mem_mb(100) as reservation_id:
      # Run query using 100 MB of memory
      if <query failed>:
        # Immediately check broker.was_overcommitted(reservation_id) to see if
        # memory was overcommitted.
    """
    reservation_id = self._wait_until_reserved(mem_mb)
    try:
      yield reservation_id
    finally:
      self._release(mem_mb)

  def _wait_until_reserved(self, req):
    while True:
      with self._available.get_lock():
        if req <= self._available.value:
          self._available.value -= req
          LOG.debug(
              "Reserved %s MB; %s MB available; %s MB overcommitted",
              req, self._available.value, self.overcommitted_mem_mb)
          reservation_id = self._next_reservation_id.value
          increment(self._next_reservation_id)
          if self.overcommitted_mem_mb > 0:
            self._last_overcommitted_reservation_id.value = reservation_id
          return reservation_id
      sleep(0.1)

  def _release(self, req):
    with self._available.get_lock():
      self._available.value += req
      LOG.debug(
          "Released %s MB; %s MB available; %s MB overcommitted",
          req, self._available.value, self.overcommitted_mem_mb)

  def was_overcommitted(self, reservation_id):
    """Returns True if memory was overcommitted since the given reservation was made.
    For an accurate return value, this should be called just after the query ends
    or while the query is still running.
    """
    return reservation_id <= self._last_overcommitted_reservation_id.value
