#!/usr/bin/env impala-python
# -*- coding: utf-8 -*-
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
from shell.impala_client import ImpalaBeeswaxClient, ImpalaHS2Client
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
  create_client_protocol_dimension, create_client_protocol_no_strict_dimension,
  create_uncompressed_text_dimension, create_single_exec_option_dimension)
from tests.shell.util import get_impalad_host_port


class TestShellClient(ImpalaTestSuite):
  """Tests for the Impala Shell clients: ImpalaBeeswaxClient and ImpalaHS2Client."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestShellClient, cls).add_test_dimensions()
    # Limit to uncompressed text with default exec options
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Run with beeswax and HS2
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_no_strict_dimension())

  def test_fetch_size(self, vector):
    """Tests that when result spooling is disabled, setting a small batch_size causes
    the shell to fetch a single batch at a time, even when the configured fetch size is
    larger than the batch_size."""
    handle = None
    num_rows = 100
    batch_size = 10
    query_options = {'batch_size': str(batch_size), 'spool_query_results': 'false'}
    client = self.__get_shell_client(vector)

    try:
      client.connect()
      handle = client.execute_query(
          "select * from functional.alltypes limit {0}".format(num_rows), query_options)
      self.__fetch_rows(client.fetch(handle), batch_size, num_rows)
    finally:
      if handle is not None: client.close_query(handle)
      client.close_connection()

  def test_fetch_size_result_spooling(self, vector):
    """Tests that when result spooling is enabled, that the exact fetch_size is honored
    even if a small batch_size is configured."""
    handle = None
    fetch_size = 20
    num_rows = 100
    query_options = {'batch_size': '10', 'spool_query_results': 'true'}
    client = self.__get_shell_client(vector, fetch_size)

    try:
      client.connect()
      handle = client.execute_query(
          "select * from functional.alltypes limit {0}".format(num_rows), query_options)
      self.__fetch_rows(client.fetch(handle), num_rows // fetch_size, num_rows)
    finally:
      if handle is not None: client.close_query(handle)
      client.close_connection()

  def __fetch_rows(self, fetch_batches, num_batches, num_rows):
    """Fetches all rows using the given fetch_batches generator. Asserts that num_batches
    batches are produced by the generator and that num_rows are returned."""
    num_batches_count = 0
    rows_per_batch = num_rows // num_batches
    for fetch_batch in fetch_batches:
      assert len(fetch_batch) == rows_per_batch
      num_batches_count += 1
      if num_batches_count == num_batches: break
    assert num_batches_count == num_batches

  def __get_shell_client(self, vector, fetch_size=1024):
    """Returns the client specified by the protocol in the given vector."""
    impalad = get_impalad_host_port(vector).split(":")
    protocol = vector.get_value("protocol")
    if protocol == 'hs2':
      return ImpalaHS2Client(impalad, fetch_size, None)
    elif protocol == 'hs2-http':
      return ImpalaHS2Client(impalad, fetch_size, None,
              use_http_base_transport=True, http_path='cliservice')
    elif protocol == 'beeswax':
      return ImpalaBeeswaxClient(impalad, fetch_size, None)
