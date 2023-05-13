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

# This module contains tests for some of the tests/util code.

from __future__ import absolute_import, division, print_function
from tests.util.filesystem_utils import prepend_with_fs
from tests.util.parse_util import get_bytes_summary_stats_counter, \
    get_time_summary_stats_counter


def test_filesystem_utils():
  # Verify that empty FS prefix gives back the same path.
  path = "/fake-warehouse"
  assert prepend_with_fs("", path) == path

  # Verify that prepend_with_fs() is idempotent.
  fs = "fakeFs://bucket"
  path = "/fake-warehouse"
  assert prepend_with_fs(fs, path) == fs + path
  assert prepend_with_fs(fs, prepend_with_fs(fs, path)) == fs + path


def test_get_bytes_summary_stats_counter():
  """Test get_bytes_summary_stats_counter(counter_name, runtime_profile) using a dummy
     runtime profile.
  """
  runtime_profile = "- ExampleCounter: (Avg: 8.00 KB (8192) ; " \
                    "Min: 6.00 KB (6144) ; " \
                    "Max: 10.00 KB (10240) ; " \
                    "Number of samples: 4)"
  summary_stats = get_bytes_summary_stats_counter("ExampleCounter",
                                                  runtime_profile)
  assert len(summary_stats) == 1
  assert summary_stats[0].sum == 32768 and summary_stats[0].min_value == 6144 and \
         summary_stats[0].max_value == 10240 and summary_stats[0].total_num_values == 4


def test_get_time_summary_stats_counter():
  """Test get_time_summary_stats_counter(counter_name, runtime_profile) using a dummy
     runtime profile.
  """
  # This is constructed to test the parsing logic for timestamps, so the number don't
  # add up.
  runtime_profile = "- ExampleTimeStats: (Avg: 161.554ms ; " \
                    "Min: 101.411us ; " \
                    "Max: 1h2m3s4ms5us6ns ; " \
                    "Number of samples: 6)"
  summary_stats = get_time_summary_stats_counter("ExampleTimeStats", runtime_profile)
  assert len(summary_stats) == 1
  assert summary_stats[0].sum == 969324000
  assert summary_stats[0].min_value == 101411
  assert summary_stats[0].max_value == 3723004005006
  assert summary_stats[0].total_num_values == 6
