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
import pytest

from tests.comparison import cli_options
from tests.comparison.cluster import CmCluster, MiniCluster


__cluster = None

def pytest_addoption(parser):
  if not hasattr(parser, "add_argument"):
    parser.add_argument = parser.addoption
  cli_options.add_cm_options(parser)


@pytest.fixture
def cluster(request):
  global __cluster
  if not __cluster:
    cm_host = get_option_value(request, "cm_host")
    if cm_host:
      __cluster = CmCluster(cm_host, port=get_option_value(request, "cm_port"),
          user=get_option_value(request, "cm_user"),
          password=get_option_value(request, "cm_password"),
          cluster_name=get_option_value(request, "cm_cluster_name"))
    else:
      __cluster = MiniCluster()
  return __cluster


@pytest.yield_fixture
def hive_cursor(request):
  with cluster(request).hive.connect() as conn:
    with conn.cursor() as cur:
      yield cur


@pytest.yield_fixture
def cursor(request):
  with cluster(request).impala.connect() as conn:
    with conn.cursor() as cur:
      yield cur


def get_option_value(request, dest_var_name):
  return request.config.getoption("--" + dest_var_name.replace("_", "-"))
