import pytest

import cli_options
from cluster import CmCluster, MiniCluster

"""This module provides pytest 'fixtures'. See cluster_tests.py for usage."""

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
