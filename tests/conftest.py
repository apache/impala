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

# py.test configuration module
#
from impala.dbapi import connect as impala_connect
from kudu import connect as kudu_connect
from random import choice, sample
from string import ascii_lowercase, digits
from zlib import crc32
import contextlib
import logging
import os
import pytest

from common.test_result_verifier import QueryTestResult
from tests.common.patterns import is_valid_impala_identifier
from tests.comparison.db_connection import ImpalaConnection
from tests.util.filesystem_utils import FILESYSTEM, ISILON_WEBHDFS_PORT

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('test_configuration')

DEFAULT_CONN_TIMEOUT = 45
DEFAULT_EXPLORATION_STRATEGY = 'core'
DEFAULT_HDFS_XML_CONF = os.path.join(os.environ['HADOOP_CONF_DIR'], "hdfs-site.xml")
DEFAULT_HIVE_SERVER2 = 'localhost:11050'
DEFAULT_IMPALAD_HS2_PORT = '21050'
DEFAULT_IMPALADS = "localhost:21000,localhost:21001,localhost:21002"
DEFAULT_KUDU_MASTER_HOSTS = os.getenv('KUDU_MASTER_HOSTS', '127.0.0.1')
DEFAULT_KUDU_MASTER_PORT = os.getenv('KUDU_MASTER_PORT', '7051')
DEFAULT_METASTORE_SERVER = 'localhost:9083'
DEFAULT_NAMENODE_ADDR = None
if FILESYSTEM == 'isilon':
  DEFAULT_NAMENODE_ADDR = "{node}:{port}".format(node=os.getenv("ISILON_NAMENODE"),
                                                 port=ISILON_WEBHDFS_PORT)


def pytest_addoption(parser):
  """Adds a new command line options to py.test"""
  parser.addoption("--exploration_strategy", default=DEFAULT_EXPLORATION_STRATEGY,
                   help="Default exploration strategy for all tests. Valid values: core, "
                   "pairwise, exhaustive.")

  parser.addoption("--workload_exploration_strategy", default=None,
                   help="Override exploration strategy for specific workloads using the "
                   "format: workload:exploration_strategy. Ex: tpch:core,tpcds:pairwise.")

  parser.addoption("--impalad", default=DEFAULT_IMPALADS,
                   help="A comma-separated list of impalad host:ports to target. Note: "
                   "Not all tests make use of all impalad, some tests target just the "
                   "first item in the list (it is considered the 'default'")

  parser.addoption("--impalad_hs2_port", default=DEFAULT_IMPALAD_HS2_PORT,
                   help="The impalad HiveServer2 port.")

  parser.addoption("--metastore_server", default=DEFAULT_METASTORE_SERVER,
                   help="The Hive Metastore server host:port to connect to.")

  parser.addoption("--hive_server2", default=DEFAULT_HIVE_SERVER2,
                   help="Hive's HiveServer2 host:port to connect to.")

  parser.addoption("--kudu_master_hosts", default=DEFAULT_KUDU_MASTER_HOSTS,
                   help="Kudu master. Can be supplied as hostname, or hostname:port.")

  parser.addoption("--minicluster_xml_conf", default=DEFAULT_HDFS_XML_CONF,
                   help="The full path to the HDFS xml configuration file")

  parser.addoption("--namenode_http_address", default=DEFAULT_NAMENODE_ADDR,
                   help="The host:port for the HDFS Namenode's WebHDFS interface. Takes"
                   " precedence over any configuration read from --minicluster_xml_conf")

  parser.addoption("--update_results", action="store_true", default=False,
                   help="If set, will generate new results for all tests run instead of "
                   "verifying the results.")

  parser.addoption("--table_formats", dest="table_formats", default=None,
                   help="Override the test vectors and run only using the specified "
                   "table formats. Ex. --table_formats=seq/snap/block,text/none")

  parser.addoption("--scale_factor", dest="scale_factor", default=None,
                   help="If running on a cluster, specify the scale factor"
                   "Ex. --scale_factor=500gb")

# KERBEROS TODO: I highly doubt that the default is correct.  Try "hive".
  parser.addoption("--hive_service_name", dest="hive_service_name",
                   default="Hive Metastore Server",
                   help="The principal service name for the hive metastore client when "
                   "using kerberos.")

  parser.addoption("--use_kerberos", action="store_true", default=False,
                   help="use kerberos transport for running tests")

  parser.addoption("--sanity", action="store_true", default=False,
                   help="Runs a single test vector from each test to provide a quick "
                   "sanity check at the cost of lower test coverage.")

  parser.addoption("--skip_hbase", action="store_true", default=False,
                   help="Skip HBase tests")

  parser.addoption("--testing_remote_cluster", action="store_true", default=False,
                   help=("Indicates that tests are being run against a remote cluster. "
                         "Some tests may be marked to skip or xfail on remote clusters."))

  parser.addoption("--test_no_krpc", dest="test_no_krpc", action="store_true",
                   default=False, help="Run all tests with KRPC disabled. This assumes "
                   "that the test cluster has been started with --disable_krpc.")


def pytest_assertrepr_compare(op, left, right):
  """
  Provides a hook for outputting type-specific assertion messages

  Expected to return a list or strings, where each string will be printed as a new line
  in the result output report on assertion failure.
  """
  if isinstance(left, QueryTestResult) and isinstance(right, QueryTestResult) and \
     op == "==":
    result = ['Comparing QueryTestResults (expected vs actual):']
    for l, r in map(None, left.rows, right.rows):
      result.append("%s == %s" % (l, r) if l == r else "%s != %s" % (l, r))
    if len(left.rows) != len(right.rows):
      result.append('Number of rows returned (expected vs actual): '
                    '%d != %d' % (len(left.rows), len(right.rows)))

    # pytest has a bug/limitation where it will truncate long custom assertion messages
    # (it has a hardcoded string length limit of 80*8 characters). To ensure this info
    # isn't lost, always log the assertion message.
    LOG.error('\n'.join(result))
    return result

  # pytest supports printing the diff for a set equality check, but does not do
  # so well when we're doing a subset check. This handles that situation.
  if isinstance(left, set) and isinstance(right, set) and op == '<=':
    # If expected is not a subset of actual, print out the set difference.
    result = ['Items in expected results not found in actual results:']
    result.append(('').join(list(left - right)))
    LOG.error('\n'.join(result))
    return result


def pytest_xdist_setupnodes(config, specs):
  """Hook that is called when setting up the xdist plugin"""
  # Force the xdist plugin to be quiet. In verbose mode it spews useless information.
  config.option.verbose = 0


def pytest_generate_tests(metafunc):
  """
  This is a hook to parameterize the tests based on the input vector.

  If a test has the 'vector' fixture specified, this code is invoked and it will build
  a set of test vectors to parameterize the test with.
  """
  if 'vector' in metafunc.fixturenames:
    metafunc.cls.add_test_dimensions()
    vectors = metafunc.cls.ImpalaTestMatrix.generate_test_vectors(
        metafunc.config.option.exploration_strategy)
    if len(vectors) == 0:
      LOG.warning('No test vectors generated. Check constraints and input vectors')

    vector_names = map(str, vectors)
    # In the case this is a test result update or sanity run, select a single test vector
    # to run. This is okay for update_results because results are expected to be the same
    # for all test vectors.
    if metafunc.config.option.update_results or metafunc.config.option.sanity:
      vectors = vectors[0:1]
      vector_names = vector_names[0:1]
    metafunc.parametrize('vector', vectors, ids=vector_names)


@pytest.fixture
def testid_checksum(request):
  """
  Return a hex string representing the CRC32 checksum of the parametrized test
  function's full pytest test ID. The full pytest ID includes relative path, module
  name, possible test class name, function name, and any parameters.

  This could be combined with some prefix in order to form identifiers unique to a
  particular test case.
  """
  # For an example of what a full pytest ID looks like, see below (written as Python
  # multi-line literal)
  #
  # ("query_test/test_cancellation.py::TestCancellationParallel::()::test_cancel_select"
  #  "[table_format: avro/snap/block | exec_option: {'disable_codegen': False, "
  #  "'abort_on_error': 1, 'exec_single_node_rows_threshold': 0, 'batch_size': 0, "
  #  "'num_nodes': 0} | query_type: SELECT | cancel_delay: 3 | action: WAIT | "
  #  "query: select l_returnflag from lineitem]")
  return '{0:x}'.format(crc32(request.node.nodeid) & 0xffffffff)


@pytest.fixture
def unique_database(request, testid_checksum):
  """
  Return a database name unique to any test using the fixture. The fixture creates the
  database during setup, allows the test to know the database name, and drops the
  database after the test has completed.

  By default, the database name is a concatenation of the test function name and the
  testid_checksum. The database name prefix can be changed via parameter (see below).

  A good candidate for a test to use this fixture is a test that needs to have a
  test-local database or test-local tables that are created and destroyed at test run
  time. Because the fixture generates a unique name, tests using this fixture can be run
  in parallel as long as they don't need exclusion on other test-local resources.

  Sample usage:

    def test_something(self, vector, unique_database):
      # fixture creates database test_something_48A80F
      self.client.execute('DROP TABLE IF EXISTS `{0}`.`mytable`'.format(unique_database))
      # test does other stuff with the unique_database name as needed

  We also allow for parametrization:

    from tests.common.parametrize import UniqueDatabase

    @UniqueDatabase.parametrize(name_prefix='mydb', num_dbs=3, sync_ddl=True)
    def test_something(self, vector, unique_database):
      # fixture creates databases mydb_48A80F, mydb_48A80F2, mydb_48A80F3 with sync_ddl
      self.client.execute('DROP TABLE IF EXISTS `{0}`.`mytable`'.format(unique_database))
      # test does other stuff with the unique_database name as needed

  The supported parameters:

    name_prefix: string (defaults to test function __name__)
      - prefix to be used for the database name

    num_dbs: integer (defaults to 1)
      - number of unique databases to create
      - the name of the 2nd, 3rd, etc. databases are generated by appending "2", "3",
        etc., to the first database name (which does not have a "1" suffix)

    sync_ddl: boolean (defaults to False)
      - indicates whether the unique database should be created with sync_ddl

  For a similar DB-API 2 compliant connection/cursor that uses HS2 see the 'conn' and
  'unique_cursor' fixtures below.
  """

  # Test cases are at the function level, so no one should "accidentally" re-scope this.
  assert 'function' == request.scope, ('This fixture must have scope "function" since '
                                       'the fixture must guarantee unique per-test '
                                       'databases.')

  db_name_prefix = request.function.__name__
  sync_ddl = False
  num_dbs = 1
  fixture_params = getattr(request, 'param', None)
  if fixture_params is not None:
    if "name_prefix" in fixture_params:
      db_name_prefix = fixture_params["name_prefix"]
    if "sync_ddl" in fixture_params:
      sync_ddl = fixture_params["sync_ddl"]
    if "num_dbs" in fixture_params:
      num_dbs = fixture_params["num_dbs"]

  first_db_name = '{0}_{1}'.format(db_name_prefix, testid_checksum)
  db_names = [first_db_name]
  for i in range(2, num_dbs + 1):
    db_names.append(first_db_name + str(i))
  for db_name in db_names:
    if not is_valid_impala_identifier(db_name):
      raise ValueError('Unique database name "{0}" is not a valid Impala identifer; check'
                       ' test function name or any prefixes for long length or invalid '
                       'characters.'.format(db_name))

  def cleanup():
    # Make sure we don't try to drop the current session database
    request.instance.execute_query_expect_success(request.instance.client, "use default")
    for db_name in db_names:
      request.instance.execute_query_expect_success(
          request.instance.client, 'DROP DATABASE `{0}` CASCADE'.format(db_name),
          {'sync_ddl': sync_ddl})
      LOG.info('Dropped database "{0}" for test ID "{1}"'.format(
          db_name, str(request.node.nodeid)))

  request.addfinalizer(cleanup)

  for db_name in db_names:
    request.instance.execute_query_expect_success(
        request.instance.client, 'DROP DATABASE IF EXISTS `{0}` CASCADE'.format(db_name),
        {'sync_ddl': sync_ddl})
    request.instance.execute_query_expect_success(
        request.instance.client, 'CREATE DATABASE `{0}`'.format(db_name),
        {'sync_ddl': sync_ddl})
    LOG.info('Created database "{0}" for test ID "{1}"'.format(db_name,
                                                               str(request.node.nodeid)))
  return first_db_name


@pytest.yield_fixture
def kudu_client():
  """Provides a new Kudu client as a pytest fixture. The client only exists for the
     duration of the method it is used in.
  """
  kudu_master = pytest.config.option.kudu_master_hosts

  if "," in kudu_master:
    raise Exception("Multi-master not supported yet")
  if ":" in kudu_master:
    host, port = kudu_master.split(":")
  else:
    host, port = kudu_master, DEFAULT_KUDU_MASTER_PORT
  kudu_client = kudu_connect(host, port)
  yield kudu_client

  try:
    kudu_client.close()
  except Exception as e:
    LOG.warn("Error closing Kudu client: %s", e)


@pytest.yield_fixture(scope="class")
def conn(request):
  """Provides a new DB-API compliant connection to Impala as a pytest fixture. The
     same connection is used for all test methods in a class. The class may provide the
     following customizations:
       - get_db_name(): The name of the database to connect to.
       - auto_create_db(): If declared and the method returns True, the database will
         be created before tests run and dropped afterwards. If a database name is
         provided by get_db_name(), it must not exist. Classes that use both
         auto_create_db() and get_db_name() should generate a random name in
         get_db_name() and cache it.
       - get_conn_timeout(): The timeout, in seconds, to use for this connection.
     The returned connection will have a 'db_name' property.

     See the 'unique_database' fixture above if you want to use Impala's custom python
     API instead of DB-API.
  """
  db_name = __call_cls_method_if_exists(request.cls, "get_db_name")
  use_unique_conn = __call_cls_method_if_exists(request.cls, "auto_create_db")
  timeout = __call_cls_method_if_exists(request.cls, "get_conn_timeout") or \
      DEFAULT_CONN_TIMEOUT
  if use_unique_conn:
    with __unique_conn(db_name=db_name, timeout=timeout) as conn:
      yield conn
  else:
    with __auto_closed_conn(db_name=db_name) as conn:
      yield conn


def __call_cls_method_if_exists(cls, method_name):
  """Returns the result of calling the method 'method_name' on class 'class' if the class
     defined such a method, otherwise returns None.
  """
  method = getattr(cls, method_name, None)
  if method:
    return method()


@contextlib.contextmanager
def __unique_conn(db_name=None, timeout=DEFAULT_CONN_TIMEOUT):
  """Connects to Impala and creates a new database, then returns a connection to it.
     This is intended to be used in a "with" block. Upon exit, the database will be
     dropped. A database name can be provided by 'db_name', a database by that name
     must not exist prior to calling this method.

     with __unique_conn() as conn:
       # Use conn
     # The database no longer exists and the conn is closed.

     The returned connection will have a 'db_name' property.
  """
  if not db_name:
    db_name = choice(ascii_lowercase) + "".join(sample(ascii_lowercase + digits, 5))
  with __auto_closed_conn() as conn:
    with __auto_closed_cursor(conn) as cur:
      cur.execute("CREATE DATABASE %s" % db_name)
  with __auto_closed_conn(db_name=db_name, timeout=timeout) as conn:
    try:
      yield conn
    finally:
      try:
        with __auto_closed_cursor(conn) as cur:
          try:
            cur.execute("USE DEFAULT")
            cur.execute("DROP DATABASE IF EXISTS %s CASCADE" % db_name)
          except Exception as e:
            LOG.warn("Error dropping database: %s", e)
      except Exception as e:
        LOG.warn("Error creating a cursor: %s", e)


@contextlib.contextmanager
def __auto_closed_conn(db_name=None, timeout=DEFAULT_CONN_TIMEOUT):
  """Returns a connection to Impala. This is intended to be used in a "with" block.
     The connection will be closed upon exiting the block.

     The returned connection will have a 'db_name' property.
  """
  default_impalad = pytest.config.option.impalad.split(',')[0]
  impalad_host = default_impalad.split(':')[0]
  hs2_port = pytest.config.option.impalad_hs2_port

  conn = impala_connect(host=impalad_host, port=hs2_port, database=db_name,
                        timeout=timeout)
  try:
    conn.db_name = db_name
    yield conn
  finally:
    try:
      conn.close()
    except Exception as e:
      LOG.warn("Error closing Impala connection: %s", e)


@pytest.yield_fixture
def cursor(conn):
  """Provides a new DB-API compliant cursor from a connection provided by the conn()
     fixture. The cursor only exists for the duration of the method it is used in.

     The returned cursor will have a 'conn' property. The 'conn' will have a 'db_name'
     property.
  """
  with __auto_closed_cursor(conn) as cur:
    yield cur


@pytest.yield_fixture(scope="class")
def cls_cursor(conn):
  """Provides a new DB-API compliant cursor from a connection provided by the conn()
     fixture. The cursor exists for the duration of the class it is used in.

     The returned cursor will have a 'conn' property. The 'conn' will have a 'db_name'
     property.
  """
  with __auto_closed_cursor(conn) as cur:
    yield cur


@pytest.yield_fixture
def unique_cursor():
  """Provides a new DB-API compliant cursor to a newly created Impala database. The
     cursor only exists for the duration of the method it is used in. The database will
     be dropped after the test executes.

     The returned cursor will have a 'conn' property. The 'conn' will have a 'db_name'
     property.
  """
  with __unique_conn() as conn:
    with __auto_closed_cursor(conn) as cur:
      yield cur


@contextlib.contextmanager
def __auto_closed_cursor(conn):
  """Returns a cursor created from conn. This is intended to be used in a "with" block.
     The cursor will be closed upon exiting the block.
  """
  cursor = conn.cursor()
  cursor.conn = conn
  try:
    yield cursor
  finally:
    try:
      cursor.close()
    except Exception as e:
      LOG.warn("Error closing Impala cursor: %s", e)


@pytest.yield_fixture
def impala_testinfra_cursor():
  """
  Return ImpalaCursor object. Used for "tests of tests" for the infra for the query
  generator, stress test, etc.
  """
  # This differs from the cursors above, which return direct Impyla cursors. Tests that
  # use this fixture want to interact with the objects in
  # tests.comparison.db_connection, which need testing.
  with ImpalaConnection() as conn:
    cursor = conn.cursor()
    try:
      yield cursor
    finally:
      cursor.close()


@pytest.fixture(autouse=True, scope='session')
def validate_pytest_config():
  """
  Validate that pytest command line options make sense.
  """
  if pytest.config.option.testing_remote_cluster:
    local_prefixes = ('localhost', '127.', '0.0.0.0')
    if any(pytest.config.option.impalad.startswith(loc) for loc in local_prefixes):
      logging.error("--testing_remote_cluster can not be used with a local impalad")
      pytest.exit("Invalid pytest config option: --testing_remote_cluster")
