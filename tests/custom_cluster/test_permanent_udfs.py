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
import glob
import os
import pytest
import re
import shutil

from tempfile import mkdtemp
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.filesystem_utils import get_fs_path

class TestUdfPersistence(CustomClusterTestSuite):
  """ Tests the behavior of UDFs and UDAs between catalog restarts. With IMPALA-1748,
  these functions are persisted to the metastore and are loaded again during catalog
  startup"""

  DATABASE = 'udf_permanent_test'
  JAVA_FN_TEST_DB = 'java_permanent_test'
  HIVE_IMPALA_INTEGRATION_DB = 'hive_impala_integration_db'
  HIVE_UDF_JAR = os.getenv('DEFAULT_FS') + '/test-warehouse/hive-exec.jar';
  JAVA_UDF_JAR = os.getenv('DEFAULT_FS') + '/test-warehouse/impala-hive-udfs.jar';
  LOCAL_LIBRARY_DIR = mkdtemp(dir="/tmp")

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestUdfPersistence, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfPersistence, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    super(TestUdfPersistence, self).setup_method(method)
    impalad = self.cluster.impalads[0]
    self.client = impalad.service.create_beeswax_client()
    self.__cleanup()
    self.__load_drop_functions(
        self.CREATE_UDFS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libTestUdfs.so'))
    self.__load_drop_functions(
        self.DROP_SAMPLE_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libudasample.so'))
    self.__load_drop_functions(
        self.CREATE_SAMPLE_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libudasample.so'))
    self.__load_drop_functions(
        self.CREATE_TEST_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libTestUdas.so'))
    self.uda_count =\
        self.CREATE_SAMPLE_UDAS_TEMPLATE.count("create aggregate function") +\
        self.CREATE_TEST_UDAS_TEMPLATE.count("create aggregate function")
    self.udf_count = self.CREATE_UDFS_TEMPLATE.count("create function")
    self.client.execute("CREATE DATABASE IF NOT EXISTS %s" % self.JAVA_FN_TEST_DB)
    self.client.execute("CREATE DATABASE IF NOT EXISTS %s" %
        self.HIVE_IMPALA_INTEGRATION_DB)

  def teardown_method(self, method):
    self.__cleanup()

  def __cleanup(self):
    self.client.execute("DROP DATABASE IF EXISTS %s CASCADE" % self.DATABASE)
    self.client.execute("DROP DATABASE IF EXISTS %s CASCADE" % self.JAVA_FN_TEST_DB)
    self.client.execute("DROP DATABASE IF EXISTS %s CASCADE"
       % self.HIVE_IMPALA_INTEGRATION_DB)
    shutil.rmtree(self.LOCAL_LIBRARY_DIR, ignore_errors=True)

  def __load_drop_functions(self, template, database, location):
    queries = template.format(database=database, location=location)
    # Split queries and remove empty lines
    queries = [q for q in queries.split(';') if q.strip()]
    for query in queries:
      result = self.client.execute(query)
      assert result is not None

  def __restart_cluster(self):
    self._stop_impala_cluster()
    self._start_impala_cluster(list())
    impalad = self.cluster.impalads[0]
    self.client = impalad.service.create_beeswax_client()

  def verify_function_count(self, query, count):
    result = self.client.execute(query)
    assert result is not None and len(result.data) == count

  @pytest.mark.execute_serially
  def test_permanent_udfs(self):
    # Make sure the pre-calculated count tallies with the number of
    # functions shown using "show [aggregate] functions" statement
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.DATABASE), self.udf_count);
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), self.uda_count)
    # invalidate metadata and make sure the count tallies
    result = self.client.execute("INVALIDATE METADATA")
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.DATABASE), self.udf_count);
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), self.uda_count)
    # Restart the cluster, this triggers a full metadata reload
    self.__restart_cluster()
    # Make sure the counts of udfs and udas match post restart
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.DATABASE), self.udf_count);
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), self.uda_count)
    # Drop sample udas and verify the count matches pre and post restart
    self.__load_drop_functions(
        self.DROP_SAMPLE_UDAS_TEMPLATE, self.DATABASE,
        get_fs_path('/test-warehouse/libudasample.so'))
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), 1)
    self.__restart_cluster()
    self.verify_function_count(
            "SHOW AGGREGATE FUNCTIONS in {0}".format(self.DATABASE), 1)


  def __verify_udf_in_hive(self, udf):
    (query, result) = self.SAMPLE_JAVA_UDFS_TEST[udf]
    stdout = self.run_stmt_in_hive("select " + query.format(
        db=self.HIVE_IMPALA_INTEGRATION_DB))
    assert stdout is not None and result in str(stdout)

  def __verify_udf_in_impala(self, udf):
    (query, result) = self.SAMPLE_JAVA_UDFS_TEST[udf]
    stdout = self.client.execute("select " + query.format(
        db=self.HIVE_IMPALA_INTEGRATION_DB))
    assert stdout is not None and result in str(stdout.data)

  def __describe_udf_in_hive(self, udf, db=HIVE_IMPALA_INTEGRATION_DB):
    """ Describe the specified function, returning stdout. """
    # Hive 2+ caches UDFs, so we have to explicitly invalidate the UDF if
    # we've made changes on the Impala side.
    stmt = "RELOAD FUNCTION ; DESCRIBE FUNCTION {0}.{1}".format(db, udf)
    return self.run_stmt_in_hive(stmt)

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  def test_corrupt_java_udf(self):
    """ IMPALA-3820: This tests if the Catalog server can gracefully handle
    Java UDFs with unresolved dependencies."""
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    # Create a Java UDF with unresolved dependencies from Hive and
    # restart the Catalog server. Catalog should ignore the
    # function load.
    self.run_stmt_in_hive("create function %s.corrupt_udf as \
        'org.apache.impala.UnresolvedUdf' using jar '%s'"
        % (self.JAVA_FN_TEST_DB, self.JAVA_UDF_JAR))
    self.__restart_cluster()
    # Make sure the function count is 0
    self.verify_function_count(
        "SHOW FUNCTIONS in {0}".format(self.JAVA_FN_TEST_DB), 0)

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  def test_corrupt_java_bad_function(self):
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    """ IMPALA-11528: This tests if a corrupt function exists inside of Hive
    which does not derive from UDF. The way we do this here is to create a valid
    function in Hive which does derive from UDF, but switch the underlying jar to
    one that does not derive from the UDF class. """

    CORRUPT_JAR = "test-warehouse/test_corrupt.jar"
    self.filesystem_client.delete_file_dir(CORRUPT_JAR)
    # impala-hive-udfs.jar contains the class CorruptUdf which derives from UDF
    # which is a valid function.
    self.filesystem_client.copy("/test-warehouse/impala-hive-udfs.jar",
        "/" + CORRUPT_JAR)
    self.run_stmt_in_hive("create function %s.corrupt_bad_function_udf as \
        'org.apache.impala.CorruptUdf' using jar '%s/%s'"
        % (self.JAVA_FN_TEST_DB, os.getenv('DEFAULT_FS'), CORRUPT_JAR))
    # Now copy the CorruptUdf class from the impala-corrupt-hive-udfs.jar file which
    # does not derive from UDF, making it an invalid UDF.
    self.filesystem_client.delete_file_dir(CORRUPT_JAR)
    self.filesystem_client.copy("/test-warehouse/impala-corrupt-hive-udfs.jar",
        "/" + CORRUPT_JAR)
    self.__restart_cluster()
    # Make sure the function count is 0
    self.verify_function_count(
        "SHOW FUNCTIONS in {0}".format(self.JAVA_FN_TEST_DB), 0)

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
     catalogd_args= "--local_library_dir={0}".format(LOCAL_LIBRARY_DIR))
  def test_java_udfs_hive_integration(self):
    ''' This test checks the integration between Hive and Impala on
    CREATE FUNCTION and DROP FUNCTION statements for persistent Java UDFs.
    The main objective of the test is to check the following four cases.
      - Add Java UDFs from Impala and make sure they are visible in Hive
      - Drop Java UDFs from Impala and make sure this reflects in Hive.
      - Add Java UDFs from Hive and make sure they are visitble in Impala
      - Drop Java UDFs from Hive and make sure this reflects in Impala
    '''
    # Add Java UDFs from Impala and check if they are visible in Hive.
    # Hive has bug that doesn't display the permanent function in show functions
    # statement. So this test relies on describe function statement which prints
    # a message if the function is not present.
    udfs_to_test = list(self.SAMPLE_JAVA_UDFS)
    if int(os.environ['IMPALA_HIVE_MAJOR_VERSION']) == 2:
      udfs_to_test += self.SAMPLE_JAVA_UDFS_HIVE2_ONLY
    for (fn, fn_symbol) in udfs_to_test:
      self.client.execute(self.DROP_JAVA_UDF_TEMPLATE.format(
          db=self.HIVE_IMPALA_INTEGRATION_DB, function=fn))
      self.client.execute(self.CREATE_JAVA_UDF_TEMPLATE.format(
          db=self.HIVE_IMPALA_INTEGRATION_DB, function=fn,
          location=self.HIVE_UDF_JAR, symbol=fn_symbol))
      hive_stdout = self.__describe_udf_in_hive(fn)
      assert "does not exist" not in hive_stdout
      self.__verify_udf_in_hive(fn)
      # Drop the function from Impala and check if it reflects in Hive.
      self.client.execute(self.DROP_JAVA_UDF_TEMPLATE.format(
          db=self.HIVE_IMPALA_INTEGRATION_DB, function=fn))
      hive_stdout = self.__describe_udf_in_hive(fn)
      assert "does not exist" in hive_stdout

    # Create the same set of functions from Hive and make sure they are visible
    # in Impala. There are two ways to make functions visible in Impala: invalidate
    # metadata and refresh functions <db>.
    REFRESH_COMMANDS = ["INVALIDATE METADATA",
        "REFRESH FUNCTIONS {0}".format(self.HIVE_IMPALA_INTEGRATION_DB)]
    for refresh_command in REFRESH_COMMANDS:
      for (fn, fn_symbol) in udfs_to_test:
        self.run_stmt_in_hive(self.CREATE_HIVE_UDF_TEMPLATE.format(
            db=self.HIVE_IMPALA_INTEGRATION_DB, function=fn,
            location=self.HIVE_UDF_JAR, symbol=fn_symbol))
      self.client.execute(refresh_command)
      for (fn, fn_symbol) in udfs_to_test:
        result = self.client.execute("SHOW FUNCTIONS IN {0}".format(
            self.HIVE_IMPALA_INTEGRATION_DB))
        assert result is not None and len(result.data) > 0 and\
            fn in str(result.data)
        self.__verify_udf_in_impala(fn)
        # Drop the function in Hive and make sure it reflects in Impala.
        self.run_stmt_in_hive(self.DROP_JAVA_UDF_TEMPLATE.format(
            db=self.HIVE_IMPALA_INTEGRATION_DB, function=fn))
      self.client.execute(refresh_command)
      self.verify_function_count(
          "SHOW FUNCTIONS in {0}".format(self.HIVE_IMPALA_INTEGRATION_DB), 0)
      # Make sure we deleted all the temporary jars we copied to the local fs
      assert len(glob.glob(self.LOCAL_LIBRARY_DIR + "/*.jar")) == 0

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
     catalogd_args= "--local_library_dir={0}".format(LOCAL_LIBRARY_DIR))
  def test_refresh_native(self):
    ''' This test checks that a native function is visible in Impala after a
    REFRESH FUNCTIONS command. We will add the native function through Hive
    by setting DBPROPERTIES of a database.'''
    # First we create the function in Impala.
    create_func_impala = ("create function {database}.identity_tmp(bigint) "
        "returns bigint location '{location}' symbol='Identity'")
    self.client.execute(create_func_impala.format(
        database=self.HIVE_IMPALA_INTEGRATION_DB,
        location=get_fs_path('/test-warehouse/libTestUdfs.so')))

    # Impala puts the native function into a database property table. We extract the key
    # value pair that represents the function from the table.
    describe_db_hive = "DESCRIBE DATABASE EXTENDED {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB)
    result = self.run_stmt_in_hive(describe_db_hive)
    regex = r"{.*(impala_registered_function.*?)=(.*?)[,}]"
    match = re.search(regex, result)
    func_name = match.group(1)
    func_contents = match.group(2)

    # Recreate the database, this deletes the function.
    self.client.execute("DROP DATABASE {database} CASCADE".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    self.client.execute("CREATE DATABASE {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    result = self.client.execute("SHOW FUNCTIONS IN {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result is not None and len(result.data) == 0

    # Place the function into the recreated database by modifying it's properties.
    alter_db_hive = "ALTER DATABASE {database} SET DBPROPERTIES ('{fn_name}'='{fn_val}')"
    self.run_stmt_in_hive(alter_db_hive.format(
        database=self.HIVE_IMPALA_INTEGRATION_DB,
        fn_name=func_name,
        fn_val=func_contents))
    result = self.client.execute("SHOW FUNCTIONS IN {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result is not None and len(result.data) == 0

    # The function should be visible in Impala after a REFRESH FUNCTIONS.
    self.client.execute("REFRESH FUNCTIONS {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    result = self.client.execute("SHOW FUNCTIONS IN {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result is not None and len(result.data) > 0 and\
        "identity_tmp" in str(result.data)

    # Verify that the function returns a correct result.
    result = self.client.execute("SELECT {database}.identity_tmp(10)".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result.data[0] == "10"
    # Make sure we deleted all the temporary jars we copied to the local fs
    assert len(glob.glob(self.LOCAL_LIBRARY_DIR + "/*.jar")) == 0

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
     catalogd_args= "--local_library_dir={0}".format(LOCAL_LIBRARY_DIR))
  def test_refresh_replace(self):
    ''' This test checks that if we drop a function and then create a
    different function with the same name in Hive, the new function will
    be visible in Impala after REFRESH FUNCTIONS.'''
    # Create an original function.
    create_orig_func_hive = ("create function {database}.test_func as "
        "'org.apache.hadoop.hive.ql.udf.UDFHex' using jar '{jar}'")
    self.run_stmt_in_hive(create_orig_func_hive.format(
        database=self.HIVE_IMPALA_INTEGRATION_DB, jar=self.JAVA_UDF_JAR))
    result = self.client.execute("SHOW FUNCTIONS IN {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result is not None and len(result.data) == 0
    # Verify the function becomes visible in Impala after REFRESH FUNCTIONS.
    self.client.execute("REFRESH FUNCTIONS {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    result = self.client.execute("SHOW FUNCTIONS IN {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert (result is not None and len(result.data) == 3 and
        "test_func" in str(result.data))
    result = self.client.execute("SELECT {database}.test_func(123)".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result.data[0] == "7B"

    # Drop the original function and create a different function with the same name as
    # the original, but a different JAR.
    drop_orig_func_hive = "DROP FUNCTION {database}.test_func"
    self.run_stmt_in_hive(drop_orig_func_hive.format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    create_replacement_func_hive = ("create function {database}.test_func as "
        "'org.apache.hadoop.hive.ql.udf.UDFBin' using jar '{jar}'")
    self.run_stmt_in_hive(create_replacement_func_hive.format(
        database=self.HIVE_IMPALA_INTEGRATION_DB, jar=self.JAVA_UDF_JAR))
    self.client.execute("REFRESH FUNCTIONS {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    result = self.client.execute("SHOW FUNCTIONS IN {database}".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert (result is not None and len(result.data) == 1 and
        "test_func" in str(result.data))
    # Verify that the function has actually been updated.
    result = self.client.execute("SELECT {database}.test_func(123)".format(
        database=self.HIVE_IMPALA_INTEGRATION_DB))
    assert result.data[0] == "1111011"
    # Make sure we deleted all the temporary jars we copied to the local fs
    assert len(glob.glob(self.LOCAL_LIBRARY_DIR + "/*.jar")) == 0

  @pytest.mark.execute_serially
  def test_java_udfs_from_impala(self):
    """ This tests checks the behavior of permanent Java UDFs in Impala."""
    self.verify_function_count(
            "SHOW FUNCTIONS in {0}".format(self.JAVA_FN_TEST_DB), 0);
    # Create a non persistent Java UDF and make sure we can't create a
    # persistent Java UDF with same name
    self.client.execute("create function %s.%s(boolean) returns boolean "\
        "location '%s' symbol='%s'" % (self.JAVA_FN_TEST_DB, "identity",
        self.JAVA_UDF_JAR, "org.apache.impala.TestUdf"))
    result = self.execute_query_expect_failure(self.client,
        self.CREATE_JAVA_UDF_TEMPLATE.format(db=self.JAVA_FN_TEST_DB,
        function="identity", location=self.JAVA_UDF_JAR,
        symbol="org.apache.impala.TestUdf"))
    assert "Function already exists" in str(result)
    # Test the same with a NATIVE function
    self.client.execute("create function {database}.identity(int) "\
        "returns int location '{location}' symbol='Identity'".format(
        database=self.JAVA_FN_TEST_DB,
        location="/test-warehouse/libTestUdfs.so"))
    result = self.execute_query_expect_failure(self.client,
        self.CREATE_JAVA_UDF_TEMPLATE.format(db=self.JAVA_FN_TEST_DB,
        function="identity", location=self.JAVA_UDF_JAR,
        symbol="org.apache.impala.TestUdf"))
    assert "Function already exists" in str(result)

    # Test the reverse. Add a persistent Java UDF and ensure we cannot
    # add non persistent Java UDFs or NATIVE functions with the same name.
    self.client.execute(self.CREATE_JAVA_UDF_TEMPLATE.format(
        db=self.JAVA_FN_TEST_DB, function="identity_java",
        location=self.JAVA_UDF_JAR, symbol="org.apache.impala.TestUdf"))
    result = self.execute_query_expect_failure(self.client, "create function "\
        "%s.%s(boolean) returns boolean location '%s' symbol='%s'" % (
        self.JAVA_FN_TEST_DB, "identity_java", self.JAVA_UDF_JAR,
        "org.apache.impala.TestUdf"))
    assert "Function already exists" in str(result)
    result = self.execute_query_expect_failure(self.client, "create function "\
        "{database}.identity_java(int) returns int location '{location}' "\
        "symbol='Identity'".format(database=self.JAVA_FN_TEST_DB,
        location="/test-warehouse/libTestUdfs.so"))
    assert "Function already exists" in str(result)
    # With IF NOT EXISTS, the query shouldn't fail.
    result = self.execute_query_expect_success(self.client, "create function "\
        " if not exists {database}.identity_java(int) returns int location "\
        "'{location}' symbol='Identity'".format(database=self.JAVA_FN_TEST_DB,
        location="/test-warehouse/libTestUdfs.so"))
    result = self.client.execute("SHOW FUNCTIONS in %s" % self.JAVA_FN_TEST_DB)
    self.execute_query_expect_success(self.client,
        "DROP FUNCTION IF EXISTS {db}.impala_java".format(db=self.JAVA_FN_TEST_DB))

    # Drop the persistent Java function.
    # Test the same create with IF NOT EXISTS. No exception should be thrown.
    # Add a Java udf which has a few incompatible 'evaluate' functions in the
    # symbol class. Catalog should load only the compatible ones. JavaUdfTest
    # has 8 evaluate signatures out of which only 3 are valid.
    compatibility_fn_count = 3
    self.client.execute(self.CREATE_JAVA_UDF_TEMPLATE.format(
        db=self.JAVA_FN_TEST_DB, function="compatibility",
        location=self.JAVA_UDF_JAR, symbol="org.apache.impala.JavaUdfTest"))
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s like 'compatibility*'" % self.JAVA_FN_TEST_DB,
        compatibility_fn_count)
    result = self.client.execute("SHOW FUNCTIONS in %s" % self.JAVA_FN_TEST_DB)
    function_count = len(result.data)
    # Invalidating metadata should preserve all the functions
    self.client.execute("INVALIDATE METADATA")
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s" % self.JAVA_FN_TEST_DB, function_count)
    # Restarting the cluster should preserve only the persisted functions. In
    # this case, identity(boolean) should be wiped out.
    self.__restart_cluster()
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s" % self.JAVA_FN_TEST_DB, function_count-1)
    # Dropping persisted Java UDFs with old syntax should raise an exception
    self.execute_query_expect_failure(self.client,
        "DROP FUNCTION compatibility(smallint)")
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s like 'compatibility*'" % self.JAVA_FN_TEST_DB, 3)
    # Drop the functions and make sure they don't appear post restart.
    self.client.execute("DROP FUNCTION %s.compatibility" % self.JAVA_FN_TEST_DB)
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s like 'compatibility*'" % self.JAVA_FN_TEST_DB, 0)
    self.__restart_cluster()
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s like 'compatibility*'" % self.JAVA_FN_TEST_DB, 0)

    # Try to load a UDF that has no compatible signatures. Make sure it is not added
    # to Hive and Impala.
    result = self.execute_query_expect_failure(self.client,
        self.CREATE_JAVA_UDF_TEMPLATE.format(db=self.JAVA_FN_TEST_DB, function="badudf",
        location=self.JAVA_UDF_JAR, symbol="org.apache.impala.IncompatibleUdfTest"))
    assert "No compatible signatures" in str(result)
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s like 'badudf*'" % self.JAVA_FN_TEST_DB, 0)
    result = self.__describe_udf_in_hive('badudf', db=self.JAVA_FN_TEST_DB)
    assert "does not exist" in str(result)
    # Create the same function from hive and make sure Impala doesn't load any signatures.
    self.run_stmt_in_hive(self.CREATE_HIVE_UDF_TEMPLATE.format(
        db=self.JAVA_FN_TEST_DB, function="badudf",
        location=self.JAVA_UDF_JAR, symbol="org.apache.impala.IncompatibleUdfTest"))
    result = self.__describe_udf_in_hive('badudf', db=self.JAVA_FN_TEST_DB)
    assert "does not exist" not in str(result)
    self.client.execute("INVALIDATE METADATA")
    self.verify_function_count(
        "SHOW FUNCTIONS IN %s like 'badudf*'" % self.JAVA_FN_TEST_DB, 0)
    # Add a function with the same name from Impala. It should fail.
    result = self.execute_query_expect_failure(self.client,
        self.CREATE_JAVA_UDF_TEMPLATE.format(db=self.JAVA_FN_TEST_DB, function="badudf",
        location=self.JAVA_UDF_JAR, symbol="org.apache.impala.TestUdf"))
    assert "Function badudf already exists" in str(result)
    # Drop the function and make sure the function if dropped from hive
    self.client.execute(self.DROP_JAVA_UDF_TEMPLATE.format(
        db=self.JAVA_FN_TEST_DB, function="badudf"))
    result = self.__describe_udf_in_hive('badudf', db=self.JAVA_FN_TEST_DB)
    assert "does not exist" in str(result)

  # Create sample UDA functions in {database} from library {location}

  DROP_SAMPLE_UDAS_TEMPLATE = """
    drop function if exists {database}.test_count(int);
    drop function if exists {database}.hll(int);
    drop function if exists {database}.sum_small_decimal(decimal(9,2));
  """

  CREATE_JAVA_UDF_TEMPLATE = """
    CREATE FUNCTION {db}.{function} LOCATION '{location}' symbol='{symbol}'
  """

  CREATE_HIVE_UDF_TEMPLATE = """
    CREATE FUNCTION {db}.{function} as '{symbol}' USING JAR '{location}'
  """

  DROP_JAVA_UDF_TEMPLATE = "DROP FUNCTION IF EXISTS {db}.{function}"

  # Sample java udfs from hive-exec.jar. Function name to symbol class mapping
  SAMPLE_JAVA_UDFS = [
      ('udfpi', 'org.apache.hadoop.hive.ql.udf.UDFPI'),
      ('udfbin', 'org.apache.hadoop.hive.ql.udf.UDFBin'),
      ('udfhex', 'org.apache.hadoop.hive.ql.udf.UDFHex'),
      ('udfconv', 'org.apache.hadoop.hive.ql.udf.UDFConv'),
      ('udflike', 'org.apache.hadoop.hive.ql.udf.UDFLike'),
      ('udfsign', 'org.apache.hadoop.hive.ql.udf.UDFSign'),
      ('udfascii','org.apache.hadoop.hive.ql.udf.UDFAscii')
  ]

  # These UDFs are available in Hive 2 but in Hive 3 are now implemented
  # using a new GenericUDF interface that we don't support.
  SAMPLE_JAVA_UDFS_HIVE2_ONLY = [
      ('udfhour', 'org.apache.hadoop.hive.ql.udf.UDFHour'),
      ('udfyear', 'org.apache.hadoop.hive.ql.udf.UDFYear'),
  ]

  # Simple tests to verify java udfs in SAMPLE_JAVA_UDFS
  SAMPLE_JAVA_UDFS_TEST = {
    'udfpi' : ('{db}.udfpi()', '3.141592653589793'),
    'udfbin' : ('{db}.udfbin(123)', '1111011'),
    'udfhex' : ('{db}.udfhex(123)', '7B'),
    'udfconv'  : ('{db}.udfconv("100", 2, 10)', '4'),
    'udfhour'  : ('{db}.udfhour("12:55:12")', '12'),
    'udflike'  : ('{db}.udflike("abc", "def")', 'false'),
    'udfsign'  : ('{db}.udfsign(0)', '0'),
    'udfyear' : ('{db}.udfyear("1990-02-06")', '1990'),
    'udfascii' : ('{db}.udfascii("abc")','97')
  }

  CREATE_SAMPLE_UDAS_TEMPLATE = """
    create database if not exists {database};

    create aggregate function {database}.test_count(int) returns bigint
    location '{location}' update_fn='CountUpdate';

    create aggregate function {database}.hll(int) returns string
    location '{location}' update_fn='HllUpdate';

    create aggregate function {database}.sum_small_decimal(decimal(9,2))
    returns decimal(9,2) location '{location}' update_fn='SumSmallDecimalUpdate';
  """

  # Create test UDA functions in {database} from library {location}
  CREATE_TEST_UDAS_TEMPLATE = """
    drop function if exists {database}.trunc_sum(double);

    create database if not exists {database};

    create aggregate function {database}.trunc_sum(double)
    returns bigint intermediate double location '{location}'
    update_fn='TruncSumUpdate' merge_fn='TruncSumMerge'
    serialize_fn='TruncSumSerialize' finalize_fn='TruncSumFinalize';
  """

  # Create test UDF functions in {database} from library {location}
  CREATE_UDFS_TEMPLATE = """
    drop function if exists {database}.identity(boolean);
    drop function if exists {database}.identity(tinyint);
    drop function if exists {database}.identity(smallint);
    drop function if exists {database}.identity(int);
    drop function if exists {database}.identity(bigint);
    drop function if exists {database}.identity(float);
    drop function if exists {database}.identity(double);
    drop function if exists {database}.identity(string);
    drop function if exists {database}.identity(timestamp);
    drop function if exists {database}.identity(date);
    drop function if exists {database}.identity(decimal(9,0));
    drop function if exists {database}.identity(decimal(18,1));
    drop function if exists {database}.identity(decimal(38,10));
    drop function if exists {database}.all_types_fn(
        string, boolean, tinyint, smallint, int, bigint, float, double, decimal(2,0),
        date, binary);
    drop function if exists {database}.no_args();
    drop function if exists {database}.var_and(boolean...);
    drop function if exists {database}.var_sum(int...);
    drop function if exists {database}.var_sum(double...);
    drop function if exists {database}.var_sum(string...);
    drop function if exists {database}.var_sum(decimal(4,2)...);
    drop function if exists {database}.var_sum_multiply(double, int...);
    drop function if exists {database}.constant_timestamp();
    drop function if exists {database}.constant_date();
    drop function if exists {database}.validate_arg_type(string);
    drop function if exists {database}.count_rows();
    drop function if exists {database}.constant_arg(int);
    drop function if exists {database}.validate_open(int);
    drop function if exists {database}.mem_test(bigint);
    drop function if exists {database}.mem_test_leaks(bigint);
    drop function if exists {database}.unmangled_symbol();
    drop function if exists {database}.four_args(int, int, int, int);
    drop function if exists {database}.five_args(int, int, int, int, int);
    drop function if exists {database}.six_args(int, int, int, int, int, int);
    drop function if exists {database}.seven_args(int, int, int, int, int, int, int);
    drop function if exists {database}.eight_args(int, int, int, int, int, int, int, int);

    create database if not exists {database};

    create function {database}.identity(boolean) returns boolean
    location '{location}' symbol='Identity';

    create function {database}.identity(tinyint) returns tinyint
    location '{location}' symbol='Identity';

    create function {database}.identity(smallint) returns smallint
    location '{location}' symbol='Identity';

    create function {database}.identity(int) returns int
    location '{location}' symbol='Identity';

    create function {database}.identity(bigint) returns bigint
    location '{location}' symbol='Identity';

    create function {database}.identity(float) returns float
    location '{location}' symbol='Identity';

    create function {database}.identity(double) returns double
    location '{location}' symbol='Identity';

    create function {database}.identity(string) returns string
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE';

    create function {database}.identity(binary) returns binary
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE';

    create function {database}.identity(timestamp) returns timestamp
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_12TimestampValE';

    create function {database}.identity(date) returns date
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_7DateValE';

    create function {database}.identity(decimal(9,0)) returns decimal(9,0)
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10DecimalValE';

    create function {database}.identity(decimal(18,1)) returns decimal(18,1)
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10DecimalValE';

    create function {database}.identity(decimal(38,10)) returns decimal(38,10)
    location '{location}'
    symbol='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10DecimalValE';

    create function {database}.all_types_fn(
        string, boolean, tinyint, smallint, int, bigint, float, double, decimal(2,0),
        date, binary)
    returns int
    location '{location}' symbol='AllTypes';

    create function {database}.no_args() returns string
    location '{location}'
    symbol='_Z6NoArgsPN10impala_udf15FunctionContextE';

    create function {database}.var_and(boolean...) returns boolean
    location '{location}' symbol='VarAnd';

    create function {database}.var_sum(int...) returns int
    location '{location}' symbol='VarSum';

    create function {database}.var_sum(double...) returns double
    location '{location}' symbol='VarSum';

    create function {database}.var_sum(string...) returns int
    location '{location}' symbol='VarSum';

    create function {database}.var_sum(decimal(4,2)...) returns decimal(18,2)
    location '{location}' symbol='VarSum';

    create function {database}.var_sum_multiply(double, int...) returns double
    location '{location}'
    symbol='_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE';

    create function {database}.constant_timestamp() returns timestamp
    location '{location}' symbol='ConstantTimestamp';

    create function {database}.constant_date() returns date
    location '{location}' symbol='ConstantDate';

    create function {database}.validate_arg_type(string) returns boolean
    location '{location}' symbol='ValidateArgType';

    create function {database}.count_rows() returns bigint
    location '{location}' symbol='Count' prepare_fn='CountPrepare' close_fn='CountClose';

    create function {database}.constant_arg(int) returns int
    location '{location}' symbol='ConstantArg' prepare_fn='ConstantArgPrepare' close_fn='ConstantArgClose';

    create function {database}.validate_open(int) returns boolean
    location '{location}' symbol='ValidateOpen'
    prepare_fn='ValidateOpenPrepare' close_fn='ValidateOpenClose';

    create function {database}.mem_test(bigint) returns bigint
    location '{location}' symbol='MemTest'
    prepare_fn='MemTestPrepare' close_fn='MemTestClose';

    create function {database}.mem_test_leaks(bigint) returns bigint
    location '{location}' symbol='MemTest'
    prepare_fn='MemTestPrepare';
  """
