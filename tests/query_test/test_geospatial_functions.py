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

import os
from subprocess import check_call, check_output

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfApacheHive, SkipIfFS
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import get_fs_path
from tests.util.get_parquet_metadata import get_parquet_metadata_from_hdfs_folder
from tests.util.iceberg_util import get_latest_metadata_path


@SkipIfApacheHive.feature_not_supported
class TestGeospatialFuctions(ImpalaTestSuite):
  """Tests the geospatial builtin functions in the default WKB_EXPERIMENTAL mode."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestGeospatialFuctions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Currently only a text table has geospatial data, skip other fileformats.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text')

  def test_esri_geospatial_functions(self, vector):
    # tests generated from
    # https://github.com/Esri/spatial-framework-for-hadoop/tree/master/hive/test
    self.run_test_case('QueryTest/geospatial-esri', vector)

  def test_esri_geospatial_functions_extra(self, vector):
    # manually added tests
    self.run_test_case('QueryTest/geospatial-esri-extra', vector)

  def test_wkb_serialization(self, vector):
    # WKB serialization round-trip and error cases
    self.run_test_case('QueryTest/geospatial-wkb-serialization', vector)

  def test_relations_table(self, vector):
    self.run_test_case('QueryTest/geospatial-relations-table', vector)

  def test_geometry_type(self, vector, unique_database):
    # tests for GEOMETRY type specific behavior
    self.run_test_case('QueryTest/geometry-type', vector, use_db=unique_database)

  def test_iceberg_geometry(self, vector, unique_database):
    # Iceberg table with GEOMETRY column: create, insert, select, alter.
    self.run_test_case('QueryTest/iceberg-geometry', vector, use_db=unique_database)

  def test_iceberg_geometry_parquet_no_stats(self, unique_database, tmpdir):
    # Impala does not write value statistics for a GEOMETRY column: byte-wise WKB min/max
    # are meaningless, and geospatial bounding-box stats are not implemented yet
    # (TODO IMPALA-15163). Reading a single data file's footer, verify on the same file:
    #  1) the GEOMETRY column has no min/max Statistics and no geospatial_statistics
    #     (only null_count is recorded); and
    #  2) no per-page column index is written for the GEOMETRY column, while its offset
    #     index still is, and a non-geometry column has both.
    tbl_name = "geom_pq_stats"
    fq_tbl = "{0}.{1}".format(unique_database, tbl_name)
    self.execute_query(
        "CREATE TABLE {0} (id INT, geom GEOMETRY) STORED AS ICEBERG "
        "TBLPROPERTIES('format-version'='3')".format(fq_tbl))
    self.execute_query(
        "INSERT INTO {0} VALUES "
        "(1, ST_Point(1.0, 2.0)), (2, ST_Point(5.0, 8.0)), "
        "(3, ST_LineString(0.0, 0.0, 10.0, 10.0)), (4, NULL)".format(fq_tbl))

    hdfs_dir = get_fs_path(
        "/test-warehouse/{0}.db/{1}/data".format(unique_database, tbl_name))
    file_metadatas = get_parquet_metadata_from_hdfs_folder(hdfs_dir, str(tmpdir))
    assert len(file_metadatas) == 1
    row_groups = file_metadatas[0].row_groups
    assert len(row_groups) == 1
    columns = {col.meta_data.path_in_schema[-1]: col for col in row_groups[0].columns}
    assert 'geom' in columns and 'id' in columns, list(columns)
    geom_col = columns['geom']
    id_col = columns['id']

    # (1) No value statistics for GEOMETRY: no min/max, no geospatial bbox.
    assert geom_col.meta_data.geospatial_statistics is None
    geom_stats = geom_col.meta_data.statistics
    assert geom_stats is not None
    assert geom_stats.min_value is None and geom_stats.max_value is None
    assert geom_stats.min is None and geom_stats.max is None
    # The null count is still recorded.
    assert geom_stats.null_count == 1

    def has_column_index(col):
      return (col.column_index_offset is not None
              and col.column_index_length is not None)

    def has_offset_index(col):
      return (col.offset_index_offset is not None
              and col.offset_index_length is not None)

    # (2) Page index: the GEOMETRY column keeps its offset index (page locations, always
    # written) but has no column index (per-page min/max, meaningless for WKB); a
    # non-geometry column has both.
    assert not has_column_index(geom_col)
    assert has_offset_index(geom_col)
    assert has_column_index(id_col)
    assert has_offset_index(id_col)

  def _create_geometry_table_with_crs(self, unique_database, tmpdir, tbl, crs_type):
    """Creates an Iceberg table with a GEOMETRY column using Impala, then rewrites the
    geometry type in the table's current metadata.json to 'crs_type' (e.g.
    'geometry(EPSG:3857)'). No engine available here can write such a table (Trino
    cannot), so the CRS is patched into the Iceberg metadata. The file is rewritten in
    place, so 'metadata_location' in HMS keeps pointing to it. Returns the fully
    qualified table name, after INVALIDATE METADATA."""
    fq_tbl = "{0}.{1}".format(unique_database, tbl)
    self.execute_query(
        "CREATE TABLE {0} (id INT, geom GEOMETRY) STORED AS ICEBERG "
        "TBLPROPERTIES('format-version'='3')".format(fq_tbl))
    metadata_path = get_latest_metadata_path(self.client, unique_database, tbl)
    metadata = check_output(['hadoop', 'fs', '-cat', metadata_path]).decode('utf-8')
    patched = metadata.replace('"geometry"', '"{0}"'.format(crs_type))
    assert patched != metadata, metadata
    local_path = os.path.join(str(tmpdir), 'patched.metadata.json')
    with open(local_path, 'w') as f:
      f.write(patched)
    check_call(['hadoop', 'fs', '-put', '-f', local_path, metadata_path])
    self.execute_query("INVALIDATE METADATA {0}".format(fq_tbl))
    return fq_tbl

  def test_iceberg_geometry_non_default_crs(self, unique_database, tmpdir):
    # Impala supports only the default CRS (OGC:CRS84), so loading an Iceberg table whose
    # GEOMETRY column declares a different CRS has to be rejected.
    fq_tbl = self._create_geometry_table_with_crs(unique_database, tmpdir,
        "geom_non_default_crs", "geometry(EPSG:3857)")
    # The table can no longer be loaded, so even DESCRIBE fails.
    err = self.execute_query_expect_failure(self.client, "DESCRIBE {0}".format(fq_tbl))
    expected = "Unsupported geometry CRS: EPSG:3857. Only the default CRS"
    assert expected in str(err), str(err)

  def test_iceberg_geometry_default_crs_case_insensitive(self, unique_database, tmpdir):
    # The default CRS is accepted regardless of the case it is spelled in.
    for i, crs_type in enumerate(["geometry(OGC:CRS84)", "geometry(ogc:crs84)"]):
      fq_tbl = self._create_geometry_table_with_crs(unique_database, tmpdir,
          "geom_default_crs_{0}".format(i), crs_type)
      result = self.execute_query("DESCRIBE {0}".format(fq_tbl))
      assert "geom\tgeometry" in "\n".join(result.data), result.data
      self.execute_query("SELECT id, ST_AsText(geom) FROM {0}".format(fq_tbl))

  @SkipIfFS.hive
  def test_iceberg_geometry_hive_interop(self, unique_database):
    # Hive rejects reading Iceberg tables with GEOMETRY columns.
    self.execute_query(
        "CREATE TABLE {0}.ice_geom_hive_test (id INT, geom GEOMETRY) "
        "STORED AS ICEBERG TBLPROPERTIES('format-version'='3')".format(unique_database))
    self.execute_query(
        "INSERT INTO {0}.ice_geom_hive_test VALUES "
        "(1, ST_Point(1.0, 2.0)), (2, ST_Point(3.0, 4.0))".format(unique_database))
    try:
      self.run_stmt_in_hive(
          "SELECT * FROM {0}.ice_geom_hive_test".format(unique_database))
    except Exception as e:
      # Hive may reject due to unsupported format version 3 or unsupported geometry type.
      assert "GEOMETRY type is not supported)" in str(e), \
          "Unexpected error from Hive: " + str(e)
    else:
      assert False, "Expected Hive query to fail on geometry/v3 table"
