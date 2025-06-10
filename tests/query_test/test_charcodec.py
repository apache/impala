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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension
from tests.common.skip import SkipIfFS
import codecs
import os
import pytest
import random
import tempfile
import shutil
import sys

if sys.version_info[0] >= 3:
    unichr = chr  # Python 3

_hiragana_range = [codepoint for codepoint in range(0x3040, 0x309F) if codepoint not in
    # problematic symbols: unassigned, deprecated, etc:
    set([0x3040, 0x3094, 0x3095, 0x3096, 0x3097, 0x3098, 0x3099, 0x309A, 0x309B, 0x309C])]

_cyrillic_range = [codepoint for codepoint in range(0x0410, 0x045F) if codepoint not in
    # problematic symbols: unassigned, deprecated, etc:
    set([0x0450, 0x0452, 0x0453, 0x0454, 0x0455, 0x0456, 0x0457, 0x0458,
         0x0459, 0x045A, 0x045B, 0x045C, 0x045D, 0x045E])]

_charsets = {
  'gbk': u''.join(unichr(i) for i in range(0x4E00, 0x9FA6)),
  'latin1': u''.join(unichr(i) for i in range(0x20, 0x7F)),
  'shift_jis': u''.join(unichr(i) for i in _hiragana_range),
  'cp1251': u''.join(unichr(i) for i in range(0x0410, 0x044F)),
  'koi8-r': u''.join(unichr(i) for i in _cyrillic_range)
}


def _generate_random_word(charset, min_length=1, max_length=20):
  length = random.randint(min_length, max_length)
  return u''.join(random.choice(charset) for _ in range(length))


def _compare_tables(selfobj, db, utf8_table, encoded_table, row_count):
    # Compare count(*) of the encoded table with the utf8 table
    count_utf8 = selfobj.client.execute("""select count(*) from {}.{}"""
        .format(db, utf8_table))
    count_encoded = selfobj.client.execute("""select count(*) from {}.{}"""
        .format(db, encoded_table))
    assert int(count_utf8.get_data()) == int(count_encoded.get_data()) == row_count

    # Compare * of the encoded table with the utf8 table
    result = selfobj.client.execute("""select * from {}.{} except select * from {}.{}
        union all select * from {}.{} except select * from {}.{}"""
        .format(db, utf8_table, db, encoded_table, db, encoded_table, db, utf8_table))
    assert result.data == []


# Tests with auto-generated data
class TestCharCodecGen(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCharCodecGen, cls).add_test_dimensions()
    encodings = list(_charsets.keys())
    # Only run the tests for single 'gbk' encoding in non-exhaustive mode.
    if cls.exploration_strategy() != 'exhaustive':
      encodings = [enc for enc in encodings if enc == 'gbk']
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension(
        'charset', *encodings))
    # There is no reason to run these tests using all dimensions.
    # See IMPALA-14063 for Sequence file format support.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('exec_option')['disable_codegen'] is False)

  # Basic Tests
  ####################################################################
  def generate_text_files(self, encoding_name, charset, test_name,
                          num_lines=10000, words_per_line=5, num_files=1,
                          min_word_length=1, max_word_length=20):
    lines_per_file = num_lines // num_files
    file_paths = []
    tmp_dir = tempfile.mkdtemp(dir=os.path.join(os.environ['IMPALA_HOME'], "testdata"))
    for file_index in range(num_files):
      data_file_path = os.path.join(tmp_dir, "charcodec_{}_{}_utf8_{}.txt"
                                    .format(encoding_name, test_name, file_index))
      file_paths.append(data_file_path)
      with codecs.open(data_file_path, 'w', encoding='utf-8') as file:
        for _ in range(lines_per_file):
          words = [_generate_random_word(charset, min_word_length, max_word_length)
                   for _ in range(words_per_line)]
          line = u','.join(words)
          file.write(line + u'\n')
    return tmp_dir, file_paths, num_lines

  def prepare_utf8_test_table(self, db, file_paths, encoding_name, vector):
    encoding_name_tbl = encoding_name.replace('-', '')
    tbl_name = "{}_gen_utf8".format(encoding_name_tbl)
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"""
        .format(db, tbl_name))
    for file_path in file_paths:
      self.filesystem_client.copy_from_local(file_path,
          self._get_table_location("{0}.{1}".format(db, tbl_name), vector))
    # remove REFRESH when IMPALA-13749 is fixed
    self.execute_query("""REFRESH {}.{}""".format(db, tbl_name))
    return tbl_name

  def prepare_encoded_test_table(self, db, utf8_table, encoding_name):
    encoding_name_tbl = encoding_name.replace('-', '')
    encoded_table = "{}_gen".format(encoding_name_tbl)
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        STORED AS TEXTFILE""".format(db, encoded_table))
    self.execute_query("""ALTER TABLE {}.{}
                       SET SERDEPROPERTIES("serialization.encoding"="{}")"""
        .format(db, encoded_table, encoding_name))
    self.execute_query("""REFRESH {}.{}""".format(db, encoded_table))
    self.execute_query("""INSERT OVERWRITE TABLE {}.{} SELECT * FROM {}.{}"""
        .format(db, encoded_table, db, utf8_table))
    return encoded_table

  def test_enc_dec_gen(self, vector, unique_database):
    """Write encoded table with Impala and read it back."""
    db = unique_database
    encoding_name = vector.get_value('charset')
    charset = _charsets[encoding_name]
    tmp_dir, file_paths, row_count = self.generate_text_files(
        encoding_name, charset, "gen")
    utf8_table = self.prepare_utf8_test_table(db, file_paths, encoding_name, vector)
    shutil.rmtree(tmp_dir)
    encoded_table = self.prepare_encoded_test_table(db, utf8_table, encoding_name)
    _compare_tables(self, db, utf8_table, encoded_table, row_count)

  def test_enc_dec_gen_long_words(self, vector, unique_database):
    db = unique_database
    encoding_name = vector.get_value('charset')
    charset = _charsets[encoding_name]
    tmp_dir, file_paths, row_count = self.generate_text_files(
        encoding_name, charset, "gen", min_word_length=100, max_word_length=1000)
    utf8_table = self.prepare_utf8_test_table(db, file_paths, encoding_name, vector)
    shutil.rmtree(tmp_dir)
    encoded_table = self.prepare_encoded_test_table(db, utf8_table, encoding_name)
    _compare_tables(self, db, utf8_table, encoded_table, row_count)

  # Split-file tests
  ####################################################################
  def test_enc_dec_gen_split(self, vector, unique_database):
    """Test table is split across multiple files."""
    db = unique_database
    encoding_name = vector.get_value('charset')
    charset = _charsets[encoding_name]
    tmp_dir, file_paths, row_count = self.generate_text_files(
        encoding_name, charset, "split", num_lines=10000, words_per_line=5, num_files=5)
    utf8_table = self.prepare_utf8_test_table(db, file_paths, encoding_name, vector)
    shutil.rmtree(tmp_dir)
    encoded_table = self.prepare_encoded_test_table(db, utf8_table, encoding_name)
    _compare_tables(self, db, utf8_table, encoded_table, row_count)

  # Hive + Compression Tests
  ####################################################################
  def prepare_encoded_test_table_compress(self, db, utf8_table, encoding_name, codec):
    encoding_name_tbl = encoding_name.replace('-', '')
    encoded_table = "{}_gen_{}".format(encoding_name_tbl, codec)
    self.run_stmt_in_hive("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        STORED AS TEXTFILE""".format(db, encoded_table))
    self.run_stmt_in_hive("""ALTER TABLE {}.{}
                          SET SERDEPROPERTIES("serialization.encoding"="{}")"""
        .format(db, encoded_table, encoding_name))
    self.run_stmt_in_hive("""set hive.exec.compress.output={};
        set mapreduce.output.fileoutputformat.compress.codec=
        org.apache.hadoop.io.compress.{}Codec;
        INSERT OVERWRITE TABLE {}.{} SELECT * FROM {}.{}
        """.format("false" if codec == "None" else "true",
                   codec, db, encoded_table, db, utf8_table))
    return encoded_table

  @SkipIfFS.hive
  def test_enc_dec_gen_compress(self, vector, unique_database):
    db = unique_database
    encoding_name = vector.get_value('charset')
    charset = _charsets[encoding_name]

    tmp_dir, file_paths, row_count = self.generate_text_files(
        encoding_name, charset, "compress", num_lines=10000)
    utf8_table = self.prepare_utf8_test_table(db, file_paths, encoding_name, vector)
    shutil.rmtree(tmp_dir)
    # Snappy codec supports streaming, ZStandard does not
    for codec in ["None", "Snappy", "ZStandard"]:
      encoded_table = self.prepare_encoded_test_table_compress(db, utf8_table,
                                                                encoding_name, codec)
      _compare_tables(self, db, utf8_table, encoded_table, row_count)

  # Partitions Tests
  ####################################################################
  def prepare_utf8_test_table_partitions(self, db, file_paths, encoding_name, vector):
    encoding_name_tbl = encoding_name.replace('-', '')
    tbl_name = "{}_gen_utf8".format(encoding_name_tbl)
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        PARTITIONED BY (part STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"""
        .format(db, tbl_name))
    for i in range(len(file_paths)):
      self.execute_query("""ALTER TABLE {}.{} ADD PARTITION (part='{}')"""
          .format(db, tbl_name, i))
      part_url = os.path.join(
          self._get_table_location("{0}.{1}".format(db, tbl_name), vector),
          "part={}".format(i))
      self.filesystem_client.copy_from_local(file_paths[i], part_url)
    self.execute_query("""REFRESH {}.{}""".format(db, tbl_name))
    return tbl_name

  def prepare_encoded_test_table_partitions(self, db, utf8_table, encoding_name,
                                            file_paths):
    encoding_name_tbl = encoding_name.replace('-', '')
    encoded_table = "{}_gen".format(encoding_name_tbl)
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        PARTITIONED BY (part STRING)
        STORED AS TEXTFILE""".format(db, encoded_table))
    for i in range(len(file_paths)):
      self.execute_query("""ALTER TABLE {}.{} ADD PARTITION (part='{}')"""
                         .format(db, encoded_table, i))
      self.execute_query("""ALTER TABLE {}.{} PARTITION (part='{}')
                        SET SERDEPROPERTIES("serialization.encoding"="{}")"""
                        .format(db, encoded_table, i, encoding_name))
      self.execute_query("""REFRESH {}.{}""".format(db, encoded_table))
      self.execute_query("""INSERT OVERWRITE TABLE {}.{} PARTITION (part='{}')
          SELECT name1, name2, name3, name4, name5 FROM {}.{} WHERE part='{}'"""
          .format(db, encoded_table, i, db, utf8_table, i))
    return encoded_table

  def test_enc_dec_gen_partitions(self, vector, unique_database):
    db = unique_database
    encoding_name = vector.get_value('charset')
    charset = _charsets[encoding_name]
    tmp_dir, file_paths, row_count = self.generate_text_files(
        encoding_name, charset, "partitions", num_lines=10000, num_files=5)
    utf8_table = self.prepare_utf8_test_table_partitions(
        db, file_paths, encoding_name, vector)
    shutil.rmtree(tmp_dir)
    encoded_table = self.prepare_encoded_test_table_partitions(db,
        utf8_table, encoding_name, file_paths)
    _compare_tables(self, db, utf8_table, encoded_table, row_count)


class TestCharCodecGenMixed(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCharCodecGenMixed, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('exec_option')['disable_codegen'] is False)

  # Mixed Partitions Tests
  ####################################################################
  def generate_text_files_mixed(self, test_file, num_lines=10000, words_per_line=5,
                                num_files=1):
    lines_per_file = num_lines // num_files
    file_paths = []
    encodings = []
    tmp_dir = tempfile.mkdtemp(dir=os.path.join(os.environ['IMPALA_HOME'], "testdata"))
    for i in range(num_files):
      encoding_name, charset = random.choice(list(_charsets.items()))
      data_file_path = os.path.join(tmp_dir, "charcodec_{}_{}_utf8_{}.txt"
                                    .format(encoding_name, test_file, i))
      encodings.append(encoding_name)
      file_paths.append(data_file_path)
      with codecs.open(data_file_path, 'w', encoding='utf-8') as file:
        for _ in range(lines_per_file):
          words = [_generate_random_word(charset) for _ in range(words_per_line)]
          line = u','.join(words)
          file.write(line + u'\n')
    return tmp_dir, file_paths, encodings, num_lines

  # Partitioned table with different encodings.
  def prepare_utf8_test_table_partitions_mixed(self, db, file_paths, vector):
    tbl_name = "mixed_gen_utf8"
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        PARTITIONED BY (part STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"""
        .format(db, tbl_name))
    for i in range(len(file_paths)):
      self.execute_query("""ALTER TABLE {}.{} ADD PARTITION (part='{}')"""
        .format(db, tbl_name, i))
      part_url = os.path.join(
          self._get_table_location("{0}.{1}".format(db, tbl_name), vector),
          "part={}".format(i))
      self.filesystem_client.copy_from_local(file_paths[i], part_url)
    self.execute_query("""REFRESH {}.{}""".format(db, tbl_name))
    return tbl_name

  def prepare_encoded_test_table_partitions_mixed(self, db, utf8_table, encodings):
    encoded_table = "mixed_gen"
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        PARTITIONED BY (part STRING)
        STORED AS TEXTFILE""".format(db, encoded_table))
    for i in range(len(encodings)):
      self.execute_query("""ALTER TABLE {}.{} ADD PARTITION (part='{}')"""
                         .format(db, encoded_table, i))
      self.execute_query("""ALTER TABLE {}.{} PARTITION (part='{}')
                        SET SERDEPROPERTIES("serialization.encoding"="{}")"""
                         .format(db, encoded_table, i, encodings[i]))
      self.execute_query("""REFRESH {}.{}""".format(db, encoded_table))
      self.execute_query("""INSERT OVERWRITE TABLE {}.{} PARTITION (part='{}')
          SELECT name1, name2, name3, name4, name5 FROM {}.{} WHERE part='{}'"""
          .format(db, encoded_table, i, db, utf8_table, i))
    return encoded_table

  def test_enc_dec_gen_partitions_mixed(self, unique_database, vector):
    db = unique_database
    tmp_dir, file_paths, encodings, row_count = self.generate_text_files_mixed(
                                                    "mixed", num_lines=10000, num_files=5)
    utf8_table = self.prepare_utf8_test_table_partitions_mixed(db, file_paths, vector)
    shutil.rmtree(tmp_dir)
    encoded_table = self.prepare_encoded_test_table_partitions_mixed(db,
        utf8_table, encodings)
    _compare_tables(self, db, utf8_table, encoded_table, row_count)


class TestCharCodecPreCreated(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCharCodecPreCreated, cls).add_test_dimensions()
    encodings = list(_charsets.keys())
    # Only run the tests for single 'gbk' encoding in non-exhaustive mode.
    if cls.exploration_strategy() != 'exhaustive':
      encodings = [enc for enc in encodings if enc == 'gbk']
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension(
        'charset', *encodings))
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('exec_option')['disable_codegen'] is False)

  def prepare_test_table(self, vector, db, tbl_name, datafile, encoding=None):
    tbl_name = tbl_name.replace('-', '')
    datafile = datafile.replace('-', '')
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (name STRING)
        STORED AS TEXTFILE""".format(db, tbl_name))
    if encoding:
      self.execute_query("""ALTER TABLE {}.{} SET
          SERDEPROPERTIES("serialization.encoding"="{}")"""
          .format(db, tbl_name, encoding))
    data_file_path = os.path.join(os.environ['IMPALA_HOME'], "testdata",
        "charcodec", datafile)
    self.filesystem_client.copy_from_local(data_file_path,
        self._get_table_location("{0}.{1}".format(db, tbl_name), vector))
    self.execute_query("""REFRESH {}.{}""".format(db, tbl_name))
    return tbl_name

  def test_precreated_files(self, vector, unique_database):
    """Read encoded precreated files."""
    db = unique_database
    enc = vector.get_value('charset')

    # Without SERDEPROPERTIES("serialization.encoding") data is read incorrectly
    utf8_table = self.prepare_test_table(
        vector, db, enc + '_names_utf8', enc + '_names_utf8.txt', None)
    encoded_table = self.prepare_test_table(
        vector, db, enc + '_names_none', enc + '_names.txt', None)
    with pytest.raises(AssertionError) as exc_info:
        _compare_tables(self, db, utf8_table, encoded_table, 3)
    assert " == []" in str(exc_info.value)

    # With SERDEPROPERTIES("serialization.encoding") data is read correctly
    encoded_table = self.prepare_test_table(
        vector, db, enc + '_names', enc + '_names.txt', enc)
    _compare_tables(self, db, utf8_table, encoded_table, 3)

  def test_precreated_decoding_with_errors(self, vector, unique_database):
    db = unique_database
    enc = vector.get_value('charset')
    # Skip for promiscious encodings
    if enc not in ['gbk', 'shift_jis']: pytest.skip()
    encoded_table = self.prepare_test_table(
        vector, db, enc + '_names_error', enc + '_names_error.txt', enc)
    err = self.execute_query_expect_failure(
        self.client, """select * from {}.{}""".format(db, encoded_table))
    assert "Error during buffer conversion: Conversion failed" in str(err)

  def test_precreated_encoding_with_errors(self, vector, unique_database):
    db = unique_database
    enc = vector.get_value('charset')
    # Skip for promiscious encodings
    if enc not in ['gbk', 'shift_jis']: pytest.skip()
    encoded_table = self.prepare_test_table(
        vector, db, enc + '_names_error', enc + '_names_error.txt', enc)
    err = self.execute_query_expect_failure(self.client, """insert overwrite {}.{}
        select cast(binary_col as string) from functional.binary_tbl"""
        .format(db, encoded_table))
    assert "Error during buffer conversion: Conversion failed" in str(err)

  @SkipIfFS.hive
  def test_read_from_hive(self, unique_database, vector):
    """Write table with Impala and read it back with Hive."""
    db = unique_database
    enc = vector.get_value('charset')

    utf8_table = self.prepare_test_table(
        vector, db, enc + '_names_utf8', enc + '_names_utf8.txt', None)
    encoded_table = self.prepare_test_table(
        vector, db, enc + '_names', enc + '_names.txt', enc)
    self.execute_query(
        """insert overwrite {}.{} select * from {}.{}"""
        .format(db, encoded_table, db, utf8_table))

    result_hive = self.run_stmt_in_hive(
        """select name from {}.{}""".format(db, encoded_table))
    result_impala = self.client.execute(
        """select name from {}.{}""".format(db, utf8_table))
    result_hive_list = result_hive.strip().split('\n')[1:]
    assert result_hive_list == result_impala.data
