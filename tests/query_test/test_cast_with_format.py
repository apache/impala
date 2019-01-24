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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_beeswax_hs2_dimension


class TestCastWithFormat(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return "functional-query"

  # Run the basic tests once for Beeaswax and once for HS2. The underlying functionality
  # is independent of the file format so make sense to pick one format for testing.
  @classmethod
  def add_test_dimensions(cls):
    super(TestCastWithFormat, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

    cls.ImpalaTestMatrix.add_dimension(create_beeswax_hs2_dimension())

  def test_basic_inputs_from_table(self, vector):
    self.run_test_case('QueryTest/cast_format_from_table', vector)

  def test_basic_inputs_without_row(self, vector):
    # Cast without format clause to cover the default format
    result = self.client.execute("select cast('2017-05-01 01:23:45.678912345' as "
        "timestamp)")
    assert result.data == ["2017-05-01 01:23:45.678912345"]

    # Basic input to cover a datetime with timezone scenario
    result = self.client.execute("select cast('2017-05-03 08:59:01.123456789PM 01:30'"
        "as timestamp FORMAT 'YYYY-MM-DD HH12:MI:SS.FF9PM TZH:TZM')")
    assert result.data == ["2017-05-03 20:59:01.123456789"]

    # Input that contains shuffled date without time
    result = self.client.execute("select cast('12-2010-05' as timestamp format "
        "'DD-YYYY-MM')")
    assert result.data == ["2010-05-12 00:00:00"]

    # Shuffle the input timestamp and the format clause
    result = self.client.execute("select cast('59 04-30-2017-05 01PM 01:08.123456789'"
        "as timestamp FORMAT 'MI DD-TZM-YYYY-MM TZHPM SS:HH12.FF9')")
    assert result.data == ["2017-05-04 20:59:01.123456789"]

    # Input and format without separators
    # Note, 12:01 HH12 AM is 00:01 with the internal 0-23 representation.
    result = self.client.execute("select cast('20170501120159123456789AM-0130' as "
        "timestamp FORMAT 'YYYYDDMMHH12MISSFFAMTZHTZM')")
    assert result.data == ["2017-01-05 00:01:59.123456789"]

    # Shuffled input without separators
    result = self.client.execute("select cast('59043020170501PM0108123456789'"
        "as timestamp FORMAT 'MIDDTZMYYYYMMTZHPMSSHH12FF9')")
    assert result.data == ["2017-05-04 20:59:01.123456789"]

    # Separator section lengths differ between input and format
    result = self.client.execute("select cast('--2017----05-01-' as "
        "timestamp FORMAT '-YYYY--MM---DD---')")
    assert result.data == ["2017-05-01 00:00:00"]

    # If the input string has unprocessed tokens
    result = self.client.execute("select cast('2017-05-01 12:30' as "
        "timestamp FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]
    result = self.client.execute("select cast('2017-05-01-12:30' as "
        "timestamp FORMAT 'YYYY-MM-DD-')")
    assert result.data == ["NULL"]

    # If the format string has unprocessed tokens
    result = self.client.execute("select cast('2017-05-01' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI')")
    assert result.data == ["NULL"]
    result = self.client.execute("select cast('2017-05-01-' as "
        "timestamp FORMAT 'YYYY-MM-DD-HH12')")
    assert result.data == ["NULL"]

    # Timestamp to string types formatting
    result = self.client.execute(
        "select cast(cast('2012-11-04 13:02:59.123456' as timestamp) "
        "as string format 'DD-MM-YYYY MI:HH12:SS A.M. FF9 DDD SSSSS HH12 HH24')")
    assert result.data == ["04-11-2012 02:01:59 P.M. 123456000 309 46979 01 13"]

    result = self.client.execute(
        "select cast(cast('2012-11-04 13:02:59.123456' as timestamp) "
        "as varchar format 'DD-MM-YYYY MI:HH12:SS A.M. FF9 DDD SSSSS HH12 HH24')")
    assert result.data == ["04-11-2012 02:01:59 P.M. 123456000 309 46979 01 13"]

    result = self.client.execute(
        "select cast(cast('2012-11-04 13:02:59.123456' as timestamp) "
        "as char(50) format 'DD-MM-YYYY MI:HH12:SS A.M. FF9 DDD SSSSS HH12 HH24')")
    assert result.data == ["04-11-2012 02:01:59 P.M. 123456000 309 46979 01 13"]

    # Cast NULL string to timestamp
    result = self.client.execute("select cast(cast(NULL as string) as timestamp "
        "FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]

    # Cast NULL timestamp to string
    result = self.client.execute("select cast(cast(NULL as timestamp) as string "
        "FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]

  def test_iso8601_format(self):
    # Basic string to timestamp scenario
    result = self.client.execute("select cast('2018-11-10T15:11:04Z' as "
        "timestamp FORMAT 'YYYY-MM-DDTHH24:MI:SSZ')")
    assert result.data == ["2018-11-10 15:11:04"]

    # ISO8601 format elements are case-insensitive
    result = self.client.execute("select cast('2018-11-09t15:11:04Z' as "
        "timestamp FORMAT 'YYYY-MM-DDTHH24:MI:SSz')")
    assert result.data == ["2018-11-09 15:11:04"]

    result = self.client.execute("select cast('2018-11-08T15:11:04z' as "
        "timestamp FORMAT 'YYYY-MM-DDtHH24:MI:SSZ')")
    assert result.data == ["2018-11-08 15:11:04"]

    # Format path
    result = self.client.execute("select cast(cast('2018-11-10 15:11:04' as "
        "timestamp) as string format 'YYYY-MM-DDTHH24:MI:SSZ')")
    assert result.data == ["2018-11-10T15:11:04Z"]

  def test_lowercase_format_elements(self):
    result = self.client.execute("select cast('2019-11-20 15:59:44.123456789 01:01' as "
        "timestamp format 'yyyy-mm-dd hh24:mi:ss.ff9 tzh-tzm')")
    assert result.data == ["2019-11-20 15:59:44.123456789"]

    result = self.client.execute("select cast('2019-300 15:59:44.123456789 01:01' as "
        "timestamp format 'yyyy-ddd hh24:mi:ss.ff9 tzh-tzm')")
    assert result.data == ["2019-10-27 15:59:44.123456789"]

    result = self.client.execute("select cast('2019-11-21 11:59:44.123456789 p.m. 01:01' "
        "as timestamp format 'yyyy-mm-dd hh12:mi:ss.ff9 am tzh-tzm')")
    assert result.data == ["2019-11-21 23:59:44.123456789"]

    result = self.client.execute("select cast('2019-11-22 10000.123456789 02:02' "
        "as timestamp format 'yyyy-mm-dd sssss ff9 tzh-tzm')")
    assert result.data == ["2019-11-22 02:46:40.123456789"]

  def test_year(self):
    # Test lower boundary of year
    result = self.client.execute("select cast('1399-05-01' as "
        "timestamp FORMAT 'YYYY-MM-DD')")
    assert result.data == ["NULL"]

    # YYYY with less than 4 digits in the input
    query_options = dict({'now_string': '2019-01-01 11:11:11'})

    result = self.execute_query("select cast('095-01-31' as "
        "timestamp FORMAT 'YYYY-MM-DD')", query_options)
    assert result.data == ["2095-01-31 00:00:00"]

    result = self.execute_query("select cast('95-02-28' as "
        "timestamp FORMAT 'YYYY-MM-DD')", query_options)
    assert result.data == ["2095-02-28 00:00:00"]

    result = self.execute_query("select cast('5-03-31' as "
        "timestamp FORMAT 'YYYY-MM-DD')", query_options)
    assert result.data == ["2015-03-31 00:00:00"]

    # YYY with less than 3 digits in the input
    result = self.execute_query("select cast('95-04-30' as "
        "timestamp FORMAT 'YYY-MM-DD')", query_options)
    assert result.data == ["2095-04-30 00:00:00"]

    result = self.execute_query("select cast('5-05-31' as "
        "timestamp FORMAT 'YYY-MM-DD')", query_options)
    assert result.data == ["2015-05-31 00:00:00"]

    # YY with 1 digits in the input
    result = self.execute_query("select cast('5-06-30' as "
        "timestamp FORMAT 'YY-MM-DD')", query_options)
    assert result.data == ["2015-06-30 00:00:00"]

    # YYY, YY, Y tokens without separators
    result = self.execute_query("select cast('0950731' as "
        "timestamp FORMAT 'YYYMMDD')", query_options)
    assert result.data == ["2095-07-31 00:00:00"]

    result = self.execute_query("select cast('950831' as "
        "timestamp FORMAT 'YYMMDD')", query_options)
    assert result.data == ["2095-08-31 00:00:00"]

    result = self.execute_query("select cast('50930' as "
        "timestamp FORMAT 'YMMDD')", query_options)
    assert result.data == ["2015-09-30 00:00:00"]

    # Timestamp to string formatting
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'YYYY')", query_options)
    assert result.data == ["2019"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'YYY')", query_options)
    assert result.data == ["019"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'YY')", query_options)
    assert result.data == ["19"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'Y')", query_options)
    assert result.data == ["9"]

  def test_round_year(self):
    query_options = dict({'now_string': '2019-01-01 11:11:11'})

    # Test lower boundar of round year
    result = self.client.execute("select cast('1399-05-01' as "
        "timestamp FORMAT 'RRRR-MM-DD')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('1400-05-21' as "
        "timestamp FORMAT 'RRRR-MM-DD')")
    assert result.data == ["1400-05-21 00:00:00"]

    # RRRR with 4-digit year falls back to YYYY
    result = self.execute_query("select cast('2017-05-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2017-05-31 00:00:00"]

    # RRRR with 3-digit year fills digits from current year
    result = self.execute_query("select cast('017-01-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2017-01-31 00:00:00"]

    # RRRR wit 1-digit year fills digits from current year
    result = self.execute_query("select cast('0-07-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2010-07-31 00:00:00"]

    # RR with 1-digit year fills digits from current year
    result = self.execute_query("select cast('9-08-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2019-08-31 00:00:00"]

    # Round year when last 2 digits of current year is less than 50
    query_options = dict({'now_string': '2049-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2049-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["1950-03-31 00:00:00"]

    query_options = dict({'now_string': '2000-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2049-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["1950-03-31 00:00:00"]

    # Round year when last 2 digits of current year is greater than 49
    query_options = dict({'now_string': '2050-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2149-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RRRR-MM-DD')", query_options)
    assert result.data == ["2050-03-31 00:00:00"]

    query_options = dict({'now_string': '2099-01-01 11:11:11'})
    result = self.execute_query("select cast('49-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2149-03-31 00:00:00"]

    result = self.execute_query("select cast('50-03-31' as "
        "timestamp FORMAT 'RR-MM-DD')", query_options)
    assert result.data == ["2050-03-31 00:00:00"]

    # In a datetime to sting cast round year act like regular 'YYYY' or 'YY' tokens.
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'RRRR')", query_options)
    assert result.data == ["2019"]

    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'RR')", query_options)
    assert result.data == ["19"]

  def test_day_in_year(self):
    # Test "day in year" token in a non leap year scenario
    result = self.execute_query("select cast('2019 1' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-01-01 00:00:00"]

    result = self.execute_query("select cast('2019 31' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-01-31 00:00:00"]

    result = self.execute_query("select cast('2019 32' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-02-01 00:00:00"]

    result = self.execute_query("select cast('2019 60' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-03-01 00:00:00"]

    result = self.execute_query("select cast('2019 365' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2019-12-31 00:00:00"]

    result = self.execute_query("select cast('2019 366' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["NULL"]

    # Test "day in year" token in a leap year scenario
    result = self.execute_query("select cast('2000 60' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2000-02-29 00:00:00"]

    result = self.execute_query("select cast('2000 61' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2000-03-01 00:00:00"]

    result = self.execute_query("select cast('2000 366' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["2000-12-31 00:00:00"]

    result = self.execute_query("select cast('2000 367' as timestamp FORMAT 'YYYY DDD')")
    assert result.data == ["NULL"]

    # Test "day in year" token without separators
    result = self.execute_query("select cast('20190011120' as timestamp "
        "FORMAT 'YYYYDDDHH12MI')")
    assert result.data == ["2019-01-01 11:20:00"]

    # Timestamp to string formatting
    result = self.execute_query("select cast(cast('2019-01-01' as timestamp) as string "
        "format'DDD')")
    assert result.data == ["001"]

    result = self.execute_query("select cast(cast('2019-12-31' as timestamp) as string "
        "format'DDD')")
    assert result.data == ["365"]

    result = self.execute_query("select cast(cast('2000-12-31' as timestamp) as string "
        "format'DDD')")
    assert result.data == ["366"]

    result = self.execute_query("select cast(cast('2019 123' as timestamp "
        "format 'YYYY DDD') as string format'DDD')")
    assert result.data == ["123"]

  def test_second_of_day(self):
    # Check boundaries
    result = self.client.execute("select cast('2019-11-10 86399.11' as "
        "timestamp FORMAT 'YYYY-MM-DD SSSSS.FF2')")
    assert result.data == ["2019-11-10 23:59:59.110000000"]

    result = self.client.execute("select cast('2019-11-10 0' as "
        "timestamp FORMAT 'YYYY-MM-DD SSSSS')")
    assert result.data == ["2019-11-10 00:00:00"]

    # Without separators full 5-digit "second of day" has to be given
    result = self.client.execute("select cast('11-10 036612019' as "
        "timestamp FORMAT 'MM-DD SSSSSYYYY')")
    assert result.data == ["2019-11-10 01:01:01"]

    # Check timezone offsets with "second of day"
    result = self.client.execute("select cast('2019-11-10 036611010' as "
        "timestamp FORMAT 'YYYY-MM-DD SSSSSTZHTZM')")
    assert result.data == ["2019-11-10 01:01:01"]

    # Timestamp to string formatting
    result = self.client.execute("select cast(cast('2019-01-01 01:01:01' as timestamp) "
        "as string format 'SSSSS')")
    assert result.data == ["03661"]

    result = self.client.execute("select cast(cast('2019-01-01' as timestamp) as string "
        "format 'SSSSS')")
    assert result.data == ["00000"]

    result = self.client.execute("select cast(cast('2019-01-01 23:59:59' as timestamp) "
        "as string format 'SSSSS')")
    assert result.data == ["86399"]

  def test_fraction_seconds(self):
    result = self.execute_query("select cast('2019-11-08 123456789' as "
        "timestamp FORMAT 'YYYY-MM-DD FF9')")
    assert result.data == ["2019-11-08 00:00:00.123456789"]

    result = self.execute_query("select cast('2019-11-08 1' as "
        "timestamp FORMAT 'YYYY-MM-DD FF')")
    assert result.data == ["2019-11-08 00:00:00.100000000"]

    result = self.execute_query("select cast('2019-11-08 1234567890' as "
        "timestamp FORMAT 'YYYY-MM-DD FF')")
    assert result.data == ["NULL"]

    result = self.execute_query("select cast('2019-11-08' as "
        "timestamp FORMAT 'YYYY-MM-DD FF')")
    assert result.data == ["NULL"]

    self.run_fraction_test(1)
    self.run_fraction_test(2)
    self.run_fraction_test(3)
    self.run_fraction_test(4)
    self.run_fraction_test(5)
    self.run_fraction_test(6)
    self.run_fraction_test(7)
    self.run_fraction_test(8)
    self.run_fraction_test(9)

  def run_fraction_test(self, length):
    MAX_LENGTH = 9
    fraction_part = ""
    for x in range(length):
      fraction_part += str(x + 1)
    template_input = "select cast('2019-11-08 %s' as timestamp FORMAT 'YYYY-MM-DD FF%s')"
    input_str = template_input % (fraction_part, length)

    expected = "2019-11-08 00:00:00." + fraction_part + ("0" * (MAX_LENGTH - length))
    result = self.execute_query(input_str)
    assert result.data == [expected]

    input2_str = template_input % (fraction_part + str(length + 1), length)
    result = self.execute_query(input2_str)
    assert result.data == ["NULL"]

  def test_meridiem_indicator(self):
    # Check 12 hour diff between AM and PM
    result = self.client.execute("select cast('2017-05-03 08 AM' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12 AM')")
    assert result.data == ["2017-05-03 08:00:00"]

    result = self.client.execute("select cast('2017-05-04 08 PM' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12 PM')")
    assert result.data == ["2017-05-04 20:00:00"]

    # Check that any meridiem indicator in the pattern matches any meridiem indicator in
    # the input
    result = self.client.execute("select cast('2017-05-05 12AM' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12PM')")
    assert result.data == ["2017-05-05 00:00:00"]

    result = self.client.execute("select cast('2017-05-06 P.M.12' as "
        "timestamp FORMAT 'YYYY-MM-DD AMHH12')")
    assert result.data == ["2017-05-06 12:00:00"]

    result = self.client.execute("select cast('2017-05-07 PM 01' as "
        "timestamp FORMAT 'YYYY-MM-DD A.M. HH12')")
    assert result.data == ["2017-05-07 13:00:00"]

    # Test lowercase indicator in input
    result = self.client.execute("select cast('2017-05-08 pm09' as "
        "timestamp FORMAT 'YYYY-MM-DD P.M.HH12')")
    assert result.data == ["2017-05-08 21:00:00"]

    result = self.client.execute("select cast('2017-05-09 10a.m.' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12PM')")
    assert result.data == ["2017-05-09 10:00:00"]

    # Test that '.' in indicator doesn't conflict with '.' as separator
    result = self.client.execute("select cast('2017-05-11 9.AM.10' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12.P.M..MI')")
    assert result.data == ["2017-05-11 09:10:00"]

    result = self.client.execute("select cast('2017-05-10.P.M..10' as "
        "timestamp FORMAT 'YYYY-MM-DD.AM.HH12')")
    assert result.data == ["2017-05-10 22:00:00"]

    # Timestamp to string formatting
    result = self.client.execute("select cast(cast('2019-01-01 00:15:10' as timestamp) "
        "as string format 'HH12 P.M.')")
    assert result.data == ["12 A.M."]

    result = self.client.execute("select cast(cast('2019-01-01 12:15:10' as timestamp) "
        "as string format 'HH12 AM')")
    assert result.data == ["12 PM"]

    result = self.client.execute("select cast(cast('2019-01-01 13:15:10' as timestamp) "
        "as string format 'HH12 a.m.')")
    assert result.data == ["01 p.m."]

    result = self.client.execute("select cast(cast('2019-01-01 23:15:10' as timestamp) "
        "as string format 'HH12 p.m.')")
    assert result.data == ["11 p.m."]

  def test_timezone_offsets(self):
    # Test positive timezone offset.
    result = self.client.execute("select cast('2018-01-01 10:00 AM +15:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-01-01 10:00:00"]

    # Test negative timezone offset.
    result = self.client.execute("select cast('2018-12-31 08:00 PM -15:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 20:00:00"]

    # Minus sign before TZM.
    result = self.client.execute("select cast('2018-12-31 08:00 AM 01:-59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    # Minus sign right before one digit TZH.
    result = self.client.execute("select cast('2018-12-31 08:00 AM--1:10' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('2018-12-31 08:00 AM-5:00' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M.TZH:TZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    # One digit negative TZH at the end of the input string.
    result = self.client.execute("select cast('2018-12-31 12:01 -1' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI TZH')")
    assert result.data == ["2018-12-31 12:01:00"]

    # Test timezone offset parsing without separators
    result = self.client.execute("select cast('201812310800AM+0515' as "
        "timestamp FORMAT 'YYYYMMDDHH12MIA.M.TZHTZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('201812310800AM0515' as "
        "timestamp FORMAT 'YYYYMMDDHH12MIA.M.TZHTZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    result = self.client.execute("select cast('201812310800AM-0515' as "
        "timestamp FORMAT 'YYYYMMDDHH12MIA.M.TZHTZM')")
    assert result.data == ["2018-12-31 08:00:00"]

    # Test signed zero TZH with not null TZM
    result = self.client.execute("select cast('2018-01-01 10:00 AM +00:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-01-01 10:00:00"]

    result = self.client.execute("select cast('2018-01-01 10:00 AM -00:59' as "
        "timestamp FORMAT 'YYYY-MM-DD HH12:MI A.M. TZH:TZM')")
    assert result.data == ["2018-01-01 10:00:00"]

    # Shuffle TZH and TZM into other elements
    result = self.client.execute("select cast('2018-01-01 15 10:00 1 AM' as "
        "timestamp FORMAT 'YYYY-MM-DD TZM HH12:MI TZH A.M.')")
    assert result.data == ["2018-01-01 10:00:00"]

    result = self.client.execute("select cast('2018-01-011510:00-01AM' as "
        "timestamp FORMAT 'YYYY-MM-DDTZMHH12:MITZHA.M.')")
    assert result.data == ["2018-01-01 10:00:00"]

    # Timezone offset with default time
    result = self.client.execute("select cast('2018-01-01 01:30' as timestamp "
        "FORMAT 'YYYY-MM-DD TZH:TZM')")
    assert result.data == ["2018-01-01 00:00:00"]

    # Single minus sign before two digit TZH.
    result = self.client.execute("select cast('2018-09-11 15:30:10-10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS-TZH')")
    assert result.data == ["2018-09-11 15:30:10"]

    # Non-digit TZH and TZM.
    result = self.client.execute("select cast('2018-09-11 17:30:10 ab:10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2018-09-11 17:30:10 -ab:10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2018-09-11 17:30:10 +ab:10' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

    result = self.client.execute("select cast('2018-09-11 18:30:10 10:ab' as timestamp "
        "FORMAT 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert result.data == ["NULL"]

  def test_format_parse_errors(self):
    # Invalid format
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'XXXX-dd-MM')")
    assert "Bad date/time conversion format: XXXX-dd-MM" in str(err)

    # Invalid use of SimpleDateFormat
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01 15:10' as timestamp format 'yyyy-MM-dd +hh:mm')")
    assert "Bad date/time conversion format: yyyy-MM-dd +hh:mm" in str(err)

    # Duplicate format element
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD MM')")
    assert "Invalid duplication of format element" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-YYYY')")
    assert "Invalid duplication of format element" in str(err)

    # Multiple year token provided
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-YY')")
    assert "Multiple year token provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYY-MM-DD-Y')")
    assert "Multiple year token provided" in str(err)

    # Year and round year conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YY-MM-DD-RRRR')")
    assert "Both year and round year are provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'RR-MM-DD-YYY')")
    assert "Both year and round year are provided" in str(err)

    # Day of year conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DDD')")
    assert "Day of year provided with day or month token" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-DD-DDD')")
    assert "Day of year provided with day or month token" in str(err)

    # Conflict between hour tokens
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH:HH24')")
    assert "Multiple hour tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH12:HH24')")
    assert "Multiple hour tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH12:HH')")
    assert "Multiple hour tokens provided" in str(err)

    # Conflict with median indicator
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD AM HH:MI A.M.')")
    assert "Multiple median indicator tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD PM HH:MI am')")
    assert "Multiple median indicator tokens provided" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH24:MI a.m.')")
    assert "Conflict between median indicator and hour token" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD p.m.')")
    assert "Missing hour token" in str(err)

    # Conflict with second of day
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD SSSSS HH')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH12:SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD HH24SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD MI SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD SS SSSSS')")
    assert "Second of day token conflicts with other token(s)" in str(err)

    # Too long format
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format '" +
        "{char: <101}".format(char="s") + "')")
    assert "The input format is too long" in str(err)

    # Timezone offsets in a datetime to string formatting
    err = self.execute_query_expect_failure(self.client,
        "select cast(cast('2017-05-01 01:15' as timestamp format 'YYYY-MM-DD TZH:TZM') "
        "as string format 'TZH')")
    assert "Timezone offset not allowed in a datetime to string conversion" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast(cast('2017-05-01 01:15' as timestamp format 'YYYY-MM-DD TZH:TZM') "
        "as string format 'TZM')")
    assert "Timezone offset not allowed in a datetime to string conversion" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast(cast('2017-05-01 01:15' as timestamp format 'YYYY-MM-DD TZH:TZM') "
        "as string format 'YYYY-MM-DD HH24:MI:SS TZH:TZM')")
    assert "Timezone offset not allowed in a datetime to string conversion" in str(err)

    # TZM requires TZH
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-12-31 08:00 AM 59' as timestamp FORMAT "
        "'YYYY-MM-DD HH12:MI A.M. TZM')")
    assert "TZH token is required for TZM" in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-12-31 08:00 AM -59' as timestamp FORMAT "
        "'YYYY-MM-DD HH12:MI A.M. TZM')")
    assert "TZH token is required for TZM" in str(err)

    # Multiple fraction second token conflict
    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'FF FF1')")
    assert "Multiple fractional second token provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'FF2 FF3')")
    assert "Multiple fractional second token provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'FF4 FF5')")
    assert "Multiple fractional second token provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'FF6 FF7')")
    assert "Multiple fractional second token provided." in str(err)

    err = self.execute_query_expect_failure(self.client,
        "select cast('2018-10-10' as timestamp format 'FF8 FF9')")
    assert "Multiple fractional second token provided." in str(err)

    # Verify that conflict check is not skipped when format ends with separators.
    err = self.execute_query_expect_failure(self.client,
        "select cast('2017-05-01' as timestamp format 'YYYY-MM-DD-RR--')")
    assert "Both year and round year are provided" in str(err)
