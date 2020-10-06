// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.datagenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

class TestDataGenerator {
  // 2 years
  private static final int DEFAULT_NUM_PARTITIONS = 24;
  // 10 tuples per day of month
  private static final int DEFAULT_MAX_TUPLES_PER_PARTITION = 310;
  // arbitrary default value
  private static final int DEFAULT_END_YEAR = 2010;
  // for generating unique ids.
  private static int id = 0;

  private static void GenerateAllTypesData(String dir, int numPartitions,
      int maxTuplesPerPartition) throws IOException {
    id = 0;
    int numYears = Math.max((numPartitions / 12) - 1, 1);
    int startYear = Math.max(DEFAULT_END_YEAR - numYears, 0);
    GregorianCalendar date = new GregorianCalendar(startYear, Calendar.JANUARY, 1);
    GregorianCalendar endDate = new GregorianCalendar(DEFAULT_END_YEAR, Calendar.DECEMBER, 31);
    int months = 0;
    while (date.before(endDate) && months < numPartitions) {
      GregorianCalendar nextMonth = (GregorianCalendar) date.clone();
      nextMonth.add(Calendar.MONTH, 1);
      GenerateAllTypesPartition(dir, date, nextMonth, 10, maxTuplesPerPartition, false);
      date = nextMonth;
      ++months;
    }
  }

  private static void GenerateAllTypesAggData(String dir, boolean writeNulls)
      throws IOException {
    id = 0;
    int startYear = 2010;
    GregorianCalendar date = new GregorianCalendar(startYear, Calendar.JANUARY, 1);
    GregorianCalendar endDate = (GregorianCalendar) date.clone();
    endDate.add(Calendar.DAY_OF_MONTH, 10);
    while (date.before(endDate)) {
      GregorianCalendar nextDay = (GregorianCalendar) date.clone();
      nextDay.add(Calendar.DAY_OF_MONTH, 1);
      GenerateAllTypesPartition(dir, date, nextDay, 1000, 1000, writeNulls);
      date = nextDay;
    }
  }

  private static void GenerateAllTypesPartition(String dir, Calendar startDate,
      Calendar endDate, int intsPerDay, int maxTuplesPerPartition, boolean writeNulls)
      throws IOException {
    SimpleDateFormat filenameFormat = new SimpleDateFormat("yyMMdd");
    PrintWriter writer = new PrintWriter(new FileWriter(new File(new File(dir),
        filenameFormat.format(startDate.getTime()) + ".txt")));
    Calendar date = (Calendar) startDate.clone();
    SimpleDateFormat df = new SimpleDateFormat("MM/dd/yy");
    SimpleDateFormat tsf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

    int count = 0;
    while (date.before(endDate) && count < maxTuplesPerPartition) {
      for (int int_col = 0; int_col < intsPerDay && count < maxTuplesPerPartition;
           ++int_col) {
        boolean bool_col = (id % 2 == 0 ? true : false);
        byte tinyint_col = (byte) (int_col % 10);
        short smallint_col = (short) (int_col % 100);
        long bigint_col = int_col * 10;
        float float_col = (float) (1.1 * int_col);
        double double_col = 10.1 * int_col;
        String date_string_col = df.format(date.getTime());
        String string_col = String.valueOf(int_col);
        String timestamp_col = tsf.format(date.getTime());
        writer.format("%d,%b,%s,%s,%s,%s,", id, bool_col,
            (writeNulls && tinyint_col == 0 ? "" : Byte.toString(tinyint_col)),
            (writeNulls && smallint_col == 0 ? "" : Short.toString(smallint_col)),
            (writeNulls && int_col == 0 ? "" : Integer.toString(int_col)),
            (writeNulls && bigint_col == 0 ? "" : Long.toString(bigint_col)));
        writer.format("%s,%s,%s,%s,%s\n",
            (writeNulls && int_col == 0 ? "" : Float.toString(float_col)),
            (writeNulls && int_col == 0 ? "" : Double.toString(double_col)),
            date_string_col, string_col, timestamp_col);
        ++id;
        ++count;
        date.add(Calendar.MINUTE, 1);
        date.add(Calendar.MILLISECOND, (int)bigint_col);
      }
      date.add(Calendar.DAY_OF_MONTH, 1);
    }
    writer.close();
  }

  // Generate cols with schema: DECIMAL(10, 4) and DECIMAL(15, 5), DECIMAL(1,1)
  private static void GenerateDecimalData(String dir, int numRows) throws IOException {
    PrintWriter writer = new PrintWriter(new FileWriter(new File(new File(dir),
        "data.txt")));
    double col1 = 0;
    double col2 = 100;
    double col3 = 0;
    double col1Delta = 0.1111;
    double col2Delta = 1.22222;
    double col3Delta = 0.1;

    for (int i = 0; i < numRows; ++i) {
      if (i % 10 == 0) col3 = 0;
      String s1 =
          BigDecimal.valueOf(col1).setScale(4, BigDecimal.ROUND_HALF_UP).toString();
      String s2 =
          BigDecimal.valueOf(col2).setScale(5, BigDecimal.ROUND_HALF_UP).toString();
      String s3 =
          BigDecimal.valueOf(col3).setScale(1, BigDecimal.ROUND_HALF_UP).toString();

      writer.format("%s,%s,%s\n", s1, s2,s3);

      col1 += col1Delta;
      col2 += col2Delta;
      col3 += col3Delta;
    }
    writer.close();
  }

  /**
   * Generate some test data.
   *
   * @param BaseOutputDirectory
   *          : Required base output folder of generated data files.
   * @throws Exception
   *           something bad happened
   */
  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: " + "TestDataGenerator BaseOutputDirectory");
    }

    // The TimeZone should be the same no matter what the TimeZone is of the computer
    // running this code, in order to ensure the generated data is always the same.
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    // Generate AllTypes
    String dirName = args[0] + "/AllTypes";
    File dir = new File(dirName);
    dir.mkdirs();
    GenerateAllTypesData(dirName, DEFAULT_NUM_PARTITIONS,
        DEFAULT_MAX_TUPLES_PER_PARTITION);

    // Generate AllTypesSmall
    dirName = args[0] + "/AllTypesSmall";
    dir = new File(dirName);
    dir.mkdirs();
    GenerateAllTypesData(dirName, 4, 25);

    // Generate AllTypesSmall
    dirName = args[0] + "/AllTypesTiny";
    dir = new File(dirName);
    dir.mkdirs();
    GenerateAllTypesData(dirName, 4, 2);

    // Generate AllTypesAgg
    dirName = args[0] + "/AllTypesAgg";
    dir = new File(dirName);
    dir.mkdirs();
    GenerateAllTypesAggData(dirName, true);

    // Generate AllTypesAgg w/o nulls
    dirName = args[0] + "/AllTypesAggNoNulls";
    dir = new File(dirName);
    dir.mkdirs();
    GenerateAllTypesAggData(dirName, false);

    // Generate Decimal data
    dirName = args[0] + "/DecimalTiny";
    dir = new File(dirName);
    dir.mkdirs();
    GenerateDecimalData(dirName, 100);
  }
}
