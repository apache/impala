// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.datagenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

class TestDataGenerator {
  private static void GenerateAllTypesData(String dir) throws IOException {
    GregorianCalendar date = new GregorianCalendar(2009, Calendar.JANUARY, 1);
    GregorianCalendar endDate = new GregorianCalendar(2010, Calendar.DECEMBER, 31);
    while (date.before(endDate)) {
      GenerateAllTypesPartition(dir, date);
      date.add(Calendar.MONTH, 1);
    }
  }

  private static void GenerateAllTypesPartition(String dir, Calendar startDate) throws IOException {
    SimpleDateFormat filenameFormat = new SimpleDateFormat("yyMM");
    PrintWriter writer =
        new PrintWriter(new FileWriter(new File(
            new File(dir), filenameFormat.format(startDate.getTime()) + ".txt")));

    Calendar date = (Calendar) startDate.clone();
    Calendar endDate = (Calendar) startDate.clone();
    endDate.add(Calendar.MONTH, 1);
    SimpleDateFormat df = new SimpleDateFormat("MM/dd/yy");
    int id = 0;
    while (date.before(endDate)) {
      for (int int_col = 0; int_col < 10; ++int_col) {
        boolean bool_col = (id % 2 == 0 ? true : false);
        byte tinyint_col = (byte)(int_col % 10);
        short smallint_col = (byte)(int_col % 100);
        long bigint_col = int_col * 10;
        float float_col = (byte)(1.1 * int_col);
        double double_col = 10.1 * int_col;
        String date_string_col = df.format(date.getTime());
        String string_col = String.valueOf(int_col);

        writer.format("%d,%b,%d,%d,%d,%d,", id, bool_col, tinyint_col, smallint_col,
                      int_col, bigint_col);
        writer.format("%f,%f,'%s','%s'\n", float_col, double_col, date_string_col, string_col);
        ++id;
      }

      date.add(Calendar.DAY_OF_MONTH, 1);
    }
    writer.close();
  }

  /**
   * Generate some test data.
   * @param args single argument: directory location of the output file
   * @throws Exception something bad happened
   */
  public static void main(String args[]) throws Exception {
    String allTypesPath = args[0] + "/AllTypes";
    File allTypesDir = new File(allTypesPath);
    allTypesDir.mkdirs();
    GenerateAllTypesData(args[0] + "/AllTypes");
  }
}
