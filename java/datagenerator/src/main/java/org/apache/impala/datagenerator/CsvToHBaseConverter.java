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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

// Converts CSV files for the AllTypes table into HBase shell commands that insert the same data.
// Reads all CSV files from the given input directory and write a single file containing all HBase commands.
public class CsvToHBaseConverter {

  public static void convertAllTypesTable(String inDir, String outDir, String hbaseTableName) {
    File inDirFile = new File(inDir);
    File outDirFile = new File(outDir);
    if (!outDirFile.exists()) {
      outDirFile.mkdirs();
    }
    try {
      FileWriter outFile = new FileWriter(outDirFile.getAbsolutePath() + "/"
          + hbaseTableName + ".hbase");
      PrintWriter out = new PrintWriter(outFile);
      String[] children = inDirFile.list();
      for (String csvFile : children) {
        if (!csvFile.endsWith(".txt")) continue;

        FileInputStream finstream = new FileInputStream(
            inDirFile.getAbsolutePath() + "/" + csvFile);
        DataInputStream in = new DataInputStream(finstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String csvLine;
        while ((csvLine = br.readLine()) != null) {
          convertLine(csvLine, out, hbaseTableName);
        }        
        in.close();
      }
      out.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
  
  public static void convertLine(String csvLine, PrintWriter out, String hbaseTableName) throws IOException {
    String[] parts = csvLine.split(",");
    if (parts.length != 11) {
      throw new IOException("Line does not have 11 columns: " + csvLine);
    }        
    
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:bigint_col", parts[5]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:bool_col", parts[1]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:date_string_col", parts[8]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:double_col", parts[7]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:float_col", parts[6]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:int_col", parts[4]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:smallint_col", parts[3]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:string_col", parts[9]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:timestamp_col", parts[10]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "d:tinyint_col", parts[2]);
  }
  
  
  /**
   * Convert CSV test data in a file with HBase shell commands for inserting the same data. 
   * 
   * @param BaseOutputDirectory
   *          : Required base output folder of generated data files.
   * @throws Exception
   *           something bad happened
   */
  public static void main(String args[]) throws Exception {
    convertAllTypesTable("AllTypesError", "HBaseAllTypesError", "functional_hbase.hbasealltypeserror");
    convertAllTypesTable("AllTypesErrorNoNulls", "HBaseAllTypesErrorNoNulls", 
        "functional_hbase.hbasealltypeserrornonulls");
    System.exit(0);
  }
}
