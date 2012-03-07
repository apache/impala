// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.datagenerator;

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
    
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "bools:bool_col", parts[1]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "ints:tinyint_col", parts[2]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "ints:smallint_col", parts[3]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "ints:int_col", parts[4]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "ints:bigint_col", parts[5]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "floats:float_col", parts[6]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "floats:double_col", parts[7]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "strings:date_string_col", parts[8]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "strings:string_col", parts[9]);
    out.format("put '%s', '%s', '%s', '%s'\n", hbaseTableName, parts[0], "strings:timestamp_col", parts[10]);
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
    convertAllTypesTable("AllTypesError", "HBaseAllTypesError", "hbasealltypeserror");
    convertAllTypesTable("AllTypesErrorNoNulls", "HBaseAllTypesErrorNoNulls", "hbasealltypeserrornonulls");
    System.exit(0);
  }
}
