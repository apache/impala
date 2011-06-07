// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.io.StringReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

class Main {
  private static Options options = new Options();

  private static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("parser [options] \"query string\"", options);
    System.exit(1);
  }

  public static void main(String args[]) {
    options.addOption("d", false, "debug parser");

    if (args.length == 0) {
      usage();
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Error parsing command line:" + e.getMessage());
      usage();
    }

    args = cmd.getArgs();
    if (args.length != 1) {
      usage();
    }
    String queryString = args[0];

    StringReader sr = new StringReader(queryString);
    SqlParser sqlParser = new SqlParser(new SqlScanner(sr));
    try {
      if (cmd.hasOption("d")) {
        sqlParser.debug_parse();
      } else {
        sqlParser.parse();
      }
    } catch (Exception e) {
      System.err.println("Error parsing \"" + queryString + "\"");
      System.err.println(sqlParser.getErrorMsg(queryString));
      System.exit(2);
    }
  }
}
