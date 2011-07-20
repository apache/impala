// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.common.collect.Lists;

// Parses and provides an iterator-like interface to a text file with the following format:
// <QUERY STRING>
// ----
// <EXPECTED RESULT SECTION 1>
// ----
// <EXPECTED RESULT SECTION 2>
// ----
// <EXPECTED RESULT SECTION 3>
// ====
// <QUERY STRING>
// ----
// <EXPECTED RESULT SECTION 1>
// ----
// <EXPECTED RESULT SECTION 2>
// ----
// <EXPECTED RESULT SECTION 3>
// ====
// ...
// Note that <QUERY STRING> and <EXPECTED RESULT SECTIONS> can consist of multiple lines.
public class TestFileParser {
  private final StringBuilder query = new StringBuilder();
  private final ArrayList<ArrayList<String>> expectedResultSections = Lists.newArrayList();
  private final String fileName;
  private InputStream stream;
  private Scanner scanner;

  public TestFileParser(String fileName) {
    this.fileName = fileName;
  }

  public void open() {
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      stream = classLoader.getResourceAsStream(fileName);
      scanner = new Scanner(stream);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public boolean hasNext() {
    return scanner.hasNextLine();
  }

  public void next() {
    expectedResultSections.clear();
    query.setLength(0);
    boolean readQuery = true;
    ArrayList<String> resultSection = null;
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      // ignore comments
      if (line.startsWith("//") || line.startsWith("#")) {
        continue;
      }
      if (line.startsWith("=")) {
        break; // done w/ this query
      }
      if (line.startsWith("-")) {
        // start of plan output section
        resultSection = new ArrayList<String>();
        expectedResultSections.add(resultSection);
        readQuery = false;
      } else if (readQuery) {
        query.append(line);
        query.append(" ");
      } else {
        resultSection.add(line);
      }
    }
  }

  public void close() {
    try {
      stream.close();
    } catch (IOException e) {
      // ignore
    }
  }

  public ArrayList<ArrayList<String>> getExpectedResultSections() {
    return expectedResultSections;
  }

  public ArrayList<String> getExpectedResult(int sectionIndex) {
    return expectedResultSections.get(sectionIndex);
  }

  public String getQuery() {
    return query.toString();
  }
}