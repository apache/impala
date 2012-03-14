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

  // the whole <QUERY STRING>, including comment and new line
  private final StringBuilder querySection = new StringBuilder();
  private int lineNum = 0;
  private final StringBuilder confString = new StringBuilder();
  private final ArrayList<ArrayList<String>> expectedResultSections = Lists.newArrayList();
  private final String fileName;
  private InputStream stream;
  private Scanner scanner;

  enum ParserState {
    QUERY,
    EXPECTED_RESULT
  }

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
    confString.setLength(0);
    query.setLength(0);
    querySection.setLength(0);
    ParserState state = ParserState.QUERY;
    ArrayList<String> resultSection = null;
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      ++lineNum;
      // ignore comments
      if (line.startsWith("//") || line.startsWith("#")) {
        if (state == ParserState.QUERY) {
          querySection.append(line + "\n");
        }
        continue;
      }
      if (line.startsWith("====")) {
        break; // done w/ this query
      }
      if (line.startsWith("----")) {
        // start of plan output section
        state = ParserState.EXPECTED_RESULT;
        resultSection = new ArrayList<String>();
        expectedResultSections.add(resultSection);
      } else {
        // Line is not a section indicator.
        switch(state) {
        case QUERY: {
          query.append(line);
          query.append(" ");
          querySection.append(line + "\n");
          break;
        }
        case EXPECTED_RESULT: {
          resultSection.add(line);
          break;
        }
        }
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
    if (sectionIndex < expectedResultSections.size()) {
      return expectedResultSections.get(sectionIndex);
    } else {
      return new ArrayList<String>();
    }
  }

  public String getConfString() {
    return confString.toString();
  }

  /**
   * Return the query string, stripping out comments and newline.
   * @return
   */
  public String getQuery() {
    return query.toString();
  }

  /**
   * Return the query section from the testfile, including comments and newline
   * @return
   */
  public String getQuerySection() {
    return querySection.toString();
  }

  public int getLineNum() {
    return lineNum;
  }
}
