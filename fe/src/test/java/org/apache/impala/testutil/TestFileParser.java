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

package org.apache.impala.testutil;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Scanner;

import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Parses a file containing one or more test case descriptions into a list of TestCase
 * objects.
 * A test file has the following format:
 *
 * <QUERY STRING>
 * ---- QUERYOPTIONS
 * <QUERYOPTION1>=<VALUE1>
 * <QUERYOPTION2>=<VALUE2>
 * ..
 * ---- <SECTION NAME 1>
 * <EXPECTED CONTENTS 1>
 * ---- <SECTION NAME 2>
 * <EXPECTED CONTENTS 2>
 * ---- <SECTION NAME 3>
 * <EXPECTED CONTENTS 3>
 * ====
 * <QUERY STRING>
 * ---- QUERYOPTIONS
 * <QUERYOPTION1>=<VALUE1>
 * <QUERYOPTION2>=<VALUE2>
 * ..
 * ---- <SECTION NAME 1>
 * <EXPECTED CONTENTS 1>
 * ---- <SECTION NAME 2>
 * <EXPECTED CONTENTS 2>
 * ---- <SECTION NAME 3>
 * <EXPECTED CONTENTS 3>
 * ====
 * Acceptable section names are ONLY those contained in TestFileParser.Section.
 *
 * Lines beginning with # or // are comments. Clients can retrieve sections with or
 * without these lines included.
 *
 * Note that <QUERY STRING> and <EXPECTED CONTENTS> sections can consist of multiple
 * lines. QUERYOPTIONS sections may contain multiple <QUERYOPTION>=<VALUE> lines.
 */
public class TestFileParser {
  private static final Logger LOG = Logger.getLogger(TestCase.class);

  /**
   * Valid section titles.
   */
  public enum Section {
    QUERY,
    TYPES,
    COLLABELS,
    RESULTS,
    PLAN,
    DISTRIBUTEDPLAN,
    PARALLELPLANS,
    FILEERRORS,
    PARTITIONS,
    SETUP,
    ERRORS,
    SCANRANGELOCATIONS,
    LINEAGE,
    QUERYOPTIONS,
    HIVE_MAJOR_VERSION;

    // Return header line for this section
    public String getHeader() {
      return "---- " + this.toString();
    }
  }

  /**
   * A container class for a single test case's sections.
   * A section is a list of strings.
   */
  public static class TestCase {

    private final EnumMap<Section, ArrayList<String>> expectedResultSections =
        Maps.newEnumMap(Section.class);

    // Line number in the test case file where this case started
    private final int startLineNum;
    private TQueryOptions options;

    public TestCase(int lineNum, TQueryOptions options) {
      this.startLineNum = lineNum;
      this.options = options;
    }

    public int getStartingLineNum() { return startLineNum; }

    public TQueryOptions getOptions() { return this.options; }

    public void setOptions(TQueryOptions options) { this.options = options; }

    protected void addSection(Section section, ArrayList<String> contents) {
      expectedResultSections.put(section, contents);
    }

    /**
     * Returns a section corresponding to the given key, or an empty list if one does not
     * exist. Comments are not included.
     */
    public ArrayList<String> getSectionContents(Section section) {
      return getSectionContents(section, false);
    }

    public ArrayList<String> getSectionContents(Section section, boolean withComments) {
      return getSectionContents(section, withComments, null);
    }

    /**
     * Returns a section corresponding to the given key, or an empty list if one does not
     * exist.
     * @param section
     *          The Section to get
     * @param withComments
     *          If set, all comment lines are included.
     * @param dbSuffix
     *          If set, table names that contain the string $DATABASE will be replaced
     *          with the specified table suffix
     * @return Collection of strings mapping to lines in the test file
     */
    public ArrayList<String> getSectionContents(Section section, boolean withComments,
                                                String dbSuffix) {
      ArrayList<String> ret = expectedResultSections.get(section);
      if (ret == null) {
        return Lists.newArrayList();
      } else if (withComments && dbSuffix == null) {
        return ret;
      }

      ArrayList<String> retList = Lists.newArrayList();
      for (String s : ret) {
        if (!(s.startsWith("#") || s.startsWith("//"))) {
          if (dbSuffix != null) {
            retList.add(s.replaceAll("\\$DATABASE", dbSuffix));
          } else {
            retList.add(s);
          }
        } else if (withComments) {
          retList.add(s);
        }
      }

      return retList;
    }

    public String getSectionAsString(Section section, boolean withComments,
        String delimiter) {
      return getSectionAsString(section, withComments, delimiter, null);
    }

    /**
     * Returns a section concatenated into a single string, with the supplied delimiter
     * used to separate each line.
     */
    public String getSectionAsString(Section section, boolean withComments,
                                     String delimiter, String dbSuffix) {
      List<String> sectionList = getSectionContents(section, withComments, dbSuffix);
      if (sectionList == null) {
        return null;
      }

      return Joiner.on(delimiter).join(sectionList);
    }

    /**
     * Returns the QUERY section as a string, with each line separated by a " ".
     */
    public String getQuery() {
      return getSectionAsString(Section.QUERY, false, "\n");
    }

    /**
     * Returns false if the current test case is invalid due to missing sections or query
     */
    public boolean isValid() {
      return !getQuery().isEmpty()
          && (!getSectionContents(Section.PLAN).isEmpty()
            || !getSectionContents(Section.DISTRIBUTEDPLAN).isEmpty()
            || !getSectionContents(Section.PARALLELPLANS).isEmpty()
            || !getSectionContents(Section.LINEAGE).isEmpty());
    }

    public boolean isEmpty() { return expectedResultSections.isEmpty(); }
  }

  private final List<TestCase> testCases = Lists.newArrayList();

  private int lineNum = 0;
  private final String fileName;
  private BufferedReader reader;
  private Scanner scanner;
  private boolean hasSetupSection = false;
  private TQueryOptions options;

  /**
   * For backwards compatibility, if no title is found this is the order in which
   * sections are labeled.
   */
  static private final ArrayList<Section> defaultSectionOrder =
    Lists.newArrayList(Section.QUERY, Section.TYPES, Section.RESULTS);

  public TestFileParser(String fileName, TQueryOptions options) {
    this.fileName = fileName;
    this.options = options;
  }

  public List<TestCase> getTestCases() {
    return testCases;
  }

  public String getTestFileName() {
    return fileName;
  }

  public boolean hasSetupSection() {
    return hasSetupSection;
  }

  /**
   * Initializes the scanner and the input stream corresponding to the test file name
   */
  private void open(String table) {
    try {
      String testFileBaseDir =
          new File(System.getenv("IMPALA_HOME"), "testdata/workloads").getPath();
      String fullPath = new File(testFileBaseDir, fileName).getPath();
      reader = new BufferedReader(new FileReader(fullPath));
      scanner = new Scanner(reader);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * Consumes input from the test file until a single test case has been parsed.
   */
  private TestCase parseOneTestCase() {
    Section currentSection = Section.QUERY;
    ArrayList<String> sectionContents = Lists.newArrayList();
    // Each test case in the test file has its own copy of query options.
    TestCase currentTestCase = new TestCase(lineNum, options.deepCopy());
    int sectionCount = 0;

    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      ++lineNum;
      if (line.startsWith("====") && sectionCount > 0) {
        currentTestCase.addSection(currentSection, sectionContents);
        parseQueryOptions(currentTestCase);
        if (!currentTestCase.isValid()) {
          throw new IllegalStateException("Invalid test case" +
              " at line " + currentTestCase.startLineNum + " detected.");
        }
        return currentTestCase; // done with this test case
      }
      if (line.startsWith("----")) {
        sectionCount++;
        // New section
        currentTestCase.addSection(currentSection, sectionContents);
        boolean found = false;
        line = line.trim().toLowerCase();

        // Check for section header - a missing header probably means an old test file.
        if (!line.endsWith("----")) {
          for (Section s : Section.values()) {
            if (line.matches("----\\s+" + s.toString().toLowerCase() + "\\b.*")) {
              currentSection = s;
              if (s == Section.SETUP) {
                hasSetupSection = true;
              }
              found = true;
              break;
            }
          }
          if (!found) {
            throw new IllegalStateException("Unknown section name: " + line);
          }
        } else {
          // Backwards compatibility only - TODO remove once all test files have section
          // headers.
          if (sectionCount >= defaultSectionOrder.size()) {
            throw new IllegalStateException("Unexpected number of untitled sections: "
                + sectionCount);
          }
          currentSection = defaultSectionOrder.get(sectionCount);
          LOG.warn("No section header found. Guessing: " + currentSection);
        }

        if (!currentTestCase.getSectionContents(currentSection).isEmpty()) {
          throw new IllegalStateException("Duplicate sections are not allowed: "
              + currentSection);
        }
        sectionContents = Lists.newArrayList();
      } else {
        sectionContents.add(line);
      }
    }

    if (!currentTestCase.isEmpty() && !currentTestCase.isValid()) {
      throw new IllegalStateException("Invalid test case" +
          " at line " + currentTestCase.startLineNum + " detected.");
    }

    return currentTestCase;
  }

  /**
   * Parses QUERYOPTIONS section. Adds the parsed query options to "testCase.options".
   * Throws an IllegalStateException if parsing failed.
   */
  private void parseQueryOptions(TestCase testCase) {
    String optionsStr = testCase.getSectionAsString(Section.QUERYOPTIONS, false, ",");
    if (optionsStr == null || optionsStr.isEmpty()) return;

    TQueryOptions result = null;
    try {
      result = FeSupport.ParseQueryOptions(optionsStr, testCase.getOptions());
    } catch (InternalException e) {
      throw new IllegalStateException("Failed to parse query options: " + optionsStr +
          " - " + e.getMessage(), e);
    }
    Preconditions.checkNotNull(result);
    testCase.setOptions(result);
  }

  /**
   * Parses a test file in its entirety and constructs a list of TestCases.
   */
  public void parseFile() {
    parseFile(null);
  }

  public void parseFile(String table) {
    try {
      open(table);
      testCases.clear();
      while (scanner.hasNextLine()) {
        TestCase testCase = parseOneTestCase();
        if (!testCase.isEmpty()) testCases.add(testCase);
      }
    } finally {
      close();
    }
  }

  private void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }

    if (scanner != null) {
      scanner.close();
    }
  }
}
