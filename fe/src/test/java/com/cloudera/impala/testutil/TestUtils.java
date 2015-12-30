// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Scanner;
import java.util.Map;
import java.io.StringWriter;
import java.io.StringReader;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.json.JsonReader;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.thrift.TClientRequest;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TSessionState;
import com.cloudera.impala.thrift.TSessionType;
import com.cloudera.impala.thrift.TUniqueId;

import com.google.common.collect.Maps;

public class TestUtils {
  private final static Logger LOG = LoggerFactory.getLogger(TestUtils.class);
  private final static String[] expectedFilePrefix_ = { "hdfs:", "file: " };
  private final static String[] ignoreContentAfter_ = { "HOST:", "LOCATIONS:" };
  // Special prefix that designates an expected value specified as a regex rather
  // than a literal
  private final static String regexAgainstActual_ = "regex:";

  // Our partition file paths are returned in the format of:
  // hdfs://<host>:<port>/<table>/year=2009/month=4/-47469796--667104359_25784_data.0
  // Everything after the month=4 can vary run to run so we want to filter this out
  // when comparing expected vs actual results. We also want to filter out the
  // host/port because that could vary run to run as well.
  private final static String HDFS_FILE_PATH_FILTER = "-*\\d+--\\d+_\\d+.*$";
  private final static String HDFS_HOST_PORT_FILTER = "//\\w+:\\d+/";

  /**
   * Do a line-by-line comparison of actual and expected output.
   * Comparison of the individual lines ignores whitespace.
   * If an expected line starts with expectedFilePrefix,
   * then the expected vs. actual comparison is successful if the actual string contains
   * the expected line (ignoring the expectedFilePrefix prefix).
   * If orderMatters is false, we consider actual to match expected if they
   * both contains the same output lines regardless of order.
   *
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutput(
      ArrayList<String> actual, ArrayList<String> expected, boolean orderMatters) {
    if (!orderMatters) {
      Collections.sort(actual);
      Collections.sort(expected);
    }
    int mismatch = -1; // line in actual w/ mismatch
    int maxLen = Math.min(actual.size(), expected.size());
    for (int i = 0; i < maxLen; ++i) {
      String expectedStr = expected.get(i).trim();
      String actualStr = actual.get(i);
      // Look for special prefixes in containsPrefixes.
      boolean containsPrefix = false;
      for (int prefixIdx = 0; prefixIdx < expectedFilePrefix_.length; ++prefixIdx) {
        containsPrefix = expectedStr.contains(expectedFilePrefix_[prefixIdx]);
        if (containsPrefix) {
          expectedStr = expectedStr.replaceFirst(expectedFilePrefix_[prefixIdx], "");
          actualStr = actualStr.replaceFirst(expectedFilePrefix_[prefixIdx], "");
          expectedStr = applyHdfsFilePathFilter(expectedStr);
          actualStr = applyHdfsFilePathFilter(actualStr);
          break;
        }
      }

      boolean ignoreAfter = false;
      for (int j = 0; j < ignoreContentAfter_.length; ++j) {
        ignoreAfter |= expectedStr.startsWith(ignoreContentAfter_[j]);
      }

      if (expectedStr.startsWith(regexAgainstActual_)) {
        // Get regex to check against by removing prefix.
        String regex = expectedStr.replace(regexAgainstActual_, "").trim();
        if (!actualStr.matches(regex)) {
          mismatch = i;
          break;
        }
        // Accept actualStr.
        continue;
      }

      // do a whitespace-insensitive comparison
      Scanner e = new Scanner(expectedStr);
      Scanner a = new Scanner(actualStr);
      while (a.hasNext() && e.hasNext()) {
        if (containsPrefix) {
          if (!a.next().contains(e.next())) {
            mismatch = i;
            break;
          }
        } else {
          if (!a.next().equals(e.next())) {
            mismatch = i;
            break;
          }
        }
      }
      if (mismatch != -1) {
        break;
      }

      if (ignoreAfter) {
        if (e.hasNext() && !a.hasNext()) {
          mismatch = i;
          break;
        }
      } else if (a.hasNext() != e.hasNext()) {
        mismatch = i;
        break;
      }
    }
    if (mismatch == -1 && actual.size() < expected.size()) {
      // actual is a prefix of expected
      StringBuilder output =
          new StringBuilder("Actual result is missing lines:\n");
      for (int i = 0; i < actual.size(); ++i) {
        output.append(actual.get(i)).append("\n");
      }
      output.append("Missing:\n");
      for (int i = actual.size(); i < expected.size(); ++i) {
        output.append(expected.get(i)).append("\n");
      }
      return output.toString();
    }

    if (mismatch != -1) {
      // print actual and expected, highlighting mismatch
      StringBuilder output =
          new StringBuilder("Actual does not match expected result:\n");
      for (int i = 0; i <= mismatch; ++i) {
        output.append(actual.get(i)).append("\n");
      }
      // underline mismatched line with "^^^..."
      for (int i = 0; i < actual.get(mismatch).length(); ++i) {
        output.append('^');
      }
      output.append("\n");
      for (int i = mismatch + 1; i < actual.size(); ++i) {
        output.append(actual.get(i)).append("\n");
      }
      output.append("\nExpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    if (actual.size() > expected.size()) {
      // print actual and expected
      StringBuilder output =
          new StringBuilder("Actual result contains extra output:\n");
      for (String str : actual) {
        output.append(str).append("\n");
      }
      output.append("\nExpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    return "";
  }

  /**
   * Applied a filter on the HDFS path to strip out information that might vary
   * from run to run.
   */
  private static String applyHdfsFilePathFilter(String hdfsPath) {
    hdfsPath = hdfsPath.replaceAll(HDFS_HOST_PORT_FILTER, " ");
    return hdfsPath.replaceAll(HDFS_FILE_PATH_FILTER, "");
  }


  /**
   * Create a TQueryCtx for executing FE tests.
   */
  public static TQueryCtx createQueryContext() {
    return createQueryContext(Catalog.DEFAULT_DB, System.getProperty("user.name"));
  }

  /**
   * Create a TQueryCtx for executing FE tests using the given default DB and user.
   */
  public static TQueryCtx createQueryContext(String defaultDb, String user) {
    TQueryCtx queryCtx = new TQueryCtx();
    queryCtx.setRequest(new TClientRequest("FeTests", new TQueryOptions()));
    queryCtx.setQuery_id(new TUniqueId());
    queryCtx.setSession(new TSessionState(new TUniqueId(), TSessionType.BEESWAX,
        defaultDb, user, new TNetworkAddress("localhost", 0)));
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    queryCtx.setNow_string(formatter.format(Calendar.getInstance().getTime()));
    queryCtx.setPid(1000);
    return queryCtx;
  }

  /**
   * Pretty print a JSON string.
   */
  public static String prettyPrintJson(String json) {
    StringWriter sw = new StringWriter();
    JsonWriter jsonWriter = null;
    try {
      JsonReader jr = Json.createReader(new StringReader(json));
      JsonObject jobj = jr.readObject();
      Map<String, Object> properties = Maps.newHashMap();
      properties.put(JsonGenerator.PRETTY_PRINTING, true);
      JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
      jsonWriter = writerFactory.createWriter(sw);
      jsonWriter.writeObject(jobj);
    } catch (Exception e) {
      LOG.error(String.format("Error pretty printing JSON string %s: %s", json,
        e.getMessage()));
      return "";
    } finally {
      if (jsonWriter != null) jsonWriter.close();
    }
    return sw.toString();
  }
}
