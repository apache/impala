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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TUniqueId;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class TestUtils {
  private final static Logger LOG = LoggerFactory.getLogger(TestUtils.class);
  private final static String[] ignoreContentAfter_ = { "HOST:", "LOCATIONS:" };
  // Special prefix that designates an expected value specified as a regex rather
  // than a literal
  private final static String REGEX_AGAINST_ACTUAL = "regex:";

  // Regexes that match various elements in plan.
  private final static String NUMBER_REGEX = "\\d+(\\.\\d+)?";
  private final static String BYTE_SUFFIX_REGEX = "[KMGT]?B";
  private final static String BYTE_VALUE_REGEX = NUMBER_REGEX + BYTE_SUFFIX_REGEX;
  // Note: The older Hive Server JDBC driver (Hive .9 and earlier) is named similarly:
  // "org.apache.hadoop.hive.jdbc.HiveDriver". However, Impala currently only supports
  // the Hive Server 2 JDBC driver (Hive .10 and later).
  final static String HIVE_SERVER2_DRIVER_NAME =
      "org.apache.hive.jdbc.HiveDriver";
  // The default connection string template to connect to localhost on a given port
  // number.
  final static String HS2_CONNECTION_TEMPLATE = "jdbc:hive2://localhost:%d/default";

  public interface ResultFilter {
    public boolean matches(String input);
    public String transform(String input);
  }

  // Our partition file paths are returned in the format of:
  // hdfs://<host>:<port>/<table>/year=2009/month=4/-47469796--667104359_25784_data.0
  // Everything after the month=4 can vary run to run so we want to filter this out
  // when comparing expected vs actual results. We also want to filter out the
  // host/port because that could vary run to run as well.
  static class PathFilter implements ResultFilter {
    private final static String PATH_FILTER = "-*\\d+--\\d+_\\d+.*$";
    private final static String PORT_FILTER = "//\\w+(\\.\\w+)?(\\.\\w+)?:\\d+";
    private final String filterKey_;

    public PathFilter(String prefix) { filterKey_ = prefix; }

    @Override
    public boolean matches(String input) { return input.contains(filterKey_); }

    @Override
    public String transform(String input) {
      String result = input.replaceFirst(filterKey_, "");
      result = result.replaceAll(PATH_FILTER, " ");
      return result.replaceAll(PORT_FILTER, "");
    }
  }

  /**
   * Filter to ignore the value from elements in the format key=value.
   */
  public static class IgnoreValueFilter implements ResultFilter {
    // Literal string containing the key name.
    protected final String keyPrefix;
    protected final String valueRegex;

    /**
     * Create a filter that ignores the value from key value pairs where the key is
     * the literal 'key' value and the value matches 'valueRegex'. The key and the
     * value are separated by the 'separator' character.
     */
    public IgnoreValueFilter(
        String key, String valueRegex, char separator) {
      // Include leading space to avoid matching partial keys, e.g. if key is "bar" we
      // don't want to match "foobar=".
      this.keyPrefix = " " + key + Character.toString(separator);
      this.valueRegex = valueRegex;
    }

    /**
     *  Create a filter that ignores the value from key value pairs where the key is
     *  the literal 'key' value and the value matches 'valueRegex'. The key and the
     *  value are separated by '='.
     */
    public IgnoreValueFilter(String key, String valueRegex) {
      this(key, valueRegex, '=');
    }

    @Override
    public boolean matches(String input) { return input.contains(keyPrefix); }

    @Override
    public String transform(String input) {
      return input.replaceAll(keyPrefix + valueRegex, keyPrefix);
    }
  }

  /**
   * Filter to replace the value from elements in the format key=value.
   */
  public static class ReplaceValueFilter extends IgnoreValueFilter {
    // Literal string containing the replacement regex.
    private final String replaceRegex;

    /**
     * Create a filter that replaces the value from key value pairs where the key is
     * the literal 'key' value and the value matches 'valueRegex'. The key and the
     * value are separated by the 'separator' character.
     */
    public ReplaceValueFilter(
        String key, String valueRegex, String replaceRegex, char separator) {
      super(key, valueRegex, separator);
      this.replaceRegex = replaceRegex;
    }

    @Override
    public String transform(String input) {
      return input.replaceAll(keyPrefix + valueRegex, keyPrefix + replaceRegex);
    }
  }

  /**
   * Filter to ignore the filesystem schemes in the scan node explain output. See
   * {@link org.apache.impala.planner.PlannerTestBase.PlannerTestOption#VALIDATE_SCAN_FS}
   * for more details.
   */
  public static final ResultFilter SCAN_NODE_SCHEME_FILTER = new ResultFilter() {

    private final String fsSchemes = "(HDFS|S3|LOCAL|ADLS|OSS)";
    private final Pattern scanNodeFsScheme = Pattern.compile("SCAN " + fsSchemes);
    // We don't match the size because the FILE_SIZE_FILTER could remove it
    private final Pattern scanNodeInputMetadata =
        Pattern.compile(fsSchemes + " partitions=\\d+/\\d+ files=\\d+ size=");

    @Override
    public boolean matches(String input) {
      return scanNodeInputMetadata.matcher(input).find()
          || scanNodeFsScheme.matcher(input).find();
    }

    @Override
    public String transform(String input) {
      return input.replaceAll(fsSchemes, "");
    }
  };

  // File size could vary from run to run. For example, the parquet file header size
  // or column metadata size could change if the Impala version changes. That doesn't
  // mean anything is wrong with the plan, so we want to filter the file size out.
  public static final IgnoreValueFilter FILE_SIZE_FILTER =
      new IgnoreValueFilter("size", BYTE_VALUE_REGEX);

  // Ignore the row-size=8B entries
  public static final IgnoreValueFilter ROW_SIZE_FILTER =
      new IgnoreValueFilter("row-size", "\\S+");

  // Ignore 'cardinality=27.30K' or 'cardinality=unavailable' or
  // 'cardinality=1.58K(filtered from 2.88M)' entries.
  // See PlanNode.getExplainString().
  public static final IgnoreValueFilter CARDINALITY_FILTER = new IgnoreValueFilter(
      "cardinality", "(unavailable|[0-9\\.KMGT]+(\\(filtered from [0-9\\.KMGT]+\\))?)");

  public static final IgnoreValueFilter ICEBERG_SNAPSHOT_ID_FILTER =
      new IgnoreValueFilter("Iceberg snapshot id", " \\d+", ':');

  // Ignore any values after 'rows=' in partitions: 0/24 rows=12.83K or
  // partitions: 0/24 rows=unavailable entries
  public static final ReplaceValueFilter PARTITIONS_FILTER =
      new ReplaceValueFilter("partitions", "( \\d+/\\d+ rows=)\\S+", "$1", ':');

  // Ignore the exact estimated row count, which depends on the file sizes.
  static IgnoreValueFilter SCAN_RANGE_ROW_COUNT_FILTER =
      new IgnoreValueFilter("max-scan-range-rows", PrintUtils.METRIC_REGEX);

  // Filters that are always applied
  private static final List<ResultFilter> DEFAULT_FILTERS = Arrays.<ResultFilter>asList(
    SCAN_RANGE_ROW_COUNT_FILTER, new PathFilter("hdfs:"), new PathFilter("file: "));

  // Filters that ignore the values of resource requirements that appear in
  // "EXTENDED" and above explain plans.
  public static final List<ResultFilter> RESOURCE_FILTERS = Arrays.<ResultFilter>asList(
      new IgnoreValueFilter("mem-estimate", BYTE_VALUE_REGEX),
      new IgnoreValueFilter("mem-reservation", BYTE_VALUE_REGEX),
      new IgnoreValueFilter("thread-reservation", NUMBER_REGEX),
      new IgnoreValueFilter("Memory", BYTE_VALUE_REGEX),
      new IgnoreValueFilter("Threads", NUMBER_REGEX));

  /**
   * Do a line-by-line comparison of actual and expected output.
   * Comparison of the individual lines ignores whitespace.
   * If an expected line starts with expectedFilePrefix,
   * then the expected vs. actual comparison is successful if the actual string contains
   * the expected line (ignoring the expectedFilePrefix prefix).
   * If orderMatters is false, we consider actual to match expected if they
   * both contains the same output lines regardless of order.
   * lineFilters is a list of filters that are applied to corresponding lines in the
   * actual and expected output if the filter matches the expected output.
   *
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutput(ArrayList<String> actual, ArrayList<String> expected,
      boolean orderMatters, List<ResultFilter> lineFilters) {
    if (!orderMatters) {
      Collections.sort(actual);
      Collections.sort(expected);
    }
    int mismatch = -1; // line in actual w/ mismatch
    int maxLen = Math.min(actual.size(), expected.size());
    outer:
    for (int i = 0; i < maxLen; ++i) {
      String expectedStr = expected.get(i);
      String actualStr = actual.get(i);
      // Apply all default and caller-supplied filters to the expected and actual output.
      boolean containsPrefix = false;
      for (List<ResultFilter> filters:
          Arrays.<List<ResultFilter>>asList(DEFAULT_FILTERS, lineFilters)) {
        for (ResultFilter filter: filters) {
          if (filter.matches(expectedStr) || filter.matches(actualStr)) {
            containsPrefix = true;
            expectedStr = filter.transform(expectedStr);
            actualStr = filter.transform(actualStr);
          }
        }
      }

      boolean ignoreAfter = false;
      for (int j = 0; j < ignoreContentAfter_.length; ++j) {
        ignoreAfter |= expectedStr.startsWith(ignoreContentAfter_[j]);
      }
      if (expectedStr.startsWith(REGEX_AGAINST_ACTUAL)) {
        // Get regex to check against by removing prefix.
        String regex = expectedStr.replace(REGEX_AGAINST_ACTUAL, "").trim();
        if (!actualStr.matches(regex)) {
          mismatch = i;
          break;
        }
        // Accept actualStr.
        continue;
      }

      // do a whitespace-insensitive comparison
      try (Scanner e = new Scanner(expectedStr);
           Scanner a = new Scanner(actualStr)) {
        while (a.hasNext() && e.hasNext()) {
          String aToken = a.next();
          String eToken = e.next();
          if (containsPrefix) {
            if (!aToken.contains(eToken)) {
              mismatch = i;
              break outer;
            }
          } else if (!aToken.equals(eToken)) {
            mismatch = i;
            break outer;
          }
        }

        if (ignoreAfter) {
          if (e.hasNext() && !a.hasNext()) {
            mismatch = i;
            break outer;
          }
        } else if (a.hasNext() != e.hasNext()) {
          mismatch = i;
          break outer;
        }
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
   * Create a TQueryCtx for executing FE tests.
   */
  public static TQueryCtx createQueryContext() {
    return createQueryContext(Catalog.DEFAULT_DB, System.getProperty("user.name"));
  }

  public static TQueryCtx createQueryContext(String defaultDb, String user) {
    TQueryCtx queryCtx = createQueryContext(defaultDb, user, new TQueryOptions());
    // Disable rewrites by default because some analyzer tests have non-executable
    // constant exprs (e.g. dummy UDFs) that do not work with constant folding.
    queryCtx.client_request.query_options.setEnable_expr_rewrites(false);
    return queryCtx;
  }

  public static TQueryCtx createQueryContext(TQueryOptions options) {
    return createQueryContext(Catalog.DEFAULT_DB,
        System.getProperty("user.name"), options);
  }

  /**
   * Create a TQueryCtx for executing FE tests using the given default DB and user.
   * Expr rewrites are disabled by default and should be enabled by the caller
   * if so desired.
   */
  public static TQueryCtx createQueryContext(String defaultDb,
      String user, TQueryOptions options) {
    TQueryCtx queryCtx = new TQueryCtx();
    queryCtx.setClient_request(new TClientRequest("FeTests", options));
    queryCtx.setQuery_id(new TUniqueId());
    queryCtx.setSession(new TSessionState(new TUniqueId(), TSessionType.BEESWAX,
        defaultDb, user, new TNetworkAddress("localhost", 0)));
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    Date now = Calendar.getInstance().getTime();
    queryCtx.setNow_string(formatter.format(now));
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    queryCtx.setUtc_timestamp_string(formatter.format(now));
    queryCtx.setLocal_time_zone("UTC");
    queryCtx.setStart_unix_millis(System.currentTimeMillis());
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

  /**
   * Returns the hive major version from environment
   */
  public static int getHiveMajorVersion() {
    String hiveMajorVersion = Preconditions.checkNotNull(System.getenv(
        "IMPALA_HIVE_MAJOR_VERSION"));
    return Integer.parseInt(hiveMajorVersion);
  }

  /**
   * Gets checks if the catalog server running on the given host and port has
   * catalog-v2 enabled
   * @return
   * @throws IOException
   */
  public static boolean isCatalogV2Enabled(String host, int port) throws IOException {
    Preconditions.checkNotNull(host);
    Preconditions.checkState(port >= 0);
    String topicMode = getConfigValue(new URL(String.format("http://%s:%s"
            + "/varz?json", host, port)), "catalog_topic_mode");
    Preconditions.checkNotNull(topicMode);
    return topicMode.equals("minimal");
  }

  /**
   * Gets a flag value from the given URL. Useful to scrubbing the catalog/coordinator
   * varz json output to look for interesting configurations
   */
  private static String getConfigValue(URL url, String key) throws IOException {
    Map<Object, Object> map = new ObjectMapper().readValue(url, Map.class);
    if (map.containsKey("flags")) {
      Preconditions.checkState(map.containsKey("flags"));
      ArrayList<LinkedHashMap<String, String>> flags =
          (ArrayList<LinkedHashMap<String, String>>) map.get("flags");
      for (LinkedHashMap<String, String> flag : flags) {
        if (flag.getOrDefault("name", "").equals(key)) return flag.get("current");
      }
    }
    return null;
  }

  /**
   * Returns a random alphanumeric string of given length
   */
  public static String getRandomString(int size) {
    return RandomStringUtils.randomAlphanumeric(size);
  }
}
