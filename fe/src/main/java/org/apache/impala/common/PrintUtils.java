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

package org.apache.impala.common;

import static org.apache.impala.common.ByteUnits.GIGABYTE;
import static org.apache.impala.common.ByteUnits.KILOBYTE;
import static org.apache.impala.common.ByteUnits.MEGABYTE;
import static org.apache.impala.common.ByteUnits.PETABYTE;
import static org.apache.impala.common.ByteUnits.TERABYTE;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import com.google.common.base.Joiner;

/**
 * Utility functions for pretty printing.
 */
public class PrintUtils {
  /**
   * Prints the given number of bytes in PB, TB, GB, MB, KB with 2 decimal points.
   * For example 5000 will be returned as 4.88KB.
   */
  public static String printBytes(long bytes) {
    double result = bytes;
    // Avoid String.format() due to IMPALA-1572 which happens on JDK7 but not JDK6.
    // IMPALA-6759: Please update tests/stress/concurrent_select.py MEM_ESTIMATE_PATTERN
    // if you add additional unit prefixes.
    if (bytes >= PETABYTE) return new DecimalFormat(".00PB").format(result / PETABYTE);
    if (bytes >= TERABYTE) return new DecimalFormat(".00TB").format(result / TERABYTE);
    if (bytes >= GIGABYTE) return new DecimalFormat(".00GB").format(result / GIGABYTE);
    if (bytes >= MEGABYTE) return new DecimalFormat(".00MB").format(result / MEGABYTE);
    if (bytes >= KILOBYTE) return new DecimalFormat(".00KB").format(result / KILOBYTE);
    return bytes + "B";
  }

  public static final long KILO = 1000;
  public static final long MEGA = KILO * 1000;
  public static final long GIGA = MEGA * 1000;
  public static final long TERA = GIGA * 1000;

  /**
   * Print a value using simple metric (power of 1000) units. Units are
   * (none), K, M, G or T. Value has two digits past the decimal point.
   */
  public static String printMetric(long value) {
    double result = value;
    if (value >= TERA) return new DecimalFormat(".00T").format(result / TERA);
    if (value >= GIGA) return new DecimalFormat(".00G").format(result / GIGA);
    if (value >= MEGA) return new DecimalFormat(".00M").format(result / MEGA);
    if (value >= KILO) return new DecimalFormat(".00K").format(result / KILO);
    return Long.toString(value);
  }

  /**
   * Pattern to use when searching for a metric-encoded value.
   */
  public static final String METRIC_REGEX = "(\\d+(?:.\\d+)?)([TGMK]?)";

  /**
   * Pattern to use when searching for or parsing a metric-encoded value.
   */
  public static final Pattern METRIC_PATTERN =
      Pattern.compile(METRIC_REGEX, Pattern.CASE_INSENSITIVE);

  /**
   * Decode a value metric-encoded using {@link #printMetric(long)}.
   * @param value metric-encoded string
   * @return approximate numeric value, or -1 if the value is invalid
   * (metric encoded strings can never be negative normally)
   */
  public static double decodeMetric(String value) {
    Matcher m = METRIC_PATTERN.matcher(value);
    if (! m.matches()) return -1;
    return decodeMetric(m.group(1), m.group(2));
  }

  /**
   * Decode a metric-encoded string already parsed into parts.
   * @param valueStr numeric part of the value
   * @param units units part of the value
   * @return approximate numeric value
   */
  // Yes, "PrintUtils" is an odd place for a parse function, but
  // best to keep the formatter and parser together.
  public static double decodeMetric(String valueStr, String units) {
    double value = Double.parseDouble(valueStr);
    switch (units.toUpperCase()) {
    case "":
      return value;
    case "K":
      return value * KILO;
    case "M":
      return value * MEGA;
    case "G":
      return value * GIGA;
    case "T":
      return value * TERA;
    default:
      return -1;
    }
  }

  /**
   * Same as printBytes() except 0 decimal points are shown for MB and KB.
   */
  public static String printBytesRoundedToMb(long bytes) {
    double result = bytes;
    // Avoid String.format() due to IMPALA-1572 which happens on JDK7 but not JDK6.
    // IMPALA-6759: Please update tests/stress/concurrent_select.py MEM_ESTIMATE_PATTERN
    // if you add additional unit prefixes.
    if (bytes >= PETABYTE) return new DecimalFormat(".00PB").format(result / PETABYTE);
    if (bytes >= TERABYTE) return new DecimalFormat(".00TB").format(result / TERABYTE);
    if (bytes >= GIGABYTE) return new DecimalFormat(".00GB").format(result / GIGABYTE);
    if (bytes >= MEGABYTE) return new DecimalFormat("0MB").format(result / MEGABYTE);
    if (bytes >= KILOBYTE) return new DecimalFormat("0KB").format(result / KILOBYTE);
    return bytes + "B";
  }

  /**
   * Print an estimated cardinality. No need to print the exact value
   * because estimates are not super-precise.
   */
  public static String printEstCardinality(long cardinality) {
    return (cardinality != -1) ? printMetric(cardinality) : "unavailable";
  }

  /**
   * Print an exact cardinality (such as a row count) as one of three formats:
   *
   * * "unavailable" (if value < 0)
   * * number (if value is small)
   * * xx.xxU (dd,ddd) (if the value is large)
   */
  public static String printExactCardinality(long value) {
    if (value == -1) return "unavailable";
    String result = printMetric(value);
    if (value < KILO) return result;
    return String.format("%s (%,d)", result, value);
  }

  public static String printNumHosts(String prefix, long numHosts) {
    return prefix + "hosts=" + ((numHosts != -1) ? numHosts : "unavailable");
  }

  public static String printNumInstances(String prefix, long numInstances) {
    return prefix + "instances=" + ((numInstances != -1) ? numInstances : "unavailable");
  }

  /**
   * Prints the given square matrix into matrixStr. Separates cells by cellSpacing.
   */
  public static void printMatrix(
      boolean[][] matrix, int cellSpacing, StringBuilder matrixStr) {
    // Print labels.
    matrixStr.append(StringUtils.repeat(' ', cellSpacing));
    String formatStr = "%Xd".replace("X", String.valueOf(cellSpacing));
    for (int i = 0; i < matrix.length; ++i) {
      matrixStr.append(String.format(formatStr, i));
    }
    matrixStr.append("\n");

    // Print matrix.
    for (int i = 0; i < matrix.length; ++i) {
      matrixStr.append(String.format(formatStr, i));
      for (int j = 0; j < matrix.length; ++j) {
        int cell = (matrix[i][j]) ? 1 : 0;
        matrixStr.append(String.format(formatStr, cell));
      }
      matrixStr.append("\n");
    }
  }

  /**
   * Wrap a string by inserting newlines so that no line exceeds a given length.
   * Any newlines in the input are maintained.
   */
  public static String wrapString(String s, int wrapLength) {
    String wrapped = WordUtils.wrap(s, wrapLength, null, true);
    // Remove any trailing blanks from a line.
    wrapped = wrapped.replaceAll(" +$", "");
    return wrapped;
  }

  /**
   * Join the objects in 'objects' in a human-readable form, such as
   * "'obj 1', 'obj 2', 'obj 3'". No escaping of quotes is performed.
   */
  public static String joinQuoted(Iterable<?> objs) {
    Iterator<?> it = objs.iterator();
    if (!it.hasNext()) return "";
    return "'" + Joiner.on("', '").join(it) + "'";
  }

  public static String printTimeNs(long value) {
    if (value < 0)
      return "";
    if (value >= 1000000000) {
      return printTimeMs(value/1000000);
    } else if (value >= 1000000) {
      return String.format("%.3fms", (double)value/1000000);
    } else if (value >= 1000) {
      return String.format("%.3fus", (double)value/1000);
    } else {
      return value + "ns";
    }
  }

  public static String printTimeMs(long value) {
    if (value < 0) {
      return "";
    }
    boolean hour = false, minute = false, second = false;
    StringBuilder sb = new StringBuilder();
    if (value >= 3600000) {
      sb.append(value / 3600000).append("h");
      value %= 3600000;
      hour = true;
    }
    if (value >= 60000) {
      sb.append(value / 60000).append("m");
      value %= 60000;
      minute = true;
    }
    if (!hour && value >= 1000) {
      sb.append(value / 1000).append("s");
      value %= 1000;
      second = true;
    }
    if (!hour && !minute && value != 0) {
      String remainTime = String.valueOf(value);
      if (second) {
        while (remainTime.length() < 3) {
          remainTime = "0" + remainTime;
        }
      }
      sb.append(remainTime).append("ms");
    }
    return sb.toString();
  }
}
