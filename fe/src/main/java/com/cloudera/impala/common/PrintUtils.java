// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.common;

/**
 * Utility functions for pretty printing.
 */
public class PrintUtils {
  /**
   * Prints the given number of bytes in PB, TB, GB, MB, KB with 2 decimal points.
   * For example 5000 will be returned as 4.88KB.
   */
  public static String printBytes(long bytes) {
    final long KB = 1024;
    final long MB = KB * 1024;
    final long GB = MB * 1024;
    final long TB = GB * 1024;
    final long PB = TB * 1024;

    double result = bytes;
    if (bytes > PB) return String.format("%.2f", result / PB) + "PB";
    if (bytes > TB) return String.format("%.2f", result / TB) + "TB";
    if (bytes > GB) return String.format("%.2f", result / GB) + "GB";
    if (bytes > MB) return String.format("%.2f", result / MB) + "MB";
    if (bytes > KB) return String.format("%.2f", result / KB) + "KB";
    return bytes + "B";
  }

  public static String printCardinality(String prefix, long cardinality) {
    return prefix + "cardinality: " +
        ((cardinality != -1) ? String.valueOf(cardinality) : "unavailable");
  }

  public static String printMemCost(String prefix, long perHostMemCost) {
    return prefix + "per-host memory: " +
        ((perHostMemCost != -1) ? printBytes(perHostMemCost) : "unavailable");
  }
}
