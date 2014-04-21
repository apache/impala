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

import static com.cloudera.impala.common.ByteUnits.GIGABYTE;
import static com.cloudera.impala.common.ByteUnits.KILOBYTE;
import static com.cloudera.impala.common.ByteUnits.MEGABYTE;
import static com.cloudera.impala.common.ByteUnits.PETABYTE;
import static com.cloudera.impala.common.ByteUnits.TERABYTE;

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
    if (bytes >= PETABYTE) return String.format("%.2f", result / PETABYTE) + "PB";
    if (bytes >= TERABYTE) return String.format("%.2f", result / TERABYTE) + "TB";
    if (bytes >= GIGABYTE) return String.format("%.2f", result / GIGABYTE) + "GB";
    if (bytes >= MEGABYTE) return String.format("%.2f", result / MEGABYTE) + "MB";
    if (bytes >= KILOBYTE) return String.format("%.2f", result / KILOBYTE) + "KB";
    return bytes + "B";
  }

  public static String printCardinality(String prefix, long cardinality) {
    return prefix + "cardinality=" +
        ((cardinality != -1) ? String.valueOf(cardinality) : "unavailable");
  }

  public static String printHosts(String prefix, long numHosts) {
    return prefix + "hosts=" + ((numHosts != -1) ? numHosts : "unavailable");
  }

  public static String printMemCost(String prefix, long perHostMemCost) {
    return prefix + "per-host-mem=" +
        ((perHostMemCost != -1) ? printBytes(perHostMemCost) : "unavailable");
  }

  /**
   * Prints the given square matrix into matrixStr. Separates cells by cellSpacing.
   */
  public static void printMatrix(boolean[][] matrix, int cellSpacing,
      StringBuilder matrixStr) {
    // Print labels.
    for (int i = 0; i < cellSpacing; ++i) {
      matrixStr.append(" ");
    }
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
}
