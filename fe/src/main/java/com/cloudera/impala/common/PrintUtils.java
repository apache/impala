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

import java.text.DecimalFormat;

import org.apache.commons.lang3.StringUtils;

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
    if (bytes >= PETABYTE) return new DecimalFormat(".00PB").format(result / PETABYTE);
    if (bytes >= TERABYTE) return new DecimalFormat(".00TB").format(result / TERABYTE);
    if (bytes >= GIGABYTE) return new DecimalFormat(".00GB").format(result / GIGABYTE);
    if (bytes >= MEGABYTE) return new DecimalFormat(".00MB").format(result / MEGABYTE);
    if (bytes >= KILOBYTE) return new DecimalFormat(".00KB").format(result / KILOBYTE);
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
}
