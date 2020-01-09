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
package org.apache.impala.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.hadoop.HoodieROTablePathFilter;

import java.util.ArrayList;
import java.util.List;

public class HudiUtil {
  /**
   * This method will remove invalid FileStatus from the list based on Hudi's timestamp,
   * and return a list of file status contains only the latest version parquet files.
   * e.g. Input: [ABC_20200101.parquet, ABC_20200102.parquet, DEF_20200101.parquet]
   * Return: [ABC_20200102.parquet, DEF_20200101.parquet]
   */
  public static List<FileStatus> filterFilesForHudiROPath(List<FileStatus> stats) {
    List<FileStatus> validStats = new ArrayList<>(stats);
    HoodieROTablePathFilter hudiFilter = new HoodieROTablePathFilter();
    validStats.removeIf(f -> !hudiFilter.accept(f.getPath()));
    return validStats;
  }
}
