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


#ifndef IMPALA_UTIL_TABLE_PRINTER_H
#define IMPALA_UTIL_TABLE_PRINTER_H

#include <boost/uuid/uuid.hpp>

#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace impala {

/// Utility class to pretty print tables. Rows can be added and at the end
/// printed with fixed with spacing.
class TablePrinter {
 public:
  TablePrinter();

  /// Add a column to the table. All calls to AddColumn() must come before any calls to
  /// AddRow().
  void AddColumn(const std::string& label, bool left_align);

  /// Sets the max per column output width (otherwise, columns will be as wide as the
  /// largest value). Values longer than this will be cut off.
  void set_max_output_width(int width);

  /// Add a row to the table. This must have the same width as labels.
  void AddRow(const std::vector<std::string>& row);

  /// Print to a table with prefix coming before the output.
  std::string ToString(const std::string& prefix = "") const;

 private:
  std::vector<std::string> labels_;
  /// For each column, true if the value should be left aligned, right aligned otherwise.
  std::vector<bool> left_align_;

  /// -1 to indicate unlimited.
  int max_output_width_;

  std::vector<std::vector<std::string>> rows_;
  std::vector<int> max_col_widths_;

  /// Helper function to print one row to ss.
  void PrintRow(std::stringstream* ss, const std::vector<std::string>& row,
      const std::vector<int>& widths) const;
};

}

#endif
