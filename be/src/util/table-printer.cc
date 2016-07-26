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

#include "util/table-printer.h"
#include "common/logging.h"

#include <iomanip>
#include <sstream>

#include "common/names.h"

using namespace impala;

// Number of spaces to pad adjacent columns.
const int COLUMN_PAD = 2;

TablePrinter::TablePrinter() : max_output_width_(-1) {
}

void TablePrinter::AddColumn(const string& label, bool left_align) {
  labels_.push_back(label);
  left_align_.push_back(left_align);
  max_col_widths_.push_back(label.size());
}

void TablePrinter::set_max_output_width(int width) {
  max_output_width_ = width;
}

// Add a row to the table. This must have the same width as labels.
void TablePrinter::AddRow(const vector<string>& row) {
  DCHECK_EQ(row.size(), labels_.size());
  rows_.push_back(row);
  for (int i = 0; i < row.size(); ++i) {
    if (row[i].size() > max_col_widths_[i]) max_col_widths_[i] = row[i].size();
  }
}

void TablePrinter::PrintRow(stringstream* s, const vector<string>& row,
      const vector<int>& widths) const {
  DCHECK_EQ(row.size(), labels_.size());
  stringstream& ss = *s;
  for (int i = 0; i < row.size(); ++i) {
    if (left_align_[i]) {
      ss << std::left;
    } else {
      ss << std::right;
    }
    ss << setw(widths[i]);
    stringstream tmp;
    if (i != 0) tmp << " ";
    if (row[i].size() > widths[i] - COLUMN_PAD) {
      tmp << string(row[i].data(), widths[i] - COLUMN_PAD - 3) << "...";
    } else {
      tmp << row[i];
    }
    if (i != row.size() - 1) tmp << " ";
    ss << tmp.str();
  }
}

string TablePrinter::ToString(const string& prefix) const {
  vector<int> output_widths = max_col_widths_;
  int total_width = 0;
  for (int i = 0; i < output_widths.size(); ++i) {
    if (max_output_width_ >= 0) {
      output_widths[i] = std::min(output_widths[i], max_output_width_);
    }
    output_widths[i] += COLUMN_PAD;
    total_width += output_widths[i];
  }

  stringstream ss;
  ss << prefix;

  // Print the labels and line after.
  PrintRow(&ss, labels_, output_widths);
  ss << endl;

  // Print line after labels
  for (int i = 0; i < total_width; ++i) {
    ss << "-";
  }
  ss << endl;

  // Print the rows
  for (int i = 0; i < rows_.size(); ++i) {
    PrintRow(&ss, rows_[i], output_widths);
    if (i != rows_.size() - 1) ss << endl;
  }

  return ss.str();
}
