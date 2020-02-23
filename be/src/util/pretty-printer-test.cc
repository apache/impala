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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "testutil/gtest-util.h"
#include "util/pretty-printer.h"

#include "common/names.h"

namespace impala {

TEST(PrettyPrinterTest, Unit) {
  // Note no restriction to 2 decimal places with UNIT < 1000.
  EXPECT_EQ(PrettyPrinter::Print(1.2345678f, TUnit::UNIT), "1.234568");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::UNIT), "1");
  EXPECT_EQ(PrettyPrinter::Print("string", TUnit::UNIT), "string");

  EXPECT_EQ(PrettyPrinter::Print(1000, TUnit::UNIT), "1.00K");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000, TUnit::UNIT), "1.00M");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000 * 1000, TUnit::UNIT), "1.00B");
}

TEST(PrettyPrinterTest, UnitPerSecond) {
  EXPECT_EQ(PrettyPrinter::Print(1.2345678f, TUnit::UNIT_PER_SECOND), "1.23 /sec");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::UNIT_PER_SECOND), "1.00 /sec");
  EXPECT_EQ(PrettyPrinter::Print("string", TUnit::UNIT_PER_SECOND), "string");

  EXPECT_EQ(PrettyPrinter::Print(1000, TUnit::UNIT_PER_SECOND), "1.00 K/sec");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000, TUnit::UNIT_PER_SECOND),
      "1.00 M/sec");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000 * 1000, TUnit::UNIT_PER_SECOND),
      "1.00 B/sec");
}

TEST(PrettyPrinterTest, CpuTicks) {
  EXPECT_EQ(PrettyPrinter::Print(1000, TUnit::CPU_TICKS), "1.00K clock cycles");
  EXPECT_EQ(PrettyPrinter::Print(1500, TUnit::CPU_TICKS), "1.50K clock cycles");
  EXPECT_EQ(PrettyPrinter::Print(1000.1, TUnit::CPU_TICKS), "1.00K clock cycles");
}

TEST(PrettyPrinterTest, Bytes) {
  EXPECT_EQ(PrettyPrinter::Print(1.2345678f, TUnit::BYTES), "1.23 B");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::BYTES), "1.00 B");

  EXPECT_EQ(PrettyPrinter::Print(1024, TUnit::BYTES), "1.00 KB");
  EXPECT_EQ(PrettyPrinter::Print(1024 * 1024, TUnit::BYTES), "1.00 MB");
  EXPECT_EQ(PrettyPrinter::Print(1024 * 1024 * 1024, TUnit::BYTES), "1.00 GB");

  // Negative values
  EXPECT_EQ(PrettyPrinter::Print(-2, TUnit::BYTES), "-2.00 B");
  EXPECT_EQ(PrettyPrinter::Print(-1024, TUnit::BYTES), "-1.00 KB");
  EXPECT_EQ(PrettyPrinter::Print(-1024 * 1024, TUnit::BYTES), "-1.00 MB");
  EXPECT_EQ(PrettyPrinter::Print(-1024 * 1024 * 1024, TUnit::BYTES), "-1.00 GB");

  EXPECT_EQ(PrettyPrinter::Print(1536, TUnit::BYTES), "1.50 KB");
  EXPECT_EQ(PrettyPrinter::Print(1536.0, TUnit::BYTES), "1.50 KB");

  // Units are ignored for strings
  EXPECT_EQ(PrettyPrinter::Print("string", TUnit::BYTES), "string");
}

TEST(PrettyPrinterTest, BytesPerSecond) {
  EXPECT_EQ(PrettyPrinter::Print(1.2345678f, TUnit::BYTES_PER_SECOND),
      "1.23 B/sec");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::BYTES_PER_SECOND), "1.00 B/sec");

  EXPECT_EQ(PrettyPrinter::Print(1024, TUnit::BYTES_PER_SECOND), "1.00 KB/sec");
  EXPECT_EQ(PrettyPrinter::Print(1024 * 1024, TUnit::BYTES_PER_SECOND),
      "1.00 MB/sec");
  EXPECT_EQ(PrettyPrinter::Print(1024 * 1024 * 1024, TUnit::BYTES_PER_SECOND),
      "1.00 GB/sec");

  EXPECT_EQ(PrettyPrinter::Print(1536, TUnit::BYTES_PER_SECOND), "1.50 KB/sec");
  EXPECT_EQ(PrettyPrinter::Print(1536.0, TUnit::BYTES_PER_SECOND), "1.50 KB/sec");
}

TEST(PrettyPrinterTest, Seconds) {
  EXPECT_EQ(PrettyPrinter::Print(0.00001, TUnit::TIME_S), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(0.0001, TUnit::TIME_S), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(0.021, TUnit::TIME_S), "21ms");
  EXPECT_EQ(PrettyPrinter::Print(0.31, TUnit::TIME_S), "310ms");
  EXPECT_EQ(PrettyPrinter::Print(0.1, TUnit::TIME_S), "100ms");
  EXPECT_EQ(PrettyPrinter::Print(1.5, TUnit::TIME_S), "1s500ms");

  EXPECT_EQ(PrettyPrinter::Print(60, TUnit::TIME_S), "1m");
  EXPECT_EQ(PrettyPrinter::Print(60 * 60, TUnit::TIME_S), "1h");
  EXPECT_EQ(PrettyPrinter::Print(60 * 60 * 24, TUnit::TIME_S), "24h");

  EXPECT_EQ(PrettyPrinter::Print(1000, TUnit::TIME_S), "16m40s");

  // Only three decimal places of precision are supported for seconds.
  EXPECT_EQ(PrettyPrinter::Print(1.5009, TUnit::TIME_S), "1s500ms");

  EXPECT_EQ(PrettyPrinter::Print(12345, TUnit::TIME_S), "3h25m");
  EXPECT_EQ(PrettyPrinter::Print(1.23456789123, TUnit::TIME_S), "1s234ms");
  EXPECT_EQ(PrettyPrinter::Print(12345.67, TUnit::TIME_S), "3h25m");
  EXPECT_EQ(PrettyPrinter::Print(1234567, TUnit::TIME_S), "342h56m");
  EXPECT_EQ(PrettyPrinter::Print(1234567.8989, TUnit::TIME_S), "342h56m");
  EXPECT_EQ(PrettyPrinter::Print(123456789, TUnit::TIME_S), "34293h33m");
  EXPECT_EQ(PrettyPrinter::Print(2147483647, TUnit::TIME_S), "596523h14m");
  EXPECT_EQ(PrettyPrinter::Print(123823456789, TUnit::TIME_S), "34395404h39m");
}

TEST(PrettyPrinterTest, NanoSeconds) {
  EXPECT_EQ(PrettyPrinter::Print(0.00001, TUnit::TIME_NS), "0.000ns");
  EXPECT_EQ(PrettyPrinter::Print(0.0001, TUnit::TIME_NS), "0.000ns");
  EXPECT_EQ(PrettyPrinter::Print(0.021, TUnit::TIME_NS), "0.021ns");
  EXPECT_EQ(PrettyPrinter::Print(0.31, TUnit::TIME_NS), "0.310ns");
  EXPECT_EQ(PrettyPrinter::Print(0.1, TUnit::TIME_NS), "0.100ns");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::TIME_NS), "1.000ns");
  EXPECT_EQ(PrettyPrinter::Print(123.4, TUnit::TIME_NS), "123.400ns");
  EXPECT_EQ(PrettyPrinter::Print(1234.9, TUnit::TIME_NS), "1.234us");
  EXPECT_EQ(PrettyPrinter::Print(12345.6789, TUnit::TIME_NS), "12.345us");
  EXPECT_EQ(PrettyPrinter::Print(1234567.890, TUnit::TIME_NS), "1.234ms");
  EXPECT_EQ(PrettyPrinter::Print(123456789.0, TUnit::TIME_NS), "123.456ms");
  EXPECT_EQ(PrettyPrinter::Print(123823456789.9, TUnit::TIME_NS), "2m3s");
  EXPECT_EQ(PrettyPrinter::Print(12123456789.9, TUnit::TIME_NS), "12s123ms");
  EXPECT_EQ(PrettyPrinter::Print(1.5009, TUnit::TIME_NS), "1.500ns");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000 * 1000, TUnit::TIME_NS), "1s000ms");
  EXPECT_EQ(PrettyPrinter::Print(123, TUnit::TIME_NS), "123.000ns");
  EXPECT_EQ(PrettyPrinter::Print(123400, TUnit::TIME_NS), "123.400us");
  EXPECT_EQ(PrettyPrinter::Print(123456789, TUnit::TIME_NS), "123.456ms");
  EXPECT_EQ(PrettyPrinter::Print(123456789, TUnit::TIME_NS), "123.456ms");
}

TEST(PrettyPrinterTest, MillisSeconds) {
  EXPECT_EQ(PrettyPrinter::Print(0.00001, TUnit::TIME_MS), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(0.0001, TUnit::TIME_MS), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(0.021, TUnit::TIME_MS), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(0.31, TUnit::TIME_MS), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(0.1, TUnit::TIME_MS), "0ms");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::TIME_MS), "1ms");
  EXPECT_EQ(PrettyPrinter::Print(123.4, TUnit::TIME_MS), "123ms");
  EXPECT_EQ(PrettyPrinter::Print(1234.9, TUnit::TIME_MS), "1s234ms");
  EXPECT_EQ(PrettyPrinter::Print(12345.6789, TUnit::TIME_MS), "12s345ms");
  EXPECT_EQ(PrettyPrinter::Print(1234567.890, TUnit::TIME_MS), "20m34s");
  EXPECT_EQ(PrettyPrinter::Print(123456789.0, TUnit::TIME_MS), "34h17m");
  EXPECT_EQ(PrettyPrinter::Print(123823456789.9, TUnit::TIME_MS), "34395h24m");
  EXPECT_EQ(PrettyPrinter::Print(12123456789.9, TUnit::TIME_MS), "3367h37m");
  EXPECT_EQ(PrettyPrinter::Print(1.5009, TUnit::TIME_MS), "1ms");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000 * 1000, TUnit::TIME_MS), "277h46m");
  EXPECT_EQ(PrettyPrinter::Print(123, TUnit::TIME_MS), "123ms");
  EXPECT_EQ(PrettyPrinter::Print(123400, TUnit::TIME_MS), "2m3s");
  EXPECT_EQ(PrettyPrinter::Print(123456789, TUnit::TIME_MS), "34h17m");
  EXPECT_EQ(PrettyPrinter::Print(2147483647, TUnit::TIME_MS), "596h31m");
}

TEST(PrettyPrinterTest, MicroSeconds) {
  EXPECT_EQ(PrettyPrinter::Print(0.00001, TUnit::TIME_US), "0.010ns");
  EXPECT_EQ(PrettyPrinter::Print(0.0001, TUnit::TIME_US), "0.100ns");
  EXPECT_EQ(PrettyPrinter::Print(0.021, TUnit::TIME_US), "21.000ns");
  EXPECT_EQ(PrettyPrinter::Print(0.31, TUnit::TIME_US), "310.000ns");
  EXPECT_EQ(PrettyPrinter::Print(0.1, TUnit::TIME_US), "100.000ns");
  EXPECT_EQ(PrettyPrinter::Print(1, TUnit::TIME_US), "1000.000ns");
  EXPECT_EQ(PrettyPrinter::Print(123.4, TUnit::TIME_US), "123.400us");
  EXPECT_EQ(PrettyPrinter::Print(1234.9, TUnit::TIME_US), "1.234ms");
  EXPECT_EQ(PrettyPrinter::Print(12345.6789, TUnit::TIME_US), "12.345ms");
  EXPECT_EQ(PrettyPrinter::Print(1234567.890, TUnit::TIME_US), "1s234ms");
  EXPECT_EQ(PrettyPrinter::Print(123456789.0, TUnit::TIME_US), "2m3s");
  EXPECT_EQ(PrettyPrinter::Print(123823456789.9, TUnit::TIME_US), "34h23m");
  EXPECT_EQ(PrettyPrinter::Print(12123456789.9, TUnit::TIME_US), "3h22m");
  EXPECT_EQ(PrettyPrinter::Print(1.5009, TUnit::TIME_US), "1.500us");
  EXPECT_EQ(PrettyPrinter::Print(1000 * 1000 * 1000, TUnit::TIME_US), "16m40s");
  EXPECT_EQ(PrettyPrinter::Print(123, TUnit::TIME_US), "123.000us");
  EXPECT_EQ(PrettyPrinter::Print(123400, TUnit::TIME_US), "123.400ms");
  EXPECT_EQ(PrettyPrinter::Print(123456789, TUnit::TIME_US), "2m3s");
  EXPECT_EQ(PrettyPrinter::Print(2147483647, TUnit::TIME_US), "35m47s");
}

TEST(PrettyPrinterTest, DoubleValue) {
  EXPECT_EQ(PrettyPrinter::Print(1.0, TUnit::DOUBLE_VALUE), "1.00");
}

TEST(PrettyPrinterTest, StringList) {
  vector<int> vals;
  stringstream ss1;
  PrettyPrinter::PrintStringList(vals, TUnit::UNIT, &ss1);
  EXPECT_EQ(ss1.str(), "[]");

  vals.push_back(1);
  vals.push_back(1000);
  vals.push_back(1000 * 1000);
  stringstream ss2;
  PrettyPrinter::PrintStringList(vals, TUnit::UNIT, &ss2);
  EXPECT_EQ(ss2.str(), "[1, 1.00K, 1.00M]");
}

}

