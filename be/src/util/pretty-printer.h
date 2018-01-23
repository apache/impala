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

#ifndef IMPALA_UTIL_PRETTY_PRINTER_H
#define IMPALA_UTIL_PRETTY_PRINTER_H

#include <boost/algorithm/string.hpp>
#include <cmath>
#include <sstream>
#include <iomanip>

#include "gen-cpp/RuntimeProfile_types.h"
#include "util/cpu-info.h"
#include "util/template-util.h"

/// Truncate a double to offset decimal places.
#define DOUBLE_TRUNCATE(val, offset) floor(val * pow(10, offset)) / pow(10, offset)

namespace impala {

/// Methods for printing numeric values with optional units, or other types with an
/// applicable operator<<.
class PrettyPrinter {
 public:
  static std::string Print(bool value, TUnit::type ignored, bool verbose = false) {
    std::stringstream ss;
    ss << std::boolalpha << value;
    return ss.str();
  }

  /// Prints the 'value' in a human friendly format depending on the data type.
  /// i.e. for bytes: 3145728 -> 3MB
  /// If verbose is true, this also prints the raw value (before unit conversion) for
  /// types where this is applicable.
  template<typename T>
  static ENABLE_IF_ARITHMETIC(T, std::string)
  Print(T value, TUnit::type unit, bool verbose = false) {
    std::stringstream ss;
    ss.flags(std::ios::fixed);
    switch (unit) {
      case TUnit::NONE: {
        ss << value;
        return ss.str();
      }

      case TUnit::UNIT: {
        std::string unit;
        double output = GetUnit(value, &unit);
        if (unit.empty()) {
          ss << value;
        } else {
          ss << std::setprecision(PRECISION) << output << unit;
        }
        if (verbose) ss << " (" << value << ")";
        break;
      }

      case TUnit::UNIT_PER_SECOND: {
        std::string unit;
        double output = GetUnit(value, &unit);
        if (output == 0) {
          ss << "0";
        } else {
          ss << std::setprecision(PRECISION) << output << " " << unit << "/sec";
        }
        break;
      }

      case TUnit::CPU_TICKS: {
        if (value < CpuInfo::cycles_per_ms()) {
          ss << std::setprecision(PRECISION) << (value / 1000.) << "K clock cycles";
        } else {
          value /= CpuInfo::cycles_per_ms();
          PrintTimeMs(value, &ss);
        }
        break;
      }

      case TUnit::TIME_NS: {
        PrintTimeNs(value, &ss);
        break;
      }

      case TUnit::TIME_US: {
        PrintTimeNs(value * THOUSAND, &ss);
        break;
      }

      case TUnit::TIME_MS: {
        PrintTimeMs(value, &ss);
        break;
      }

      case TUnit::TIME_S: {
        PrintTimeMs(value * 1000, &ss);
        break;
      }

      case TUnit::BYTES: {
        std::string unit;
        double output = GetByteUnit(value, &unit);
        if (output == 0) {
          ss << "0";
        } else {
          ss << std::setprecision(PRECISION) << output << " " << unit;
          if (verbose) ss << " (" << value << ")";
        }
        break;
      }

      case TUnit::BYTES_PER_SECOND: {
        std::string unit;
        double output = GetByteUnit(value, &unit);
        ss << std::setprecision(PRECISION) << output << " " << unit << "/sec";
        break;
      }

      /// TODO: Remove DOUBLE_VALUE. IMPALA-1649
      case TUnit::DOUBLE_VALUE: {
        double output = *reinterpret_cast<double*>(&value);
        ss << std::setprecision(PRECISION) << output << " ";
        break;
      }

      default:
        DCHECK(false) << "Unsupported TUnit: " << value;
        break;
    }
    return ss.str();
  }

  /// For non-arithmetics, just write the value as a string and return it.
  //
  /// TODO: There's no good is_string equivalent, so there's a needless copy for strings
  /// here.
  template<typename T>
  static ENABLE_IF_NOT_ARITHMETIC(T, std::string)
  Print(const T& value, TUnit::type unit) {
    std::stringstream ss;
    ss << std::boolalpha << value;
    return ss.str();
  }

  /// Utility method to print an iterable type to a stringstream like [v1, v2, v3]
  template <typename I>
  static void PrintStringList(const I& iterable, TUnit::type unit,
      std::stringstream* out) {
    std::vector<std::string> strings;
    for (typename I::const_iterator it = iterable.begin(); it != iterable.end(); ++it) {
      std::stringstream ss;
      ss << PrettyPrinter::Print(*it, unit);
      strings.push_back(ss.str());
    }

    (*out) <<"[" << boost::algorithm::join(strings, ", ") << "]";
  }

  /// Convenience method
  static std::string PrintBytes(int64_t value) {
    return PrettyPrinter::Print(value, TUnit::BYTES);
  }

 private:
  static const int PRECISION = 2;
  static const int TIME_NS_PRECISION = 3;
  static const int64_t KILOBYTE = 1024;
  static const int64_t MEGABYTE = KILOBYTE * 1024;
  static const int64_t GIGABYTE = MEGABYTE * 1024;

  static const int64_t SECOND = 1000;
  static const int64_t MINUTE = SECOND * 60;
  static const int64_t HOUR = MINUTE * 60;

  static const int64_t THOUSAND = 1000;
  static const int64_t MILLION = THOUSAND * 1000;
  static const int64_t BILLION = MILLION * 1000;

  template <typename T>
  static double GetByteUnit(T value, std::string* unit) {
    if (value == 0) {
      *unit = "";
      return value;
    } else if (value >= GIGABYTE || (value < 0 && value <= -GIGABYTE)) {
      *unit = "GB";
      return value / (double) GIGABYTE;
    } else if (value >= MEGABYTE || (value < 0 && value <= -MEGABYTE)) {
      *unit = "MB";
      return value / (double) MEGABYTE;
    } else if (value >= KILOBYTE || (value < 0 && value <= -KILOBYTE)) {
      *unit = "KB";
      return value / (double) KILOBYTE;
    } else {
      *unit = "B";
      return value;
    }
  }

  template <typename T>
  static double GetUnit(T value, std::string* unit) {
    if (value >= BILLION) {
      *unit = "B";
      return value / (1. * BILLION);
    } else if (value >= MILLION) {
      *unit = "M";
      return value / (1. * MILLION);
    } else if (value >= THOUSAND) {
      *unit = "K";
      return value / (1. * THOUSAND);
    } else {
      *unit = "";
      return value;
    }
  }

  /// Utility to perform integer modulo if T is integral, otherwise to use fmod().
  template <typename T>
  static ENABLE_IF_INTEGRAL(T, int64_t) Mod(const T& value, const int modulus) {
    return value % modulus;
  }

  template <typename T>
  static ENABLE_IF_FLOAT(T, double) Mod(const T& value, int modulus) {
    return fmod(value, 1. * modulus);
  }

  /// Pretty print the value (time in ns) to ss.
  template <typename T>
  static void PrintTimeNs(T value, std::stringstream* ss) {
    *ss << std::setprecision(TIME_NS_PRECISION);
    if (value >= BILLION) {
      /// If the time is over a second, print it up to ms.
      value /= MILLION;
      PrintTimeMs(value, ss);
    } else if (value >= MILLION) {
      /// if the time is over a ms, print it up to microsecond in the unit of ms.
      *ss << DOUBLE_TRUNCATE(static_cast<double>(value) / MILLION, TIME_NS_PRECISION)
        << "ms";
    } else if (value > THOUSAND) {
      /// if the time is over a microsecond, print it using unit microsecond.
      *ss << DOUBLE_TRUNCATE(static_cast<double>(value) / THOUSAND, TIME_NS_PRECISION)
        << "us";
    } else {
      *ss << DOUBLE_TRUNCATE(value, TIME_NS_PRECISION) << "ns";
    }
  }

  /// Print the value (time in ms) to ss.
  template <typename T>
  static void PrintTimeMs(T value, std::stringstream* ss) {
    DCHECK_GE(value, static_cast<T>(0));
    if (value == 0) {
      *ss << "0";
    } else {
      bool hour = false;
      bool minute = false;
      bool second = false;
      if (value >= HOUR) {
        *ss << static_cast<int64_t>(value / HOUR) << "h";
        value = Mod(value, HOUR);
        hour = true;
      }
      if (value >= MINUTE) {
        *ss << static_cast<int64_t>(value / MINUTE) << "m";
        value = Mod(value, MINUTE);
        minute = true;
      }
      if (!hour && value >= SECOND) {
        *ss << static_cast<int64_t>(value / SECOND) << "s";
        value = Mod(value, SECOND);
        second = true;
      }
      if (!hour && !minute) {
        if (second) *ss << std::setw(3) << std::setfill('0');
        *ss << static_cast<int64_t>(value) << "ms";
      }
    }
  }
};

}

#endif
