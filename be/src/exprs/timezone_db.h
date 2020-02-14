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

#ifndef IMPALA_EXPRS_TIMEZONE_DB_H
#define IMPALA_EXPRS_TIMEZONE_DB_H

#include <unordered_map>

#include "cctz/time_zone.h"
#include "common/global-types.h"
#include "common/status.h"
#include "util/zip-util.h"

namespace impala {

/// 'TimezoneDatabase' class contains functions to load and access the IANA time-zone
/// database. The IANA time-zone database (often called tz db) contains binary data files
/// that represent the history of local time for many representative locations around the
/// globe. Further information on tz db, including implementation details, is described in
/// the code repository: https://data.iana.org/time-zones/tz-link.html
///
/// The system's default tz db is located in /usr/share/zoneinfo directory. The zoneinfo
/// directory's tree structure follows the IANA time-zone naming scheme, e.g. the
/// /usr/share/zoneinfo/America/Los_Angeles file describes the "America/Los_Angeles"
/// time-zone.
///
/// This class uses the CCTZ library's 'time_zone' class internally to load, store and
/// utilize time-zone data. Further information on CCTZ is described in the code
/// repository: https://github.com/google/cctz
///
/// Initialize() should be called on process startup to load every time-zone file into
/// memory for fast lookups. It loads files from /usr/share/zoneinfo by default.
/// Alternatively, FLAGS_hdfs_zone_info_zip can be used to specify a shared zip file that
/// contains the compiled time-zone db to use.
///
/// Initialize() also defines a hard-coded set of non-standard time-zone aliases to
/// maintain a level of backward compatibility with the previous boost-based
/// implementation. Alternatively, FLAGS_hdfs_zone_alias_conf can be used to specify a
/// shared configuration file to load the non-standard aliases from.
///
/// Once Initialize() returned without error, FindTimezone() can be safely called from
/// multiple threads to look up time-zones by name.

class TimezoneDatabase {
 public:
  /// Set up the static time-zone database.
  static Status Initialize() WARN_UNUSED_RESULT;

  /// Return path to time-zone database.
  static const string& GetPath() { return tz_db_path_; }

  /// Returns name of the local time-zone or empty string if cannot find it.
  static std::string LocalZoneName();

  /// Looks up 'Timezone' object by name. Returns pointer to the 'Timezone' object if the
  /// lookup was successful and nullptr otherwise.
  static const Timezone* FindTimezone(const std::string& tz_name) {
    auto it = tz_name_map_.find(tz_name);
    return (it == tz_name_map_.end()) ? nullptr : it->second.get();
  }

  // Return the Timezone object for UTC.
  // Note that in performance critical parts of the code it is recommended
  // to use UTCPTR instead (which is just a nullptr) to represent UTC, as many
  // TimestampValue functions have optimized path for it.
  static const Timezone& GetUtcTimezone() { return UTC_TIMEZONE_; }

  /// Public proxy for LoadZoneInfo. Should be only used in BE tests.
  static Status LoadZoneInfoBeTestOnly(
      const std::string& zone_info_dir) WARN_UNUSED_RESULT {
    return LoadZoneInfo(zone_info_dir);
  }

 private:
  // For BE tests
  friend class TimezoneDbNamesTest;
  friend class TimezoneDbLoadAliasTest;
  friend class TimezoneDbLoadZoneInfoTest;

  static const std::string ZONE_INFO_DIR;
  static const std::string TIMEZONE_ALIASES;
  static const Timezone UTC_TIMEZONE_;

  /// Type to map time-zone names to Timezone objects.
  typedef std::unordered_map<std::string, std::shared_ptr<Timezone>> TimezoneMap;

  static TimezoneMap tz_name_map_;
  static std::string tz_db_path_;

  /// Returns 'true' if 'tz_segment' is a valid time-zone name segment. Time-zone name
  /// segments can have letters, digits and '_', '-', '+' characters only. Name segments
  /// must begin with an uppercase letter.
  /// Some examples of valid time-zone name segments are: Los_Angeles, GMT+1,
  /// East-Indiana.
  static bool IsTimezoneNameSegmentValid(
      const std::string& tz_segment) WARN_UNUSED_RESULT;

  /// Returns 'true' if 'tz_name' is a valid time-zone name. Time-zone names must be valid
  /// time-zone name segments delimited by '/', e.g.: America/Argentina/San_Juan.
  static bool IsTimezoneNameValid(const std::string& tz_name) WARN_UNUSED_RESULT;

  /// Parses the UTC offset in 'tz_offset' and returns 'true' if it is valid. If a valid
  /// offset was found, 'offset_sec' is set to the parsed value.
  static bool IsTimezoneOffsetValid(const std::string& tz_offset,
      int64_t* offset_sec) WARN_UNUSED_RESULT;

  /// Load 'Timezone' objects into 'tz_name_map_' from the shared 'hdfs_zone_info_zip' zip
  /// archive.
  static Status LoadZoneInfoFromHdfs(const std::string& hdfs_zone_info_zip,
      const std::string& local_dir) WARN_UNUSED_RESULT;

  /// Load 'Timezone' objects into 'tz_name_map_' from 'zone_info_dir' path.
  static Status LoadZoneInfo(const std::string& zone_info_dir) WARN_UNUSED_RESULT;

  /// Recursive function to load 'Timezone' objects into 'tz_path_map' from 'path'.
  /// 'zone_info_dir' is the root directory of the time-zone db.
  static Status LoadZoneInfoHelper(const std::string& path,
      const std::string& zone_info_dir, TimezoneMap& tz_path_map) WARN_UNUSED_RESULT;

  /// Load 'Timezone' object from file 'path'. 'zone_info_dir' is the root directory of
  /// the time-zone db.
  /// - If 'path' is not a symbolic link, load 'Timezone' object from 'path' and add it to
  /// 'tz_path_map' as a value mapped to 'path'.
  /// - If 'path' is a symbolic link to another time-zone file, load 'Timezone' object
  /// from the linked path and add it to 'tz_path_map' as a value mapped both to 'path'
  /// and the linked path.
  static void LoadTimezone(const std::string& path, const std::string& zone_info_dir,
      TimezoneMap& tz_path_map);

  /// Load 'Timezone' object from file 'path'. If successful, return 'shared_ptr' to the
  /// 'Timezone' object and nullptr otherwise.
  static std::shared_ptr<Timezone> LoadTimezoneHelper(
      const std::string& path) WARN_UNUSED_RESULT;

  /// Load custom time-zone aliases from 'hdfs_zone_alias_conf' shared file and add them
  /// to 'tz_name_map_'.
  static Status LoadZoneAliasesFromHdfs(
      const string& hdfs_zone_alias_conf) WARN_UNUSED_RESULT;

  /// Load custom time-zone aliases from 'is' and add them to 'tz_name_map_'.
  static Status LoadZoneAliases(
      std::istream &is, const char* path = nullptr) WARN_UNUSED_RESULT;
};

} // namespace impala

#endif
