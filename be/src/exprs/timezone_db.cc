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

#include "exprs/timezone_db.h"

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <regex>
#include <string>
#include <vector>

#include <boost/algorithm/string/trim.hpp>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "kudu/util/path_util.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/substitute.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/debug-util.h"
#include "util/filesystem-util.h"
#include "util/hdfs-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using std::ios_base;
using std::istream;
using std::regex;
using std::regex_constants::ECMAScript;
using std::regex_match;
using boost::algorithm::trim;
using kudu::JoinPathSegments;

DEFINE_string(hdfs_zone_info_zip, "",
    "HDFS/S3A/ADLS path to a zip archive of the IANA time-zone database to use.");
DEFINE_string(hdfs_zone_alias_conf, "",
    "HDFS/S3A/ADLS path to config file defining non-standard time-zone aliases.");
DECLARE_string(local_library_dir);

namespace impala {

static const int HDFS_READ_SIZE = 64 * 1024; // bytes

const string TimezoneDatabase::ZONE_INFO_DIR = "/usr/share/zoneinfo";
const string TimezoneDatabase::TIMEZONE_ALIASES = \
"#\n"
"# Java supports these non-standard time-zone abbreviations\n"
"#\n"
"ACT = Australia/Darwin\n"
"AET = Australia/Sydney\n"
"AGT = America/Argentina/Buenos_Aires\n"
"ART = Africa/Cairo\n"
"AST = America/Anchorage\n"
"BET = America/Sao_Paulo\n"
"BST = Asia/Dhaka\n"
"CAT = Africa/Harare\n"
"CNT = America/St_Johns\n"
"CST = America/Chicago\n"
"CTT = Asia/Shanghai\n"
"EAT = Africa/Addis_Ababa\n"
"ECT = Europe/Paris\n"
"IET = America/Indiana/Indianapolis\n"
"IST = Asia/Kolkata\n"
"JST = Asia/Tokyo\n"
"MIT = Pacific/Apia\n"
"NET = Asia/Yerevan\n"
"NST = Pacific/Auckland\n"
"PLT = Asia/Karachi\n"
"PNT = America/Phoenix\n"
"PRT = America/Puerto_Rico\n"
"PST = America/Los_Angeles\n"
"SST = Pacific/Guadalcanal\n"
"VST = Asia/Ho_Chi_Minh\n";

const Timezone TimezoneDatabase::UTC_TIMEZONE_ = cctz::utc_time_zone();

TimezoneDatabase::TimezoneMap TimezoneDatabase::tz_name_map_;
string TimezoneDatabase::tz_db_path_;

bool TimezoneDatabase::IsTimezoneNameSegmentValid(const string& tz_segment) {
  static const regex reg("[A-Z][A-Za-z0-9:_+-]*", ECMAScript);
  return regex_match(tz_segment, reg);
}

bool TimezoneDatabase::IsTimezoneNameValid(const string& tz_name) {
  static const regex reg("[A-Z][A-Za-z0-9:_+-]*(/[A-Z][A-Za-z0-9:_+-]*)*", ECMAScript);
  return regex_match(tz_name, reg);
}

bool TimezoneDatabase::IsTimezoneOffsetValid(const string& tz_offset,
    int64_t* offset_sec) {
  if (tz_offset.empty()) return false;
  // The absolute value of the offset_sec cannot be greater than or equal to 24 hours.
  constexpr int64_t max_abs_offset_sec = 24*60*60 - 1;
  StringParser::ParseResult result;
  *offset_sec = StringParser::StringToInt<int64_t>(
      tz_offset.c_str(), tz_offset.length(), &result);
  return result == StringParser::PARSE_SUCCESS &&
      *offset_sec <= max_abs_offset_sec && *offset_sec >= -max_abs_offset_sec;
}

// The implementation here was adapted from
// https://github.com/HowardHinnant/date/blob/040eed838bb1f695c31c6016dbe74bddc0302bb8/
// src/tz.cpp#L3652
// available under MIT license.
string TimezoneDatabase::LocalZoneName() {
  {
    // Allow ${TZ} to override the default zone.
    const char* zone = ":localtime";

    char* tz_env = nullptr;
    tz_env = getenv("TZ");
    if (tz_env) zone = tz_env;

    // We only support the "[:]<zone-name>" form.
    if (*zone == ':') ++zone;

    if (strcmp(zone, "localtime") != 0) return string(zone);

    // Fall through to try other means.
  }

  {
    // Check /etc/localtime.
    // - If it exists and is a symlink it should point to the current timezone file in the
    // zoneinfo directory.
    // - If it doesn't exist or is not a symlink we want to try other means.
    const char* localtime = "/etc/localtime";
    bool is_symbolic_link;
    string canonical_path;
    Status status = FileSystemUtil::IsSymbolicLink(localtime, &is_symbolic_link,
        &canonical_path);
    if (!status.ok()) {
      LOG(WARNING) << status.GetDetail();
    } else if (is_symbolic_link) {
      string linked_tz;
      if (FileSystemUtil::GetRelativePath(canonical_path, ZONE_INFO_DIR, &linked_tz)
          && !linked_tz.empty()) {
        return linked_tz;
      }

      LOG(WARNING) << "Symbolic link " << localtime << " resolved to the wrong path: "
                   << canonical_path;
    }
    // Fall through to try other means.
  }

  {
    // On some versions of some linux distro's (e.g. Ubuntu), the current timezone might
    // be in the first line of the /etc/timezone file.
    ifstream timezone_file("/etc/timezone");
    if (timezone_file.is_open()) {
      string result;
      getline(timezone_file, result);
      if (!result.empty()) return result;
    }
    // Fall through to try other means.
  }

  {
    // On some versions of some linux distro's (e.g. Red Hat), the current timezone might
    // be in the first line of the /etc/sysconfig/clock file as: ZONE="US/Eastern"
    ifstream timezone_file("/etc/sysconfig/clock");
    string result;
    while (timezone_file) {
      getline(timezone_file, result);
      if (result.rfind('#', 0) == 0) continue;
      auto p = result.find("ZONE=\"");
      if (p != string::npos) {
        result.erase(p, p + 6);
        result.erase(result.rfind('"'));
        return result;
      }
    }
    // Fall through to try other means.
  }

  LOG(WARNING) << "Could not get local timezone.";
  return "";
}

Status TimezoneDatabase::LoadZoneInfoFromHdfs(const string& hdfs_zone_info_zip,
    const string& local_dir) {
  DCHECK(!hdfs_zone_info_zip.empty());

  hdfsFS hdfs_conn, local_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(hdfs_zone_info_zip, &hdfs_conn));
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetLocalConnection(&local_conn));

  // Create a temporary directory to copy the timezone db to. The CCTZ interface only
  // loads timezone info from a directory. We abort the startup if this initialization
  // fails for some reason.
  string pathname = JoinPathSegments(local_dir, "impala.tzdb.XXXXXXX");

  // mkdtemp operates in place, so we need a mutable array.
  vector<char> local_path(pathname.c_str(), pathname.c_str() + pathname.size() + 1);
  if (mkdtemp(local_path.data()) == nullptr) {
    return Status(Substitute("Could not create temporary timezone directory: $0. Check "
        "that the directory $1 is writable by the user running Impala.",
        local_path.data(), local_dir));
  }

  Status status = CopyHdfsFile(hdfs_conn, hdfs_zone_info_zip, local_conn,
      local_path.data());
  if (!status.ok()) {
    discard_result(FileSystemUtil::RemovePaths({local_path.data()}));
    return status;
  }

  // Extract files from the zip archive.
  string archive_file = JoinPathSegments(local_path.data(),
      GetBaseName(hdfs_zone_info_zip.c_str()));
  string destination_dir = JoinPathSegments(local_path.data(), "tzdb");

  status = ZipUtil::ExtractFiles(archive_file, destination_dir);
  if (!status.ok()) {
    Status rm_status = FileSystemUtil::RemovePaths({local_path.data()});
    if (!rm_status.ok()) LOG(WARNING) << rm_status.GetDetail();
    return status;
  }

  // Find the root directory to load the time-zone db from.
  // - If 'destination_dir' contains only one subdirectory, root directory should be set
  // to that subdirectory.
  // - Otherwise, root directory is 'destination_dir'.
  vector<string> entry_names;
  status = FileSystemUtil::Directory::GetEntryNames(destination_dir, &entry_names, 2);
  if (!status.ok()) {
    Status rm_status = FileSystemUtil::RemovePaths({local_path.data()});
    if (!rm_status.ok()) LOG(WARNING) << rm_status.GetDetail();
    return status;
  }

  string zone_info_root_dir = destination_dir;
  if (entry_names.size() == 1) {
    string entry_path = JoinPathSegments(destination_dir, entry_names[0]);
    Status is_dir = FileSystemUtil::VerifyIsDirectory(entry_path);
    if (is_dir.ok()) zone_info_root_dir = entry_path;
  }

  status = LoadZoneInfo(zone_info_root_dir);

  Status rm_status = FileSystemUtil::RemovePaths({local_path.data()});
  if (!rm_status.ok()) LOG(WARNING) << rm_status.GetDetail();
  return status;
}

Status TimezoneDatabase::LoadZoneInfo(const string& zone_info_dir) {
  // Find canonical path for 'zone_info_dir'.
  string canonical_zone_info_dir;
  RETURN_IF_ERROR(FileSystemUtil::GetCanonicalPath(zone_info_dir,
      &canonical_zone_info_dir));

  // Load 'Timezone' objects into 'tz_path_map'.
  TimezoneMap tz_path_map;
  RETURN_IF_ERROR(LoadZoneInfoHelper(canonical_zone_info_dir, canonical_zone_info_dir,
      tz_path_map));

  // Iterate through 'tz_path_map' and add 'Timezone' objects to 'tz_name_map_'.
  // Use time-zone names as keys.
  for (const auto& tz: tz_path_map) {
    string tz_name;
    if (FileSystemUtil::GetRelativePath(tz.first, canonical_zone_info_dir, &tz_name)
        && IsTimezoneNameValid(tz_name)) {
      tz_name_map_[tz_name] = tz.second;
    } else {
      LOG(WARNING) << "Skipped adding " << tz.first << " to timezone db.";
    }
  }

  return Status::OK();
}

Status TimezoneDatabase::LoadZoneInfoHelper(const string& path,
    const string& zone_info_dir, TimezoneMap& tz_path_map) {
  Status status = Status::OK();

  FileSystemUtil::Directory dir(path);
  string entry_name;
  while (status.ok() && dir.GetNextEntryName(&entry_name)) {
    // Skip entries that are not valid time-zone name segments.
    if (!IsTimezoneNameSegmentValid(entry_name)) {
      LOG(WARNING) << "Skipping " << path << "/" << entry_name
                   << " path: " << entry_name
                   << " is not a valid time-zone segment name.";
      continue;
    }

    const string entry_path = JoinPathSegments(path, entry_name);
    Status is_dir = FileSystemUtil::VerifyIsDirectory(entry_path);
    if (is_dir.ok()) {
      // 'entry_path' is a directory. Load 'Timezone' objects from the directory
      // recursively.
      status = LoadZoneInfoHelper(entry_path, zone_info_dir, tz_path_map);
    } else {
      // Load time-zone from 'entry_path'. It will log a warning if 'entry_path' is not a
      // time-zone data file.
      LoadTimezone(entry_path, zone_info_dir, tz_path_map);
    }
  }

  if (status.ok()) status = dir.GetLastStatus();
  return status;
}

void TimezoneDatabase::LoadTimezone(const string& path, const string& zone_info_dir,
    TimezoneMap& tz_path_map) {
  bool is_symbolic_link;
  string canonical_path;
  Status status = FileSystemUtil::IsSymbolicLink(path, &is_symbolic_link,
      &canonical_path);
  if (!status.ok()) {
    LOG(WARNING) << status.GetDetail();
    return;
  }

  // 'path' is not a symbolic link. Read 'Timezone' from 'path'.
  if (!is_symbolic_link) {
    shared_ptr<Timezone> tz = LoadTimezoneHelper(path);
    if (tz != nullptr) tz_path_map[path] = tz;
    return;
  }

  // 'path' is a symbolic link. Check that the resolved canonical path is also under
  // 'zone_info_dir'.
  if (!FileSystemUtil::IsPrefixPath(zone_info_dir, canonical_path)) {
    LOG(WARNING) << "Symbolic link " << path << " resolved to the wrong path: "
                 << canonical_path;
    return;
  }

  // Check if 'canonical_path' has already been added as a key.
  auto it = tz_path_map.find(canonical_path);
  if (it != tz_path_map.end()) {
    tz_path_map[path] = it->second;
    return;
  }

  // 'canonical_path' hasn't been added as a key yet. Load 'Timezone' object and add it to
  // 'tz_path_map' as a value mapped both to 'path' and 'canonical_path'.
  shared_ptr<Timezone> tz = LoadTimezoneHelper(canonical_path);
  if (tz != nullptr) {
    tz_path_map[canonical_path] = tz_path_map[path] = tz;
  }
}

shared_ptr<Timezone> TimezoneDatabase::LoadTimezoneHelper(const string& path) {
  shared_ptr<Timezone> tz = make_shared<Timezone>();
  if (!cctz::load_time_zone(path, tz.get())) {
    LOG(WARNING) << "Could not load timezone: " << path;
    return nullptr;
  }
  return tz;
}

Status TimezoneDatabase::LoadZoneAliasesFromHdfs(const string& hdfs_zone_alias_conf) {
  DCHECK(!hdfs_zone_alias_conf.empty());

  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(
      HdfsFsCache::instance()->GetConnection(hdfs_zone_alias_conf, &hdfs_conn));

  hdfsFile hdfs_file = hdfsOpenFile(
      hdfs_conn, hdfs_zone_alias_conf.c_str(), O_RDONLY, 0, 0, 0);
  if (hdfs_file == nullptr) {
    return Status(GetHdfsErrorMsg("Failed to open HDFS file for reading: ",
        hdfs_zone_alias_conf));
  }

  Status status = Status::OK();
  vector<char> buffer(HDFS_READ_SIZE);
  int current_bytes_read = -1;
  stringstream ss;
  while (true) {
    current_bytes_read = hdfsRead(hdfs_conn, hdfs_file, buffer.data(), buffer.size());
    if (current_bytes_read == 0) break;
    if (current_bytes_read < 0) {
      status = Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          GetHdfsErrorMsg("Error reading from HDFS file: ", hdfs_zone_alias_conf));
      break;
    }
    ss << string(buffer.data(), current_bytes_read);
  }

  int hdfs_ret = hdfsCloseFile(hdfs_conn, hdfs_file);
  if (hdfs_ret != 0) {
    status.MergeStatus(
        Status(ErrorMsg(TErrorCode::GENERAL,
            GetHdfsErrorMsg("Failed to close HDFS file: ", hdfs_zone_alias_conf))));
  }

  if (status.ok()) {
    status = LoadZoneAliases(ss, hdfs_zone_alias_conf.c_str());
  }
  return status;
}

Status TimezoneDatabase::LoadZoneAliases(istream &is, const char* path) {
  string line, alias, value;
  const string err_msg_path_part = (path == nullptr) ? "" :  string(" in ") + path;
  int i = 0;

  while (is.good() && !is.eof()) {
    i++;
    getline(is, line);

    // Strip off comments.
    size_t comment = line.find('#');
    if (comment != string::npos) {
      line.resize(comment);
    }
    trim(line);
    if (line.empty()) continue;

    // Parse lines formatted as "alias = value".
    size_t equal_pos = line.find('=');
    if (equal_pos == string::npos) {
      return Status(Substitute("Error in line $0$1. '=' is missing.", i,
          err_msg_path_part));
    }

    // Check if alias name is present.
    alias = line.substr(0, equal_pos);
    trim(alias);
    if (alias.empty()) {
      return Status(Substitute("Error in line $0$1. Time-zone alias name is missing.", i,
          err_msg_path_part));
    }

    // Check if alias is already in 'tz_name_map_'.
    if (tz_name_map_.find(alias) != tz_name_map_.end()) {
      LOG(WARNING) << "Skipping line " << i << err_msg_path_part
                   << ". Duplicate time-zone alias: " << alias;
      continue;
    }

    // Value is either a fix offset in seconds or a time-zone name.
    value = line.substr(equal_pos + 1, string::npos);
    trim(value);
    if (value.empty()) {
      return Status(Substitute("Error in line $0$1. Missing value.", i,
          err_msg_path_part));
    }

    int64_t offset_sec;
    if (IsTimezoneOffsetValid(value, &offset_sec)) {
      // Add time-zone with a fix offset to the map.
      shared_ptr<Timezone> tz = make_shared<Timezone>(
          cctz::fixed_time_zone(cctz::sys_seconds(offset_sec)));
      tz_name_map_[alias] = tz;
    } else {
      // Check if the value is in the map.
      auto it_value = tz_name_map_.find(value);
      if (it_value != tz_name_map_.end()) {
        tz_name_map_[alias] = it_value->second;
      } else {
        LOG(WARNING) << "Skipping line " << i << err_msg_path_part
                     << ". Unknown time-zone name or invalid offset: " << value;
      }
    }
  }

  return Status::OK();
}

Status TimezoneDatabase::Initialize() {
  // Load 'Timezone' objects into 'tz_name_map_'. Use paths as keys.
  if (!FLAGS_hdfs_zone_info_zip.empty()) {
    tz_db_path_ = FLAGS_hdfs_zone_info_zip;
    RETURN_IF_ERROR(
        LoadZoneInfoFromHdfs(FLAGS_hdfs_zone_info_zip, FLAGS_local_library_dir));
  } else {
    tz_db_path_ = ZONE_INFO_DIR;
    RETURN_IF_ERROR(LoadZoneInfo(ZONE_INFO_DIR));
  }

  // Sanity check.
  if (tz_name_map_.find("UTC") == tz_name_map_.end()) {
    return Status("Failed to load UTC timezone info.");
  }

  // Add timezone aliases.
  if (!FLAGS_hdfs_zone_alias_conf.empty()) {
    RETURN_IF_ERROR(LoadZoneAliasesFromHdfs(FLAGS_hdfs_zone_alias_conf));
  } else {
    stringstream ss(TIMEZONE_ALIASES, ios_base::in);
    RETURN_IF_ERROR(LoadZoneAliases(ss));
  }

  return Status::OK();
}

}
