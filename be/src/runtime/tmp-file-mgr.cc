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

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>
#include <gutil/strings/join.h>

#include "runtime/tmp-file-mgr.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"

DEFINE_string(scratch_dirs, "/tmp", "Writable scratch directories");

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::filesystem::path;
using boost::uuids::random_generator;
using namespace strings;

namespace impala {

const string TMP_SUB_DIR_NAME = "impala-scratch";
const uint64_t AVAILABLE_SPACE_THRESHOLD_MB = 1024;
bool TmpFileMgr::initialized_;
vector<string> TmpFileMgr::tmp_dirs_;

Status TmpFileMgr::Init() {
  DCHECK(!initialized_);
  string tmp_dirs_spec = FLAGS_scratch_dirs;
  // Empty string should be interpreted as no scratch
  if (tmp_dirs_spec.empty()) {
    LOG(WARNING) << "Running without spill to disk: no scratch directories provided.";
    return Status::OK();
  }
  vector<string> all_tmp_dirs;
  split(all_tmp_dirs, tmp_dirs_spec, is_any_of(","), token_compress_on);
  vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);

  // For each tmp directory, find the disk it is on,
  // so additional tmp directories on the same disk can be skipped.
  for (int i = 0; i < all_tmp_dirs.size(); ++i) {
    path tmp_path(trim_right_copy_if(all_tmp_dirs[i], is_any_of("/")));
    // tmp_path must be a writable directory.
    Status status = FileSystemUtil::VerifyIsDirectory(tmp_path.string());
    if (!status.ok()) {
      LOG(WARNING) << "Cannot use directory " << tmp_path.string() << " for scratch: "
                   << status.msg().msg();
      continue;
    }
    // Find the disk id of tmp_path. Add the scratch directory if there isn't another
    // directory on the same disk (or if we don't know which disk it is on).
    int disk_id = DiskInfo::disk_id(tmp_path.c_str());
    if (disk_id < 0 || !is_tmp_dir_on_disk[disk_id]) {
      uint64_t available_space;
      RETURN_IF_ERROR(FileSystemUtil::GetSpaceAvailable(tmp_path.string(),
          &available_space));
      if (available_space < AVAILABLE_SPACE_THRESHOLD_MB * 1024 * 1024) {
        LOG(WARNING) << "Filesystem containing scratch directory " << tmp_path
                     << " has less than " << AVAILABLE_SPACE_THRESHOLD_MB
                     << "MB available.";
      }
      path create_dir_path(tmp_path / TMP_SUB_DIR_NAME);
      // Create the directory, destroying if already present. If this succeeds, we will
      // have an empty writable scratch directory.
      status = FileSystemUtil::CreateDirectory(create_dir_path.string());
      if (status.ok()) {
        if (disk_id >= 0) is_tmp_dir_on_disk[disk_id] = true;
        LOG(INFO) << "Using scratch directory " << create_dir_path.string() << " on disk "
                  << disk_id;
        tmp_dirs_.push_back(create_dir_path.string());
      } else {
        LOG(WARNING) << "Could not remove and recreate directory "
                     << create_dir_path.string() << ": cannot use it for scratch. "
                     << "Error was: " << status.msg().msg();
      }
    }
  }
  initialized_ = true;

  if (tmp_dirs_.empty()) {
    LOG(ERROR) << "Running without spill to disk: could not use any scratch "
               << "directories in list: " << tmp_dirs_spec << ". See previous warnings "
               << "for information on causes.";
  }
  return Status::OK();
}

Status TmpFileMgr::GetFile(int tmp_device_id, const TUniqueId& query_id,
    File** new_file) {
  DCHECK(initialized_);
  DCHECK_LT(tmp_device_id, tmp_dirs_.size());

  // Generate the full file path.
  string unique_name = lexical_cast<string>(random_generator()());
  stringstream file_name;
  file_name << PrintId(query_id) << "_" << unique_name;
  path new_file_path(tmp_dirs_[tmp_device_id]);
  new_file_path /= file_name.str();

  *new_file = new File(new_file_path.string());
  return Status::OK();
}

TmpFileMgr::File::File(const string& path)
  : path_(path),
    current_size_(0) {
}

Status TmpFileMgr::File::AllocateSpace(int64_t write_size, int64_t* offset) {
  DCHECK_GT(write_size, 0);
  if (current_size_ == 0) {
    // First call to AllocateSpace. Create the file.
    RETURN_IF_ERROR(FileSystemUtil::CreateFile(path_));
    disk_id_ = DiskInfo::disk_id(path_.c_str());
  }
  int64_t new_size = current_size_ + write_size;
  RETURN_IF_ERROR(FileSystemUtil::ResizeFile(path_, new_size));
  *offset = current_size_;
  current_size_ = new_size;
  return Status::OK();
}

Status TmpFileMgr::File::Remove() {
  if (current_size_ > 0) FileSystemUtil::RemovePaths(vector<string>(1, path_));
  return Status::OK();
}

} //namespace impala
