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

using namespace boost;
using namespace boost::filesystem;
using namespace boost::uuids;
using namespace std;
using namespace strings;

namespace impala {

const string TMP_SUB_DIR_NAME = "impala-scratch";
const uint64_t AVAILABLE_SPACE_THRESHOLD_MB = 1024;
bool TmpFileMgr::initialized_;
vector<string> TmpFileMgr::tmp_dirs_;

Status TmpFileMgr::Init() {
  DCHECK(!initialized_);
  string tmp_dirs_spec = FLAGS_scratch_dirs;
  vector<string> all_tmp_dirs;
  split(all_tmp_dirs, tmp_dirs_spec, is_any_of(","), token_compress_on);
  vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);

  // For each tmp directory, find the disk it is on,
  // so additional tmp directories on the same disk can be skipped.
  for (int i = 0; i < all_tmp_dirs.size(); ++i) {
    path tmp_path(trim_right_copy_if(all_tmp_dirs[i], is_any_of("/")));
    // tmp_path must be a writable directory.
    RETURN_IF_ERROR(FileSystemUtil::VerifyIsDirectory(tmp_path.string()));
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
      if (disk_id >= 0) is_tmp_dir_on_disk[disk_id] = true;
      path create_dir_path(tmp_path / TMP_SUB_DIR_NAME);
      tmp_dirs_.push_back(create_dir_path.string());
    }
  }
  initialized_ = true;
  Status status = FileSystemUtil::CreateDirectories(tmp_dirs_);
  if (status.ok()) {
    LOG (INFO) << "Created the following scratch dirs:" << JoinStrings(tmp_dirs_, " ");
  } else {
    // Attempt to remove the directories created. Ignore any errors.
    FileSystemUtil::RemovePaths(tmp_dirs_);
  }
  return status;
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
  return Status::OK;
}

TmpFileMgr::File::File(const string& path)
  : path_(path),
    current_offset_(0),
    current_size_(0) {
}

Status TmpFileMgr::File::AllocateSpace(int64_t write_size, int64_t* offset) {
  DCHECK_GT(write_size, 0);
  DCHECK_GE(current_size_, current_offset_);
  *offset = current_offset_;

  if (current_size_ == 0) {
    // First call to AllocateSpace. Create the file.
    RETURN_IF_ERROR(FileSystemUtil::CreateFile(path_));
    disk_id_ = DiskInfo::disk_id(path_.c_str());
  }

  current_offset_ += write_size;
  if (current_offset_ > current_size_) {
    int64_t trunc_len = current_offset_ + write_size;
    RETURN_IF_ERROR(FileSystemUtil::ResizeFile(path_, trunc_len));
    current_size_ = trunc_len;
  }

  DCHECK_GE(current_size_, current_offset_);
  return Status::OK;
}

Status TmpFileMgr::File::Remove() {
  if (current_size_ > 0) FileSystemUtil::RemovePaths(vector<string>(1, path_));
  return Status::OK;
}

} //namespace impala
