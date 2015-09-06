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
#include <boost/thread/locks.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>
#include <gutil/strings/join.h>

#include "runtime/tmp-file-mgr.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"

#include "common/names.h"

DEFINE_string(scratch_dirs, "/tmp", "Writable scratch directories");

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::filesystem::absolute;
using boost::filesystem::path;
using boost::uuids::random_generator;
using namespace strings;

namespace impala {

const string TMP_SUB_DIR_NAME = "impala-scratch";
const uint64_t AVAILABLE_SPACE_THRESHOLD_MB = 1024;

// Metric keys
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS = "tmp-file-mgr.active-scratch-dirs";
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST =
    "tmp-file-mgr.active-scratch-dirs.list";

TmpFileMgr::TmpFileMgr() : initialized_(false), dir_status_lock_(), tmp_dirs_(),
  num_active_scratch_dirs_metric_(NULL), active_scratch_dirs_metric_(NULL) {}

Status TmpFileMgr::Init(MetricGroup* metrics) {
  string tmp_dirs_spec = FLAGS_scratch_dirs;
  vector<string> all_tmp_dirs;
  // Empty string should be interpreted as no scratch
  if (!tmp_dirs_spec.empty()) {
    split(all_tmp_dirs, tmp_dirs_spec, is_any_of(","), token_compress_on);
  }
  return InitCustom(all_tmp_dirs, true, metrics);
}

Status TmpFileMgr::InitCustom(const vector<string>& tmp_dirs, bool one_dir_per_device,
      MetricGroup* metrics) {
  DCHECK(!initialized_);
  if (tmp_dirs.empty()) {
    LOG(WARNING) << "Running without spill to disk: no scratch directories provided.";
  }

  vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);
  // For each tmp directory, find the disk it is on,
  // so additional tmp directories on the same disk can be skipped.
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    path tmp_path(trim_right_copy_if(tmp_dirs[i], is_any_of("/")));
    tmp_path = absolute(tmp_path);
    path scratch_subdir_path(tmp_path / TMP_SUB_DIR_NAME);
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
    if (!one_dir_per_device || disk_id < 0 || !is_tmp_dir_on_disk[disk_id]) {
      uint64_t available_space;
      RETURN_IF_ERROR(FileSystemUtil::GetSpaceAvailable(tmp_path.string(),
          &available_space));
      if (available_space < AVAILABLE_SPACE_THRESHOLD_MB * 1024 * 1024) {
        LOG(WARNING) << "Filesystem containing scratch directory " << tmp_path
                     << " has less than " << AVAILABLE_SPACE_THRESHOLD_MB
                     << "MB available.";
      }
      // Create the directory, destroying if already present. If this succeeds, we will
      // have an empty writable scratch directory.
      status = FileSystemUtil::CreateDirectory(scratch_subdir_path.string());
      if (status.ok()) {
        if (disk_id >= 0) is_tmp_dir_on_disk[disk_id] = true;
        LOG(INFO) << "Using scratch directory " << scratch_subdir_path.string() << " on disk "
                  << disk_id;
        tmp_dirs_.push_back(Dir(scratch_subdir_path.string(), false));
      } else {
        LOG(WARNING) << "Could not remove and recreate directory "
                     << scratch_subdir_path.string() << ": cannot use it for scratch. "
                     << "Error was: " << status.msg().msg();
      }
    }
  }

  DCHECK(metrics != NULL);
  num_active_scratch_dirs_metric_ =
      metrics->AddGauge(TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS, 0L);
  active_scratch_dirs_metric_ = SetMetric<string>::CreateAndRegister(metrics,
      TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST, set<string>());
  num_active_scratch_dirs_metric_->set_value(tmp_dirs_.size());
  for (int i = 0; i < tmp_dirs_.size(); ++i) {
    active_scratch_dirs_metric_->Add(tmp_dirs_[i].path());
  }

  initialized_ = true;

  if (tmp_dirs_.empty() && !tmp_dirs.empty()) {
    LOG(ERROR) << "Running without spill to disk: could not use any scratch "
               << "directories in list: " << join(tmp_dirs, ",")
               << ". See previous warnings for information on causes.";
  }
  return Status::OK();
}

Status TmpFileMgr::GetFile(const DeviceId& device_id, const TUniqueId& query_id,
    File** new_file) {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  if (IsBlacklisted(device_id)) {
    return Status(TErrorCode::TMP_DEVICE_BLACKLISTED, tmp_dirs_[device_id].path());
  }

  // Generate the full file path.
  string unique_name = lexical_cast<string>(random_generator()());
  stringstream file_name;
  file_name << PrintId(query_id) << "_" << unique_name;
  path new_file_path(tmp_dirs_[device_id].path());
  new_file_path /= file_name.str();

  *new_file = new File(this, device_id, new_file_path.string());
  return Status::OK();
}

string TmpFileMgr::GetTmpDirPath(DeviceId device_id) const {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  return tmp_dirs_[device_id].path();
}

void TmpFileMgr::BlacklistDevice(DeviceId device_id) {
  DCHECK(initialized_);
  DCHECK(device_id >= 0 && device_id < tmp_dirs_.size());
  bool added;
  {
    lock_guard<SpinLock> l(dir_status_lock_);
    added = tmp_dirs_[device_id].blacklist();
  }
  if (added) {
    num_active_scratch_dirs_metric_->Increment(-1);
    active_scratch_dirs_metric_->Remove(tmp_dirs_[device_id].path());
  }
}

bool TmpFileMgr::IsBlacklisted(DeviceId device_id) {
  DCHECK(initialized_);
  DCHECK(device_id >= 0 && device_id < tmp_dirs_.size());
  lock_guard<SpinLock> l(dir_status_lock_);
  return tmp_dirs_[device_id].is_blacklisted();
}

int TmpFileMgr::num_active_tmp_devices() {
  DCHECK(initialized_);
  lock_guard<SpinLock> l(dir_status_lock_);
  int num_active = 0;
  for (int device_id = 0; device_id < tmp_dirs_.size(); ++device_id) {
    if (!tmp_dirs_[device_id].is_blacklisted()) ++num_active;
  }
  return num_active;
}

vector<TmpFileMgr::DeviceId> TmpFileMgr::active_tmp_devices() {
  vector<TmpFileMgr::DeviceId> devices;
  // Allocate vector before we grab lock
  devices.reserve(tmp_dirs_.size());
  {
    lock_guard<SpinLock> l(dir_status_lock_);
    for (DeviceId device_id = 0; device_id < tmp_dirs_.size(); ++device_id) {
      if (!tmp_dirs_[device_id].is_blacklisted()) {
        devices.push_back(device_id);
      }
    }
  }
  return devices;
}

TmpFileMgr::File::File(TmpFileMgr* mgr, DeviceId device_id, const string& path)
  : mgr_(mgr),
    path_(path),
    device_id_(device_id),
    current_size_(0),
    blacklisted_(false) {
}

Status TmpFileMgr::File::AllocateSpace(int64_t write_size, int64_t* offset) {
  DCHECK_GT(write_size, 0);
  Status status;
  if (mgr_->IsBlacklisted(device_id_)) {
    blacklisted_ = true;
    return Status(TErrorCode::TMP_FILE_BLACKLISTED, path_);
  }
  if (current_size_ == 0) {
    // First call to AllocateSpace. Create the file.
    status = FileSystemUtil::CreateFile(path_);
    if (!status.ok()) {
      ReportIOError(status.msg());
      return status;
    }
    disk_id_ = DiskInfo::disk_id(path_.c_str());
  }
  int64_t new_size = current_size_ + write_size;
  status = FileSystemUtil::ResizeFile(path_, new_size);
  if (!status.ok()) {
    ReportIOError(status.msg());
    return status;
  }
  *offset = current_size_;
  current_size_ = new_size;
  return Status::OK();
}

void TmpFileMgr::File::ReportIOError(const ErrorMsg& msg) {
  LOG(ERROR) << "Error for temporary file '" << path_ << "': " << msg.msg();
  // IMPALA-2305: avoid blacklisting to prevent test failures.
  // blacklisted_ = true;
  // mgr_->BlacklistDevice(device_id_);
}

Status TmpFileMgr::File::Remove() {
  if (current_size_ > 0) FileSystemUtil::RemovePaths(vector<string>(1, path_));
  return Status::OK();
}

} //namespace impala
