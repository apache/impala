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

#include "runtime/tmp-file-mgr.h"

#include <limits>
#include <mutex>
#include <linux/falloc.h>

#include <zstd.h> // for ZSTD_CLEVEL_DEFAULT
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gutil/strings/join.h>
#include <gutil/strings/substitute.h>

#include "kudu/util/env.h"
#include "runtime/bufferpool/buffer-pool-counters.h"
#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "util/bit-util.h"
#include "util/codec.h"
#include "util/collection-metrics.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/kudu-status-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

DEFINE_bool(disk_spill_encryption, true,
    "Set this to encrypt and perform an integrity "
    "check on all data spilled to disk during a query");
DEFINE_string(disk_spill_compression_codec, "",
    "(Advanced) If set, data will be compressed using the specified compression codec "
    "before spilling to disk. This can substantially reduce scratch disk usage, at the "
    "cost of requiring more CPU and memory resources to compress the data. Uses the same "
    "syntax as the COMPRESSION_CODEC query option, e.g. 'lz4', 'zstd', 'zstd:6'. If "
    "this is set, then --disk_spill_punch_holes must be enabled.");
DEFINE_int64(disk_spill_compression_buffer_limit_bytes, 512L * 1024L * 1024L,
    "(Advanced) Limit on the total bytes of compression buffers that will be used for "
    "spill-to-disk compression across all queries. If this limit is exceeded, some data "
    "may be spilled to disk in uncompressed form.");
DEFINE_bool(disk_spill_punch_holes, false,
    "(Advanced) changes the free space management strategy for files created in "
    "--scratch_dirs to punch holes in the file when space is unused. This can reduce "
    "the amount of scratch space used by queries, particularly in conjunction with "
    "disk spill compression. This option requires the filesystems of the directories "
    "in --scratch_dirs to support hole punching.");
DEFINE_string(scratch_dirs, "/tmp",
    "Writable scratch directories. "
    "This is a comma-separated list of directories. Each directory is "
    "specified as the directory path and an optional limit on the bytes that will "
    "be allocated in that directory. If the optional limit is provided, the path and "
    "the limit are separated by a colon. E.g. '/dir1:10G,/dir2:5GB,/dir3' will allow "
    "allocating up to 10GB of scratch in /dir1, 5GB of scratch in /dir2 and an "
    "unlimited amount in /dir3.");
DEFINE_bool(allow_multiple_scratch_dirs_per_device, true,
    "If false and --scratch_dirs contains multiple directories on the same device, "
    "then only the first writable directory is used");

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::filesystem::absolute;
using boost::filesystem::path;
using boost::uuids::random_generator;
using namespace impala::io;
using kudu::Env;
using kudu::RWFile;
using kudu::RWFileOptions;
using namespace strings;

namespace impala {

constexpr int64_t TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES;

const string TMP_SUB_DIR_NAME = "impala-scratch";
const uint64_t AVAILABLE_SPACE_THRESHOLD_MB = 1024;

// Metric keys
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS = "tmp-file-mgr.active-scratch-dirs";
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST =
    "tmp-file-mgr.active-scratch-dirs.list";
const string TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK =
    "tmp-file-mgr.scratch-space-bytes-used-high-water-mark";
const string TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED =
    "tmp-file-mgr.scratch-space-bytes-used";
const string SCRATCH_DIR_BYTES_USED_FORMAT =
    "tmp-file-mgr.scratch-space-bytes-used.dir-$0";

using DeviceId = TmpFileMgr::DeviceId;
using TmpDir = TmpFileMgr::TmpDir;
using WriteDoneCallback = TmpFileMgr::WriteDoneCallback;

TmpFileMgr::TmpFileMgr() {}

TmpFileMgr::~TmpFileMgr() {}

Status TmpFileMgr::Init(MetricGroup* metrics) {
  return InitCustom(FLAGS_scratch_dirs, !FLAGS_allow_multiple_scratch_dirs_per_device,
      FLAGS_disk_spill_compression_codec, FLAGS_disk_spill_punch_holes, metrics);
}

Status TmpFileMgr::InitCustom(const string& tmp_dirs_spec, bool one_dir_per_device,
    const string& compression_codec, bool punch_holes, MetricGroup* metrics) {
  vector<string> all_tmp_dirs;
  // Empty string should be interpreted as no scratch
  if (!tmp_dirs_spec.empty()) {
    split(all_tmp_dirs, tmp_dirs_spec, is_any_of(","), token_compress_on);
  }
  return InitCustom(
      all_tmp_dirs, one_dir_per_device, compression_codec, punch_holes, metrics);
}

Status TmpFileMgr::InitCustom(const vector<string>& tmp_dir_specifiers,
    bool one_dir_per_device, const string& compression_codec, bool punch_holes,
    MetricGroup* metrics) {
  DCHECK(!initialized_);
  punch_holes_ = punch_holes;
  if (tmp_dir_specifiers.empty()) {
    LOG(WARNING) << "Running without spill to disk: no scratch directories provided.";
  }
  if (!compression_codec.empty()) {
    if (!punch_holes) {
      return Status("--disk_spill_punch_holes must be true if disk spill compression "
                    "is enabled");
    }
    Status codec_parse_status = ParseUtil::ParseCompressionCodec(
        compression_codec, &compression_codec_, &compression_level_);
    if (!codec_parse_status.ok()) {
      return Status(
          Substitute("Could not parse --disk_spill_compression_codec value '$0': $1",
              compression_codec, codec_parse_status.GetDetail()));
    }
    if (compression_enabled()) {
      compressed_buffer_tracker_.reset(
          new MemTracker(FLAGS_disk_spill_compression_buffer_limit_bytes,
              "Spill-to-disk temporary compression buffers",
              ExecEnv::GetInstance()->process_mem_tracker()));
    }
  }
  vector<TmpDir> tmp_dirs;
  // Parse the directory specifiers. Don't return an error on parse errors, just log a
  // warning - we don't want to abort process startup because of misconfigured scratch,
  // since queries will generally still be runnable.
  for (const string& tmp_dir_spec : tmp_dir_specifiers) {
    vector<string> toks;
    split(toks, tmp_dir_spec, is_any_of(":"), token_compress_on);
    if (toks.size() > 2) {
      LOG(ERROR) << "Could not parse temporary dir specifier, too many colons: '"
                 << tmp_dir_spec << "'";
      continue;
    }
    int64_t bytes_limit = numeric_limits<int64_t>::max();
    if (toks.size() == 2) {
      bool is_percent;
      bytes_limit = ParseUtil::ParseMemSpec(toks[1], &is_percent, 0);
      if (bytes_limit < 0 || is_percent) {
        LOG(ERROR) << "Malformed data cache capacity configuration '" << tmp_dir_spec
                   << "'";
        continue;
      } else if (bytes_limit == 0) {
        // Interpret -1, 0 or empty string as no limit.
        bytes_limit = numeric_limits<int64_t>::max();
      }
    }
    IntGauge* bytes_used_metric = metrics->AddGauge(
        SCRATCH_DIR_BYTES_USED_FORMAT, 0, Substitute("$0", tmp_dirs.size()));
    tmp_dirs.emplace_back(toks[0], bytes_limit, bytes_used_metric);
  }

  vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);
  // For each tmp directory, find the disk it is on,
  // so additional tmp directories on the same disk can be skipped.
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    path tmp_path(trim_right_copy_if(tmp_dirs[i].path, is_any_of("/")));
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
      status = FileSystemUtil::RemoveAndCreateDirectory(scratch_subdir_path.string());
      if (status.ok()) {
        if (disk_id >= 0) is_tmp_dir_on_disk[disk_id] = true;
        LOG(INFO) << "Using scratch directory " << scratch_subdir_path.string() << " on "
                  << "disk " << disk_id
                  << " limit: " << PrettyPrinter::PrintBytes(tmp_dirs[i].bytes_limit);
        tmp_dirs_.emplace_back(scratch_subdir_path.string(), tmp_dirs[i].bytes_limit,
            tmp_dirs[i].bytes_used_metric);
      } else {
        LOG(WARNING) << "Could not remove and recreate directory "
                     << scratch_subdir_path.string() << ": cannot use it for scratch. "
                     << "Error was: " << status.msg().msg();
      }
      if (punch_holes_) {
        // Make sure hole punching is supported for the directory.
        // IMPALA-9798: this file should *not* be created inside impala-scratch
        // subdirectory to avoid races with multiple impalads starting up.
        RETURN_IF_ERROR(FileSystemUtil::CheckHolePunch(tmp_path.string()));
      }
    }
  }

  DCHECK(metrics != nullptr);
  num_active_scratch_dirs_metric_ =
      metrics->AddGauge(TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS, 0);
  active_scratch_dirs_metric_ = SetMetric<string>::CreateAndRegister(
      metrics, TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST, set<string>());
  num_active_scratch_dirs_metric_->SetValue(tmp_dirs_.size());
  for (int i = 0; i < tmp_dirs_.size(); ++i) {
    active_scratch_dirs_metric_->Add(tmp_dirs_[i].path);
  }
  scratch_bytes_used_metric_ =
      metrics->AddHWMGauge(TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK,
          TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED, 0);

  initialized_ = true;

  if (tmp_dirs_.empty() && !tmp_dirs.empty()) {
    LOG(ERROR) << "Running without spill to disk: could not use any scratch "
               << "directories in list: " << join(tmp_dir_specifiers, ",")
               << ". See previous warnings for information on causes.";
  }
  return Status::OK();
}

void TmpFileMgr::NewFile(
    TmpFileGroup* file_group, DeviceId device_id, unique_ptr<TmpFile>* new_file) {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  DCHECK(file_group != nullptr);
  // Generate the full file path.
  string unique_name = lexical_cast<string>(random_generator()());
  stringstream file_name;
  file_name << PrintId(file_group->unique_id()) << "_" << unique_name;
  path new_file_path(tmp_dirs_[device_id].path);
  new_file_path /= file_name.str();

  new_file->reset(new TmpFile(file_group, device_id, new_file_path.string()));
}

string TmpFileMgr::GetTmpDirPath(DeviceId device_id) const {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  return tmp_dirs_[device_id].path;
}

int TmpFileMgr::NumActiveTmpDevices() {
  DCHECK(initialized_);
  return tmp_dirs_.size();
}

vector<DeviceId> TmpFileMgr::ActiveTmpDevices() {
  vector<DeviceId> devices;
  for (DeviceId device_id = 0; device_id < tmp_dirs_.size(); ++device_id) {
    devices.push_back(device_id);
  }
  return devices;
}

TmpFile::TmpFile(TmpFileGroup* file_group, DeviceId device_id, const string& path)
  : file_group_(file_group),
    path_(path),
    device_id_(device_id),
    disk_id_(DiskInfo::disk_id(path.c_str())),
    blacklisted_(false) {
  DCHECK(file_group != nullptr);
}

bool TmpFile::AllocateSpace(int64_t num_bytes, int64_t* offset) {
  DCHECK_GT(num_bytes, 0);
  TmpDir* dir = GetDir();
  // Increment optimistically and roll back if the limit is exceeded.
  if (dir->bytes_used_metric->Increment(num_bytes) > dir->bytes_limit) {
    dir->bytes_used_metric->Increment(-num_bytes);
    return false;
  }
  *offset = allocation_offset_;
  allocation_offset_ += num_bytes;
  return true;
}

int TmpFile::AssignDiskQueue() const {
  return file_group_->io_mgr_->AssignQueue(path_.c_str(), disk_id_, false);
}

void TmpFile::Blacklist(const ErrorMsg& msg) {
  LOG(ERROR) << "Error for temporary file '" << path_ << "': " << msg.msg();
  blacklisted_ = true;
}

Status TmpFile::Remove() {
  // Remove the file if present (it may not be present if no writes completed).
  Status status = FileSystemUtil::RemovePaths({path_});
  int64_t bytes_in_use = file_group_->tmp_file_mgr_->punch_holes() ?
      allocation_offset_ - bytes_reclaimed_.Load() :
      allocation_offset_;
  GetDir()->bytes_used_metric->Increment(-bytes_in_use);
  return status;
}

TmpFileMgr::TmpDir* TmpFile::GetDir() {
  return &file_group_->tmp_file_mgr_->tmp_dirs_[device_id_];
}

Status TmpFile::PunchHole(int64_t offset, int64_t len) {
  DCHECK(file_group_->tmp_file_mgr_->punch_holes());
  // Because of RAII, the file is automatically closed when this function returns.
  RWFileOptions opts;
  opts.mode = Env::CREATE_OR_OPEN;
  unique_ptr<RWFile> file;
  KUDU_RETURN_IF_ERROR(Env::Default()->NewRWFile(opts, path_, &file),
      "Failed to open scratch file for hole punching");
  KUDU_RETURN_IF_ERROR(
      file->PunchHole(offset, len), "Failed to punch hole in scratch file");
  bytes_reclaimed_.Add(len);
  GetDir()->bytes_used_metric->Increment(-len);
  VLOG(3) << "Punched hole in " << path_ << " " << offset << " " << len;
  return Status::OK();
}

string TmpFile::DebugString() {
  return Substitute(
      "File $0 path '$1' device id $2 disk id $3 allocation offset $4 blacklisted $5",
      this, path_, device_id_, disk_id_, allocation_offset_, blacklisted_);
}

TmpFileGroup::TmpFileGroup(TmpFileMgr* tmp_file_mgr, DiskIoMgr* io_mgr,
    RuntimeProfile* profile, const TUniqueId& unique_id, int64_t bytes_limit)
  : tmp_file_mgr_(tmp_file_mgr),
    io_mgr_(io_mgr),
    io_ctx_(nullptr),
    unique_id_(unique_id),
    bytes_limit_(bytes_limit),
    write_counter_(ADD_COUNTER(profile, "ScratchWrites", TUnit::UNIT)),
    bytes_written_counter_(ADD_COUNTER(profile, "ScratchBytesWritten", TUnit::BYTES)),
    uncompressed_bytes_written_counter_(
        ADD_COUNTER(profile, "UncompressedScratchBytesWritten", TUnit::BYTES)),
    read_counter_(ADD_COUNTER(profile, "ScratchReads", TUnit::UNIT)),
    bytes_read_counter_(ADD_COUNTER(profile, "ScratchBytesRead", TUnit::BYTES)),
    scratch_space_bytes_used_counter_(
        ADD_COUNTER(profile, "ScratchFileUsedBytes", TUnit::BYTES)),
    disk_read_timer_(ADD_TIMER(profile, "TotalReadBlockTime")),
    encryption_timer_(ADD_TIMER(profile, "TotalEncryptionTime")),
    compression_timer_(tmp_file_mgr->compression_enabled() ?
            ADD_TIMER(profile, "TotalCompressionTime") :
            nullptr),
    current_bytes_allocated_(0),
    next_allocation_index_(0),
    free_ranges_(64) {
  DCHECK(tmp_file_mgr != nullptr);
  io_ctx_ = io_mgr_->RegisterContext();
}

TmpFileGroup::~TmpFileGroup() {
  DCHECK_EQ(tmp_files_.size(), 0);
}

Status TmpFileGroup::CreateFiles() {
  lock_.DCheckLocked();
  DCHECK(tmp_files_.empty());
  vector<DeviceId> tmp_devices = tmp_file_mgr_->ActiveTmpDevices();
  int files_allocated = 0;
  // Initialize the tmp files and the initial file to use.
  for (int i = 0; i < tmp_devices.size(); ++i) {
    DeviceId device_id = tmp_devices[i];
    unique_ptr<TmpFile> tmp_file;
    tmp_file_mgr_->NewFile(this, device_id, &tmp_file);
    tmp_files_.emplace_back(std::move(tmp_file));
    ++files_allocated;
  }
  DCHECK_EQ(tmp_files_.size(), files_allocated);
  if (tmp_files_.size() == 0) return ScratchAllocationFailedStatus({});
  // Start allocating on a random device to avoid overloading the first device.
  next_allocation_index_ = rand() % tmp_files_.size();
  return Status::OK();
}

void TmpFileGroup::Close() {
  // Cancel writes before deleting the files, since in-flight writes could re-create
  // deleted files.
  if (io_ctx_ != nullptr) io_mgr_->UnregisterContext(io_ctx_.get());
  for (std::unique_ptr<TmpFile>& file : tmp_files_) {
    Status status = file->Remove();
    if (!status.ok()) {
      LOG(WARNING) << "Error removing scratch file '" << file->path()
                   << "': " << status.msg().msg();
    }
  }
  tmp_file_mgr_->scratch_bytes_used_metric_->Increment(
      -1 * scratch_space_bytes_used_counter_->value());

  tmp_files_.clear();
}

// Rounds up to the smallest unit of allocation in a scratch file
// that will fit 'bytes'.
static int64_t RoundUpToScratchRangeSize(bool punch_holes, int64_t bytes) {
  if (punch_holes) {
    // Round up to a typical disk block size - 4KB that that hole punching can always
    // free the backing storage for the entire range.
    return BitUtil::RoundUpToPowerOf2(bytes, TmpFileMgr::HOLE_PUNCH_BLOCK_SIZE_BYTES);
  } else {
    // We recycle scratch ranges, which must be positive power-of-two sizes.
    return max<int64_t>(1L, BitUtil::RoundUpToPowerOfTwo(bytes));
  }
}

Status TmpFileGroup::AllocateSpace(
    int64_t num_bytes, TmpFile** tmp_file, int64_t* file_offset) {
  lock_guard<SpinLock> lock(lock_);
  int64_t scratch_range_bytes =
      RoundUpToScratchRangeSize(tmp_file_mgr_->punch_holes(), num_bytes);
  int free_ranges_idx = BitUtil::Log2Ceiling64(scratch_range_bytes);
  if (!free_ranges_[free_ranges_idx].empty()) {
    DCHECK(!tmp_file_mgr_->punch_holes()) << "Ranges not recycled when punching holes";
    *tmp_file = free_ranges_[free_ranges_idx].back().first;
    *file_offset = free_ranges_[free_ranges_idx].back().second;
    free_ranges_[free_ranges_idx].pop_back();
    return Status::OK();
  }

  if (bytes_limit_ != -1
      && current_bytes_allocated_ + scratch_range_bytes > bytes_limit_) {
    return Status(TErrorCode::SCRATCH_LIMIT_EXCEEDED, bytes_limit_, GetBackendString());
  }

  // Lazily create the files on the first write.
  if (tmp_files_.empty()) RETURN_IF_ERROR(CreateFiles());

  // Track the indices of any directories where we failed due to capacity. This is
  // required for error reporting if we are totally out of capacity so that it's clear
  // that some disks were at capacity.
  vector<int> at_capacity_dirs;

  // Find the next physical file in round-robin order and allocate a range from it.
  for (int attempt = 0; attempt < tmp_files_.size(); ++attempt) {
    int idx = next_allocation_index_;
    next_allocation_index_ = (next_allocation_index_ + 1) % tmp_files_.size();
    *tmp_file = tmp_files_[idx].get();
    if ((*tmp_file)->is_blacklisted()) continue;

    // Check the per-directory limit.
    if (!(*tmp_file)->AllocateSpace(scratch_range_bytes, file_offset)) {
      at_capacity_dirs.push_back(idx);
      continue;
    }
    scratch_space_bytes_used_counter_->Add(scratch_range_bytes);
    tmp_file_mgr_->scratch_bytes_used_metric_->Increment(scratch_range_bytes);
    current_bytes_allocated_ += scratch_range_bytes;
    return Status::OK();
  }
  return ScratchAllocationFailedStatus(at_capacity_dirs);
}

void TmpFileGroup::RecycleFileRange(unique_ptr<TmpWriteHandle> handle) {
  TmpFile* file = handle->file_;
  int64_t space_used_bytes =
      RoundUpToScratchRangeSize(tmp_file_mgr_->punch_holes(), handle->on_disk_len());
  if (tmp_file_mgr_->punch_holes()) {
    Status status = file->PunchHole(handle->write_range_->offset(), space_used_bytes);
    if (!status.ok()) {
      // Proceed even in the hole punching fails - we will use extra disk space but
      // functionally we can continue to spill.
      LOG_EVERY_N(WARNING, 100) << "Failed to punch hole in scratch file, couldn't "
                                << "reclaim space: " << status.GetDetail();
      return;
    }
    scratch_space_bytes_used_counter_->Add(-space_used_bytes);
    tmp_file_mgr_->scratch_bytes_used_metric_->Increment(-space_used_bytes);
    {
      lock_guard<SpinLock> lock(lock_);
      current_bytes_allocated_ -= space_used_bytes;
    }
  } else {
    int free_ranges_idx = BitUtil::Log2Ceiling64(space_used_bytes);
    lock_guard<SpinLock> lock(lock_);
    free_ranges_[free_ranges_idx].emplace_back(file, handle->write_range_->offset());
  }
}

Status TmpFileGroup::Write(MemRange buffer, WriteDoneCallback cb,
    unique_ptr<TmpWriteHandle>* handle, const BufferPoolClientCounters* counters) {
  DCHECK_GE(buffer.len(), 0);

  unique_ptr<TmpWriteHandle> tmp_handle(new TmpWriteHandle(this, cb));
  TmpWriteHandle* tmp_handle_ptr = tmp_handle.get(); // Pass ptr by value into lambda.
  WriteRange::WriteDoneCallback callback = [this, tmp_handle_ptr](
                                               const Status& write_status) {
    WriteComplete(tmp_handle_ptr, write_status);
  };
  RETURN_IF_ERROR(tmp_handle->Write(io_ctx_.get(), buffer, callback, counters));
  *handle = move(tmp_handle);
  return Status::OK();
}

Status TmpFileGroup::Read(TmpWriteHandle* handle, MemRange buffer) {
  RETURN_IF_ERROR(ReadAsync(handle, buffer));
  return WaitForAsyncRead(handle, buffer);
}

Status TmpFileGroup::ReadAsync(TmpWriteHandle* handle, MemRange buffer) {
  DCHECK(handle->write_range_ != nullptr);
  DCHECK(!handle->is_cancelled_);
  DCHECK_EQ(buffer.len(), handle->data_len());
  Status status;
  VLOG(3) << "ReadAsync " << handle->TmpFilePath() << " "
          << handle->write_range_->offset() << " " << handle->on_disk_len();
  // Don't grab 'write_state_lock_' in this method - it is not necessary because we
  // don't touch any members that it protects and could block other threads for the
  // duration of the synchronous read.
  DCHECK(!handle->write_in_flight_);
  DCHECK(handle->read_range_ == nullptr);
  DCHECK(handle->write_range_ != nullptr);

  MemRange read_buffer = buffer;
  if (handle->is_compressed()) {
    int64_t compressed_len = handle->compressed_len_;
    if (!handle->compressed_.TryAllocate(compressed_len)) {
      return tmp_file_mgr_->compressed_buffer_tracker()->MemLimitExceeded(
          nullptr, "Failed to decompress spilled data", compressed_len);
    }
    DCHECK_EQ(compressed_len, handle->write_range_->len());
    read_buffer = MemRange(handle->compressed_.buffer(), compressed_len);
  }

  // Don't grab handle->write_state_lock_, it is safe to touch all of handle's state
  // since the write is not in flight.
  handle->read_range_ = scan_range_pool_.Add(new ScanRange);
  handle->read_range_->Reset(nullptr, handle->write_range_->file(),
      handle->write_range_->len(), handle->write_range_->offset(),
      handle->write_range_->disk_id(), false, false, ScanRange::INVALID_MTIME,
      BufferOpts::ReadInto(
          read_buffer.data(), read_buffer.len(), BufferOpts::NO_CACHING));
  read_counter_->Add(1);
  bytes_read_counter_->Add(read_buffer.len());
  bool needs_buffers;
  RETURN_IF_ERROR(io_ctx_->StartScanRange(handle->read_range_, &needs_buffers));
  DCHECK(!needs_buffers) << "Already provided a buffer";
  return Status::OK();
}

Status TmpFileGroup::WaitForAsyncRead(
    TmpWriteHandle* handle, MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(handle->read_range_ != nullptr);
  // Don't grab handle->write_state_lock_, it is safe to touch all of handle's state
  // since the write is not in flight.
  SCOPED_TIMER(disk_read_timer_);
  MemRange read_buffer = handle->is_compressed() ?
      MemRange{handle->compressed_.buffer(), handle->compressed_.Size()} :
      buffer;
  DCHECK(read_buffer.data() != nullptr);
  unique_ptr<BufferDescriptor> io_mgr_buffer;
  Status status = handle->read_range_->GetNext(&io_mgr_buffer);
  if (!status.ok()) goto exit;
  DCHECK(io_mgr_buffer != NULL);
  DCHECK(io_mgr_buffer->eosr());
  DCHECK_LE(io_mgr_buffer->len(), read_buffer.len());
  if (io_mgr_buffer->len() < read_buffer.len()) {
    // The read was truncated - this is an error.
    status = Status(TErrorCode::SCRATCH_READ_TRUNCATED, read_buffer.len(),
        handle->write_range_->file(), GetBackendString(), handle->write_range_->offset(),
        io_mgr_buffer->len());
    goto exit;
  }
  DCHECK_EQ(io_mgr_buffer->buffer(),
      handle->is_compressed() ? handle->compressed_.buffer() : buffer.data());

  // Decrypt and decompress in the reverse order that we compressed then encrypted the
  // data originally.
  if (FLAGS_disk_spill_encryption) {
    status = handle->CheckHashAndDecrypt(read_buffer, counters);
    if (!status.ok()) goto exit;
  }

  if (handle->is_compressed()) {
    SCOPED_TIMER2(
        compression_timer_, counters == nullptr ? nullptr : counters->compression_time);
    scoped_ptr<Codec> decompressor;
    status = Codec::CreateDecompressor(
        nullptr, false, tmp_file_mgr_->compression_codec(), &decompressor);
    if (status.ok()) {
      int64_t decompressed_len = buffer.len();
      uint8_t* decompressed_buffer = buffer.data();
      status = decompressor->ProcessBlock(true, read_buffer.len(), read_buffer.data(),
          &decompressed_len, &decompressed_buffer);
    }
    // Free the compressed data regardless of whether the read was successful.
    handle->FreeCompressedBuffer();
    if (!status.ok()) goto exit;
  }
exit:
  // Always return the buffer before exiting to avoid leaking it.
  if (io_mgr_buffer != nullptr) handle->read_range_->ReturnBuffer(move(io_mgr_buffer));
  handle->read_range_ = nullptr;
  return status;
}

Status TmpFileGroup::RestoreData(unique_ptr<TmpWriteHandle> handle, MemRange buffer,
    const BufferPoolClientCounters* counters) {
  DCHECK_EQ(handle->data_len(), buffer.len());
  if (!handle->is_compressed()) DCHECK_EQ(handle->write_range_->data(), buffer.data());
  DCHECK(!handle->write_in_flight_);
  DCHECK(handle->read_range_ == nullptr);

  VLOG(3) << "Restore " << handle->TmpFilePath() << " " << handle->write_range_->offset()
          << " " << handle->data_len();
  Status status;
  if (handle->is_compressed()) {
    // 'buffer' already contains the data needed, because the compressed data was written
    // to 'compressed_' and (optionally) encrypted over there.
  } else if (FLAGS_disk_spill_encryption) {
    // Decrypt after the write is finished, so that we don't accidentally write decrypted
    // data to disk.
    status = handle->CheckHashAndDecrypt(buffer, counters);
  }
  RecycleFileRange(move(handle));
  return status;
}

void TmpFileGroup::DestroyWriteHandle(unique_ptr<TmpWriteHandle> handle) {
  handle->Cancel();
  handle->WaitForWrite();
  RecycleFileRange(move(handle));
}

void TmpFileGroup::WriteComplete(
    TmpWriteHandle* handle, const Status& write_status) {
  Status status;
  if (!write_status.ok()) {
    status = RecoverWriteError(handle, write_status);
    if (status.ok()) return;
  } else {
    status = write_status;
  }
  handle->WriteComplete(status);
}

Status TmpFileGroup::RecoverWriteError(
    TmpWriteHandle* handle, const Status& write_status) {
  DCHECK(!write_status.ok());
  DCHECK(handle->file_ != nullptr);

  // We can't recover from cancellation or memory limit exceeded.
  if (write_status.IsCancelled() || write_status.IsMemLimitExceeded()) {
    return write_status;
  }

  // Save and report the error before retrying so that the failure isn't silent.
  {
    lock_guard<SpinLock> lock(lock_);
    scratch_errors_.push_back(write_status);
    handle->file_->Blacklist(write_status.msg());
  }

  // Do not retry cancelled writes or propagate the error, simply return CANCELLED.
  if (handle->is_cancelled_) return Status::CancelledInternal("TmpFileMgr write");

  TmpFile* tmp_file;
  int64_t file_offset;
  // Discard the scratch file range - we will not reuse ranges from a bad file.
  // Choose another file to try. Blacklisting ensures we don't retry the same file.
  // If this fails, the status will include all the errors in 'scratch_errors_'.
  RETURN_IF_ERROR(AllocateSpace(handle->on_disk_len(), &tmp_file, &file_offset));
  return handle->RetryWrite(io_ctx_.get(), tmp_file, file_offset);
}

Status TmpFileGroup::ScratchAllocationFailedStatus(
    const vector<int>& at_capacity_dirs) {
  vector<string> tmp_dir_paths;
  for (TmpDir& tmp_dir : tmp_file_mgr_->tmp_dirs_) {
    tmp_dir_paths.push_back(tmp_dir.path);
  }
  vector<string> at_capacity_dir_paths;
  for (int dir_idx : at_capacity_dirs) {
    at_capacity_dir_paths.push_back(tmp_file_mgr_->tmp_dirs_[dir_idx].path);
  }
  Status status(TErrorCode::SCRATCH_ALLOCATION_FAILED, join(tmp_dir_paths, ","),
      GetBackendString(),
      PrettyPrinter::PrintBytes(
        tmp_file_mgr_->scratch_bytes_used_metric_->current_value()->GetValue()),
      PrettyPrinter::PrintBytes(current_bytes_allocated_),
      join(at_capacity_dir_paths, ","));
  // Include all previous errors that may have caused the failure.
  for (Status& err : scratch_errors_) status.MergeStatus(err);
  return status;
}

string TmpFileGroup::DebugString() {
  lock_guard<SpinLock> lock(lock_);
  stringstream ss;
  ss << "TmpFileGroup " << this << " bytes limit " << bytes_limit_
     << " current bytes allocated " << current_bytes_allocated_
     << " next allocation index " << next_allocation_index_ << " writes "
     << write_counter_->value() << " bytes written " << bytes_written_counter_->value()
     << " uncompressed bytes written " << uncompressed_bytes_written_counter_->value()
     << " reads " << read_counter_->value() << " bytes read "
     << bytes_read_counter_->value() << " scratch bytes used "
     << scratch_space_bytes_used_counter_ << " dist read timer "
     << disk_read_timer_->value() << " encryption timer " << encryption_timer_->value()
     << endl
     << "  " << tmp_files_.size() << " files:" << endl;
  for (unique_ptr<TmpFile>& file : tmp_files_) {
    ss << "    " << file->DebugString() << endl;
  }
  return ss.str();
}

TmpWriteHandle::TmpWriteHandle(
    TmpFileGroup* const parent, WriteRange::WriteDoneCallback cb)
  : parent_(parent),
    cb_(cb),
    compressed_(parent_->tmp_file_mgr_->compressed_buffer_tracker()) {}

TmpWriteHandle::~TmpWriteHandle() {
  DCHECK(!write_in_flight_);
  DCHECK(read_range_ == nullptr);
  DCHECK(compressed_.buffer() == nullptr);
}

string TmpWriteHandle::TmpFilePath() const {
  if (file_ == nullptr) return "";
  return file_->path();
}

int64_t TmpWriteHandle::on_disk_len() const {
  return write_range_->len();
}

Status TmpWriteHandle::Write(RequestContext* io_ctx, MemRange buffer,
    WriteRange::WriteDoneCallback callback, const BufferPoolClientCounters* counters) {
  DCHECK(!write_in_flight_);
  MemRange buffer_to_write = buffer;
  if (parent_->tmp_file_mgr_->compression_enabled() && TryCompress(buffer, counters)) {
    buffer_to_write = MemRange(compressed_.buffer(), compressed_len_);
  }
  // Ensure that the compressed buffer is freed on all the code paths where we did not
  // start the write successfully.
  bool write_started = false;
  const auto free_compressed = MakeScopeExitTrigger([this, &write_started]() {
      if (!write_started) FreeCompressedBuffer();
  });

  // Allocate space after doing compression, to avoid overallocating space.
  TmpFile* tmp_file;
  int64_t file_offset;
  RETURN_IF_ERROR(parent_->AllocateSpace(buffer_to_write.len(), &tmp_file, &file_offset));

  if (FLAGS_disk_spill_encryption) {
    RETURN_IF_ERROR(EncryptAndHash(buffer_to_write, counters));
  }

  // Set all member variables before calling AddWriteRange(): after it succeeds,
  // WriteComplete() may be called concurrently with the remainder of this function.
  data_len_ = buffer.len();
  file_ = tmp_file;
  write_range_.reset(new WriteRange(
      tmp_file->path(), file_offset, tmp_file->AssignDiskQueue(), callback));
  write_range_->SetData(buffer_to_write.data(), buffer_to_write.len());
  VLOG(3) << "Write " << tmp_file->path() << " " << file_offset << " "
          << buffer_to_write.len();
  write_in_flight_ = true;
  Status status = io_ctx->AddWriteRange(write_range_.get());
  if (!status.ok()) {
    // The write will not be in flight if we returned with an error.
    write_in_flight_ = false;
    // We won't return this TmpWriteHandle to the client of TmpFileGroup, so it won't be
    // cancelled in the normal way. Mark the handle as cancelled so it can be
    // cleanly destroyed.
    is_cancelled_ = true;
    return status;
  }
  write_started = true;
  parent_->write_counter_->Add(1);
  parent_->uncompressed_bytes_written_counter_->Add(buffer.len());
  parent_->bytes_written_counter_->Add(buffer_to_write.len());
  return Status::OK();
}

bool TmpWriteHandle::TryCompress(
    MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(parent_->tmp_file_mgr_->compression_enabled());
  SCOPED_TIMER2(parent_->compression_timer_,
      counters == nullptr ? nullptr : counters->compression_time);
  DCHECK_LT(compressed_len_, 0);
  DCHECK(compressed_.buffer() == nullptr);
  scoped_ptr<Codec> compressor;
  Status status = Codec::CreateCompressor(nullptr, false,
      Codec::CodecInfo(parent_->tmp_file_mgr_->compression_codec(),
          parent_->tmp_file_mgr_->compression_level()),
      &compressor);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to compress, couldn't create compressor: "
                 << status.GetDetail();
    return false;
  }
  int64_t compressed_buffer_len = compressor->MaxOutputLen(buffer.len());
  if (!compressed_.TryAllocate(compressed_buffer_len)) {
    LOG_EVERY_N(INFO, 100) << "Failed to compress: couldn't allocate "
                           << PrettyPrinter::PrintBytes(compressed_buffer_len);
    return false;
  }
  uint8_t* compressed_buffer = compressed_.buffer();
  int64_t compressed_len = compressed_buffer_len;
  status = compressor->ProcessBlock(
      true, buffer.len(), buffer.data(), &compressed_len, &compressed_buffer);
  if (!status.ok()) {
    compressed_.Release();
    return false;
  }
  compressed_len_ = compressed_len;
  VLOG(3) << "Buffer size: " << buffer.len() << " compressed size: " << compressed_len;
  return true;
}

Status TmpWriteHandle::RetryWrite(RequestContext* io_ctx, TmpFile* file, int64_t offset) {
  DCHECK(write_in_flight_);
  file_ = file;
  write_range_->SetRange(file->path(), offset, file->AssignDiskQueue());
  Status status = io_ctx->AddWriteRange(write_range_.get());
  if (!status.ok()) {
    // The write will not be in flight if we returned with an error.
    write_in_flight_ = false;
    return status;
  }
  return Status::OK();
}

void TmpWriteHandle::WriteComplete(const Status& write_status) {
  WriteDoneCallback cb;
  {
    lock_guard<mutex> lock(write_state_lock_);
    DCHECK(write_in_flight_);
    write_in_flight_ = false;
    // Need to extract 'cb_' because once 'write_in_flight_' is false and we release
    // 'write_state_lock_', 'this' may be destroyed.
    cb = move(cb_);

    if (is_compressed()) {
      DCHECK(compressed_.buffer() != nullptr);
      FreeCompressedBuffer();
    }

    // Notify before releasing the lock - after the lock is released 'this' may be
    // destroyed.
    write_complete_cv_.NotifyAll();
  }
  // Call 'cb' last - once 'cb' is called client code may call Read() or destroy this
  // handle.
  cb(write_status);
}

void TmpWriteHandle::Cancel() {
  CancelRead();
  {
    unique_lock<mutex> lock(write_state_lock_);
    is_cancelled_ = true;
    // TODO: in future, if DiskIoMgr supported write cancellation, we could cancel it
    // here.
  }
}

void TmpWriteHandle::CancelRead() {
  if (read_range_ != nullptr) {
    read_range_->Cancel(Status::CancelledInternal("TmpFileMgr read"));
    read_range_ = nullptr;
    FreeCompressedBuffer();
  }
}

void TmpWriteHandle::WaitForWrite() {
  unique_lock<mutex> lock(write_state_lock_);
  while (write_in_flight_) write_complete_cv_.Wait(lock);
}

Status TmpWriteHandle::EncryptAndHash(
    MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(FLAGS_disk_spill_encryption);
  SCOPED_TIMER2(parent_->encryption_timer_,
      counters == nullptr ? nullptr : counters->encryption_time);
  // Since we're using GCM/CTR/CFB mode, we must take care not to reuse a
  // key/IV pair. Regenerate a new key and IV for every data buffer we write.
  key_.InitializeRandom();
  RETURN_IF_ERROR(key_.Encrypt(buffer.data(), buffer.len(), buffer.data()));

  if (!key_.IsGcmMode()) {
    hash_.Compute(buffer.data(), buffer.len());
  }
  return Status::OK();
}

Status TmpWriteHandle::CheckHashAndDecrypt(
    MemRange buffer, const BufferPoolClientCounters* counters) {
  DCHECK(FLAGS_disk_spill_encryption);
  DCHECK(write_range_ != nullptr);
  SCOPED_TIMER2(parent_->encryption_timer_,
      counters == nullptr ? nullptr : counters->encryption_time);

  // GCM mode will verify the integrity by itself
  if (!key_.IsGcmMode()) {
    if (!hash_.Verify(buffer.data(), buffer.len())) {
      return Status(TErrorCode::SCRATCH_READ_VERIFY_FAILED, buffer.len(),
        write_range_->file(), GetBackendString(), write_range_->offset());
    }
  }
  Status decrypt_status = key_.Decrypt(buffer.data(), buffer.len(), buffer.data());
  if (!decrypt_status.ok()) {
    // Treat decryption failing as a verification failure, but include extra info from
    // the decryption status.
    Status result_status(TErrorCode::SCRATCH_READ_VERIFY_FAILED, buffer.len(),
          write_range_->file(), GetBackendString(), write_range_->offset());
    result_status.MergeStatus(decrypt_status);
    return result_status;
  }
  return Status::OK();
}

void TmpWriteHandle::FreeCompressedBuffer() {
  if (compressed_.buffer() == nullptr) return;
  DCHECK(is_compressed());
  compressed_.Release();
}

string TmpWriteHandle::DebugString() {
  unique_lock<mutex> lock(write_state_lock_);
  stringstream ss;
  ss << "Write handle " << this << " file '" << file_->path() << "'"
     << " is cancelled " << is_cancelled_ << " write in flight " << write_in_flight_;
  if (write_range_ != NULL) {
    ss << " data " << write_range_->data() << " disk range len " << write_range_->len()
       << " file offset " << write_range_->offset() << " disk id "
       << write_range_->disk_id();
  }
  return ss.str();
}
} // namespace impala
