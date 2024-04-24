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
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/error-converter.h"
#include "runtime/io/local-file-writer.h"
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
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/kudu-status-util.h"
#include "util/mem-info.h"
#include "util/os-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/scope-exit-trigger.h"
#include "util/string-parser.h"

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
    "specified as the directory path, an optional limit on the bytes that will "
    "be allocated in that directory, and an optional priority for the directory. "
    "If the optional limit is provided, the path and "
    "the limit are separated by a colon. E.g. '/dir1:10G,/dir2:5GB,/dir3' will allow "
    "allocating up to 10GB of scratch in /dir1, 5GB of scratch in /dir2 and an "
    "unlimited amount in /dir3. "
    "If the optional priority is provided, the path and the limit and priority are "
    "separated by colon. Priority based spilling will result in directories getting "
    "selected as a spill target based on their priority. The lower the numerical value "
    "the higher the priority. E.g. '/dir1:10G:0,/dir2:5GB:1,/dir3::1', will cause "
    "spilling to first fill up '/dir1' followed by using '/dir2' and '/dir3' in a "
    "round robin manner.");
DEFINE_bool(allow_multiple_scratch_dirs_per_device, true,
    "If false and --scratch_dirs contains multiple directories on the same device, "
    "then only the first writable directory is used");
DEFINE_string(remote_tmp_file_size, "16M",
    "Specify the size of a remote temporary file. Upper bound is 256MB. Lower bound "
    "is the block size. The size should be power of 2 and integer times of the block "
    "size.");
DEFINE_string(remote_tmp_file_block_size, "1M",
    "Specify the size of the block for doing file uploading and fetching. The block "
    "size should be power of 2 and less than the size of remote temporary file.");
DEFINE_string(remote_read_memory_buffer_size, "1G",
    "Specify the maximum size of read memory buffers for the remote temporary "
    "files. Only valid when --remote_batch_read is true.");
DEFINE_bool(remote_tmp_files_avail_pool_lifo, false,
    "If true, lifo is the algo to evict the local buffer files during spilling "
    "to the remote. Otherwise, fifo would be used.");
DEFINE_int32(wait_for_spill_buffer_timeout_s, 60,
    "Specify the timeout duration waiting for the buffer to write (second). If a spilling"
    "opertion fails to get a buffer from the pool within the duration, the operation"
    "fails.");
DEFINE_bool(remote_batch_read, false,
    "Set if the system uses batch reading for the remote temporary files. Batch reading"
    "allows reading a block asynchronously when the buffer pool is trying to pin one"
    "page of that block.");

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::token_compress_off;
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
const uint64_t MAX_REMOTE_TMPFILE_SIZE_THRESHOLD_MB = 512;

// For spilling to remote fs, the max size of a read memory block.
const uint64_t MAX_REMOTE_READ_MEM_BLOCK_THRESHOLD_BYTES = 16 * 1024 * 1024;

// The memory limits for the memory buffer to read the spilled data in the remote fs.
// The maximum bytes of the read buffer should be limited by the
// REMOTE_READ_BUFFER_MAX_MEM_PERCENT, which stands for the percentage of the total
// memory and REMOTE_READ_BUFFER_MEM_HARD_LIMIT_PERCENT, which stands for the percentage
// of the remaining memory which is not used by the process.
// Also, if the remaining memory is less than REMOTE_READ_BUFFER_DISABLE_THRESHOLD_PERCENT
// of the total memory, then the read buffer for remote spilled data should be disabled.
const double REMOTE_READ_BUFFER_MAX_MEM_PERCENT = 0.1;
const double REMOTE_READ_BUFFER_MEM_HARD_LIMIT_PERCENT = 0.5;
const double REMOTE_READ_BUFFER_DISABLE_THRESHOLD_PERCENT = 0.05;

// Metric keys
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS = "tmp-file-mgr.active-scratch-dirs";
const string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST =
    "tmp-file-mgr.active-scratch-dirs.list";
const string TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK =
    "tmp-file-mgr.scratch-space-bytes-used-high-water-mark";
const string TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED =
    "tmp-file-mgr.scratch-space-bytes-used";
const string TMP_FILE_MGR_SCRATCH_READ_MEMORY_BUFFER_USED_HIGH_WATER_MARK =
    "tmp-file-mgr.scratch-read-memory-buffer-used-high-water-mark";
const string TMP_FILE_MGR_SCRATCH_READ_MEMORY_BUFFER_USED =
    "tmp-file-mgr.scratch-read-memory-buffer-used";
const string SCRATCH_DIR_BYTES_USED_FORMAT =
    "tmp-file-mgr.scratch-space-bytes-used.dir-$0";
const string LOCAL_BUFF_BYTES_USED_FORMAT = "tmp-file-mgr.local-buff-bytes-used.dir-$0";
const string TMP_FILE_BUFF_POOL_DEQUEUE_DURATIONS =
    "tmp-file-mgr.tmp-file-buff-pool-dequeue-durations";

static const Status& TMP_FILE_MGR_NO_AVAILABLE_FILE_TO_EVICT = Status(ErrorMsg::Init(
    TErrorCode::GENERAL,
    "TmpFileMgr::ReserveLocalBufferSpace() failed to find available files to evict."));
static const Status& TMP_FILE_BUFFER_POOL_CONTEXT_CANCELLED =
    Status::CancelledInternal("TmpFileBufferPool");

using DeviceId = TmpFileMgr::DeviceId;
using WriteDoneCallback = TmpFileMgr::WriteDoneCallback;

TmpFileMgr::TmpFileMgr() {}

TmpFileMgr::~TmpFileMgr() {
  if(tmp_dirs_remote_ctrl_.tmp_file_pool_ != nullptr) {
    tmp_dirs_remote_ctrl_.tmp_file_pool_->ShutDown();
    tmp_dirs_remote_ctrl_.tmp_file_mgr_thread_group_.JoinAll();
  }
}

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
  one_dir_per_device_ = one_dir_per_device;
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

  bool is_percent;
  tmp_dirs_remote_ctrl_.remote_tmp_file_size_ =
      ParseUtil::ParseMemSpec(FLAGS_remote_tmp_file_size, &is_percent, 0);
  if (tmp_dirs_remote_ctrl_.remote_tmp_file_size_ <= 0) {
    return Status(Substitute(
        "Invalid value of remote_tmp_file_size '$0'", FLAGS_remote_tmp_file_size));
  }
  if (tmp_dirs_remote_ctrl_.remote_tmp_file_size_
      > MAX_REMOTE_TMPFILE_SIZE_THRESHOLD_MB * 1024 * 1024) {
    tmp_dirs_remote_ctrl_.remote_tmp_file_size_ =
        MAX_REMOTE_TMPFILE_SIZE_THRESHOLD_MB * 1024 * 1024;
  }
  tmp_dirs_remote_ctrl_.remote_tmp_block_size_ =
      ParseUtil::ParseMemSpec(FLAGS_remote_tmp_file_block_size, &is_percent,
          tmp_dirs_remote_ctrl_.remote_tmp_file_size_);
  if (tmp_dirs_remote_ctrl_.remote_tmp_block_size_ <= 0) {
    return Status(Substitute(
        "Invalid value of remote_tmp_block_size '$0'", FLAGS_remote_tmp_file_block_size));
  }
  tmp_dirs_remote_ctrl_.wait_for_spill_buffer_timeout_us_ =
      FLAGS_wait_for_spill_buffer_timeout_s * MICROS_PER_SEC;
  if (tmp_dirs_remote_ctrl_.wait_for_spill_buffer_timeout_us_ <= 0) {
    return Status(Substitute("Invalid value of wait_for_spill_buffer_timeout_us '$0'",
        FLAGS_wait_for_spill_buffer_timeout_s));
  }

  tmp_dirs_remote_ctrl_.remote_batch_read_enabled_ = FLAGS_remote_batch_read;
  if (tmp_dirs_remote_ctrl_.remote_batch_read_enabled_) {
    Status setup_read_buffer_status = tmp_dirs_remote_ctrl_.SetUpReadBufferParams();
    if (!setup_read_buffer_status.ok()) {
      LOG(WARNING) << "Disabled the read buffer for the remote temporary files "
                      "due to errors in read buffer parameters: "
                   << setup_read_buffer_status.msg().msg();
      tmp_dirs_remote_ctrl_.remote_batch_read_enabled_ = false;
    }
  }

  // Below options are using for test by setting different modes to implement the
  // spilling to the remote.
  tmp_dirs_remote_ctrl_.remote_tmp_files_avail_pool_lifo_ =
      FLAGS_remote_tmp_files_avail_pool_lifo;

  vector<std::unique_ptr<TmpDir>> tmp_dirs;
  // need_local_buffer_dir indicates if currently we need to a directory in local scratch
  // space for being the buffer of a remote directory.
  bool need_local_buffer_dir = false;

  // Parse the directory specifiers. Don't return an error on parse errors, just log a
  // warning - we don't want to abort process startup because of misconfigured scratch,
  // since queries will generally still be runnable.
  for (const string& tmp_dir_spec : tmp_dir_specifiers) {
    string tmp_dir_spec_trimmed(boost::algorithm::trim_copy(tmp_dir_spec));
    std::unique_ptr<TmpDir> tmp_dir;

    if (IsHdfsPath(tmp_dir_spec_trimmed.c_str(), false)
        || IsOzonePath(tmp_dir_spec_trimmed.c_str(), false)) {
      tmp_dir = std::make_unique<TmpDirHdfs>(tmp_dir_spec_trimmed);
    } else if (IsS3APath(tmp_dir_spec_trimmed.c_str(), false)) {
      // Initialize the S3 options for later getting S3 connection.
      s3a_options_ = {make_pair("fs.s3a.fast.upload", "true"),
          make_pair("fs.s3a.fast.upload.buffer", "disk")};
      tmp_dir = std::make_unique<TmpDirS3>(tmp_dir_spec_trimmed);
    } else if (IsGcsPath(tmp_dir_spec_trimmed.c_str(), false)) {
      // TODO(IMPALA-10561): Add support for spilling to GCS
    } else {
      tmp_dir = std::make_unique<TmpDirLocal>(tmp_dir_spec_trimmed);
    }

    DCHECK(tmp_dir != nullptr);
    Status parse_status = tmp_dir->Parse();
    if (!parse_status.ok()) {
      LOG(WARNING) << "Directory " << tmp_dir_spec.c_str() << " is not used because "
                   << parse_status.msg().msg();
      continue;
    }

    if (!tmp_dir->is_local()) {
      // Set the flag to reserve a local dir for buffer.
      // If the flag has been set, meaning that there is already one remote dir
      // registered, since we only support one remote dir, this remote dir will be
      // abandoned.
      if (need_local_buffer_dir) {
        LOG(WARNING) << "Only one remote directory is supported. Extra remote directory "
                     << tmp_dir_spec.c_str() << " is not used.";
        continue;
      } else {
        need_local_buffer_dir = true;
      }
    }
    tmp_dirs.emplace_back(move(tmp_dir));
  }

  vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);
  // For each tmp directory, find the disk it is on,
  // so additional tmp directories on the same disk can be skipped.
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    Status status = tmp_dirs[i]->VerifyAndCreate(
        metrics, &is_tmp_dir_on_disk, need_local_buffer_dir, this);
    if (!status.ok()) {
      // If the remote directory fails to verify or create, return the error.
      if (!tmp_dirs[i]->is_local()) return status;
      // If it is the local directory, continue to try next directory.
      continue;
    }
    if (tmp_dirs[i]->is_local()) {
      if (need_local_buffer_dir) {
        local_buff_dir_ = move(tmp_dirs[i]);
        need_local_buffer_dir = false;
      } else {
        tmp_dirs_.emplace_back(move(tmp_dirs[i]));
      }
    } else {
      tmp_dirs_remote_ = move(tmp_dirs[i]);
    }
  }

  // Sort the tmp directories by priority.
  std::sort(tmp_dirs_.begin(), tmp_dirs_.end(),
      [](const std::unique_ptr<TmpDir>& a, const std::unique_ptr<TmpDir>& b) {
        return a->priority_ < b->priority_;
      });

  if (HasRemoteDir()) {
    if (local_buff_dir_ == nullptr) {
      // Should at least have one local dir for the buffer. Later we might allow to use
      // s3 fast upload directly without a buffer.
      return Status(
          Substitute("No local directory configured for remote scratch space:  $0",
              tmp_dirs_remote_->path_));
    } else {
      LOG(INFO) << "Using scratch directory " << tmp_dirs_remote_->path_ << " limit: "
                << PrettyPrinter::PrintBytes(tmp_dirs_remote_->bytes_limit_);
      IntGauge* bytes_used_metric = metrics->AddGauge(
          SCRATCH_DIR_BYTES_USED_FORMAT, 0, Substitute("$0", tmp_dirs_.size()));
      tmp_dirs_remote_->bytes_used_metric_ = bytes_used_metric;
    }
  }

  DCHECK(metrics != nullptr);
  num_active_scratch_dirs_metric_ =
      metrics->AddGauge(TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS, 0);
  active_scratch_dirs_metric_ = SetMetric<string>::CreateAndRegister(
      metrics, TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST, set<string>());
  if (HasRemoteDir()) {
    num_active_scratch_dirs_metric_->SetValue(tmp_dirs_.size() + 1);
  } else {
    num_active_scratch_dirs_metric_->SetValue(tmp_dirs_.size());
  }
  for (int i = 0; i < tmp_dirs_.size(); ++i) {
    active_scratch_dirs_metric_->Add(tmp_dirs_[i]->path_);
  }
  if (HasRemoteDir()) {
    active_scratch_dirs_metric_->Add(tmp_dirs_remote_->path_);
    RETURN_IF_ERROR(CreateTmpFileBufferPoolThread(metrics));
  }

  scratch_bytes_used_metric_ =
      metrics->AddHWMGauge(TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK,
          TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED, 0);

  scratch_read_memory_buffer_used_metric_ =
      metrics->AddHWMGauge(TMP_FILE_MGR_SCRATCH_READ_MEMORY_BUFFER_USED_HIGH_WATER_MARK,
          TMP_FILE_MGR_SCRATCH_READ_MEMORY_BUFFER_USED, 0);

  initialized_ = true;

  if ((tmp_dirs_.empty() && local_buff_dir_ == nullptr) && !tmp_dirs.empty()) {
    LOG(ERROR) << "Running without spill to disk: could not use any scratch "
               << "directories in list: " << join(tmp_dir_specifiers, ",")
               << ". See previous warnings for information on causes.";
  }
  return Status::OK();
}

Status TmpFileMgr::CreateTmpFileBufferPoolThread(MetricGroup* metrics) {
  DCHECK(metrics != nullptr);
  tmp_dirs_remote_ctrl_.tmp_file_pool_.reset(new TmpFileBufferPool(this));
  std::unique_ptr<Thread> t;
  RETURN_IF_ERROR(Thread::Create("tmp-file-pool", "work-loop(TmpFileSpaceReserve Worker)",
      &TmpFileBufferPool::TmpFileSpaceReserveThreadLoop,
      tmp_dirs_remote_ctrl_.tmp_file_pool_.get(), &t));
  tmp_dirs_remote_ctrl_.tmp_file_mgr_thread_group_.AddThread(move(t));
  int64_t ONE_HOUR_IN_NS = 60L * 60L * NANOS_PER_SEC;
  tmp_dirs_remote_ctrl_.tmp_file_pool_->dequeue_timer_metric_ =
      metrics->RegisterMetric(new HistogramMetric(
          MetricDefs::Get(TMP_FILE_BUFF_POOL_DEQUEUE_DURATIONS), ONE_HOUR_IN_NS, 3));
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
  path new_file_path(tmp_dirs_[device_id]->path_);
  new_file_path /= file_name.str();

  new_file->reset(new TmpFileLocal(file_group, device_id, new_file_path.string()));
}

void TmpFileMgr::RemoveRemoteDir(TmpFileGroup* file_group, DeviceId device_id) {
  if (tmp_dirs_remote_ == nullptr) return;
  string dir = tmp_dirs_remote_->path_;
  stringstream files_dir;
  files_dir << dir << "/" << PrintId(ExecEnv::GetInstance()->backend_id(), "_") << "_"
            << PrintId(file_group->unique_id(), "_");
  hdfsFS hdfs_conn;
  Status status =
      HdfsFsCache::instance()->GetConnection(files_dir.str(), &hdfs_conn, &hdfs_conns_);
  if (status.ok()) {
    DCHECK(hdfs_conn != nullptr);
    hdfsDelete(hdfs_conn, files_dir.str().c_str(), 1);
  } else {
    LOG(WARNING) << "Failed to remove the remote directory because unable to create a "
                    "connection to "
                 << files_dir.str();
  }
}

Status TmpFileMgr::AsyncWriteRange(WriteRange* write_range, TmpFile* tmp_file) {
  if (write_range->disk_file()->disk_type() == io::DiskFileType::LOCAL) {
    DCHECK(write_range != nullptr);
    DCHECK(write_range->io_ctx() != nullptr);
    return write_range->io_ctx()->AddWriteRange(write_range);
  }
  // If spill to a remote directory, TmpFileBufferPool is helping to send the writes to
  // the DiskQueue because the local buffer for the remote file may be used up and it may
  // need to wait in the pool before the writes can be sent to the DiskQueue.
  DCHECK(tmp_dirs_remote_ctrl_.tmp_file_pool_ != nullptr);
  DCHECK(tmp_file != nullptr);
  return tmp_dirs_remote_ctrl_.tmp_file_pool_->EnqueueWriteRange(write_range, tmp_file);
}

void TmpFileMgr::EnqueueTmpFilesPoolDummyFile() {
  EnqueueTmpFilesPool(tmp_dirs_remote_ctrl_.tmp_file_pool_->tmp_file_dummy_, true);
}

void TmpFileMgr::EnqueueTmpFilesPool(shared_ptr<TmpFile>& tmp_file, bool front) {
  tmp_dirs_remote_ctrl_.tmp_file_pool_->EnqueueTmpFilesPool(tmp_file, front);
}

Status TmpFileMgr::DequeueTmpFilesPool(shared_ptr<TmpFile>* tmp_file, bool quick_return) {
  return tmp_dirs_remote_ctrl_.tmp_file_pool_->DequeueTmpFilesPool(
      tmp_file, quick_return);
}

void TmpFileMgr::ReleaseTmpFileReadBuffer(
    const unique_lock<shared_mutex>& file_lock, TmpFile* tmp_file) {
  DCHECK(tmp_file != nullptr);
  DCHECK(IsRemoteBatchReadingEnabled());
  TmpFileRemote* tmp_file_remote = static_cast<TmpFileRemote*>(tmp_file);
  for (int i = 0; i < GetNumReadBuffersPerFile(); i++) {
    tmp_file_remote->TryDeleteReadBuffer(file_lock, i);
  }
}

Status TmpFileMgr::TryEvictFile(TmpFile* tmp_file) {
  DCHECK(tmp_file != nullptr);
  if (tmp_file->disk_type() == io::DiskFileType::DUMMY) return Status::OK();

  TmpFileRemote* tmp_file_remote = static_cast<TmpFileRemote*>(tmp_file);
  DiskFile* buffer_file = tmp_file_remote->DiskBufferFile();

  // Remove the buffer of the TmpFile.
  // After deletion of the buffer, if the TmpFile doesn't exist in the remote file system
  // either, that means the TmpFile shared pointer can be removed from the TmpFileMgr,
  // because in this case, the physical file is considered no longer in the system.
  // Hold the unique locks of the files during the deletion.
  Status status = Status::OK();
  {
    unique_lock<shared_mutex> buffer_lock(buffer_file->physical_file_lock_);
    if (buffer_file->GetFileStatus() == io::DiskFileStatus::PERSISTED) {
      status = buffer_file->Delete(buffer_lock);
    }
  }
  return status;
}

Status TmpFileMgr::ReserveLocalBufferSpace(bool quick_return) {
  int64_t file_size = GetRemoteTmpFileSize();

  // The high water mark is used to record the total bytes which have been assigned, we
  // can assume that all the assigned bytes will be finally returned to the pool.
  // Before the high water mark reaches the bytes limit of the local buffer directory,
  // the caller can gain space freely. But if the high water mark is over the bytes limit,
  // the caller needs to gain space from the pool because all the available spaces are in
  // the pool now.
  TmpDir* dir = local_buff_dir_.get();
  if (tmp_dirs_remote_ctrl_.local_buff_dir_bytes_high_water_mark_.Add(file_size)
      > dir->bytes_limit_) {
    tmp_dirs_remote_ctrl_.local_buff_dir_bytes_high_water_mark_.Add(-file_size);
  } else {
    GetLocalBufferDir()->bytes_used_metric_->Increment(file_size);
    return Status::OK();
  }

  shared_ptr<TmpFile> tmp_file;
  // If all of the space of the buffer directory has been assigned, gain a file which
  // is available to be evicted from the TmpFileBufferPool. It can be a long wait if
  // quick return is not set and there is no available file in the pool.
  Status status = DequeueTmpFilesPool(&tmp_file, quick_return);
  if (!status.ok()) {
    DCHECK(tmp_file == nullptr);
    return status;
  }
  // Evict the file to release the physical space.
  // If error happens during eviction, we log an warning, and return status ok instead to
  // keep the caller doing the writing since probably the physical file is already
  // deleted.
  status = TryEvictFile(tmp_file.get());
  if (!status.ok()) {
    LOG(WARNING) << "File Eviction Failed: " << tmp_file->GetWriteFile()->path();
  }
  return Status::OK();
}

TmpDir* TmpFileMgr::GetLocalBufferDir() const {
  return local_buff_dir_.get();
}

int TmpFileMgr::NumActiveTmpDevicesLocal() {
  DCHECK(initialized_);
  return tmp_dirs_.size();
}

int TmpFileMgr::NumActiveTmpDevices() {
  DCHECK(initialized_);
  return tmp_dirs_.size() + ((tmp_dirs_remote_ == nullptr) ? 0 : 1);
}

vector<DeviceId> TmpFileMgr::ActiveTmpDevices() {
  vector<DeviceId> devices;
  DeviceId device_id = 0;
  for (; device_id < tmp_dirs_.size(); ++device_id) {
    devices.push_back(device_id);
  }
  if (tmp_dirs_remote_ != nullptr) {
    devices.push_back(device_id);
  }
  return devices;
}

string TmpFileMgr::GetTmpDirPath(DeviceId device_id) const {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size() + ((tmp_dirs_remote_ == nullptr) ? 0 : 1));
  if (device_id < tmp_dirs_.size()) {
    return tmp_dirs_[device_id]->path_;
  } else {
    return tmp_dirs_remote_->path_;
  }
}

int64_t TmpFileMgr::TmpDirRemoteCtrl::CalcMaxReadBufferBytes() {
  int64_t max_allow_bytes = 0;
  int64_t process_bytes_limit;
  int64_t total_avail_mem;
  if (!ChooseProcessMemLimit(&process_bytes_limit, &total_avail_mem).ok()) {
    // Return 0 to disable read buffer if unable to get the process and system limit.
    return max_allow_bytes;
  }
  DCHECK_GE(total_avail_mem, process_bytes_limit);
  // Only allows the read buffer if the memory not being used is larger than
  // REMOTE_READ_BUFFER_DISABLE_THRESHOLD_PERCENT of the total memory.
  if ((total_avail_mem - process_bytes_limit)
      > total_avail_mem * REMOTE_READ_BUFFER_DISABLE_THRESHOLD_PERCENT) {
    // Max allowed bytes are the minimum of REMOTE_READ_BUFFER_MAX_MEM_PERCENT of the
    // total memory and REMOTE_READ_BUFFER_MEM_HARD_LIMIT_PERCENT of the unused memory.
    max_allow_bytes = min((total_avail_mem - process_bytes_limit)
            * REMOTE_READ_BUFFER_MEM_HARD_LIMIT_PERCENT,
        total_avail_mem * REMOTE_READ_BUFFER_MAX_MEM_PERCENT);
  }
  return max_allow_bytes;
}

Status TmpFileMgr::TmpDirRemoteCtrl::SetUpReadBufferParams() {
  bool is_percent;
  // If the temporary file size is smaller than the max block size, set the block size
  // as the file size
  read_buffer_block_size_ =
      remote_tmp_file_size_ < MAX_REMOTE_READ_MEM_BLOCK_THRESHOLD_BYTES ?
      remote_tmp_file_size_ :
      MAX_REMOTE_READ_MEM_BLOCK_THRESHOLD_BYTES;
  num_read_buffer_blocks_per_file_ =
      static_cast<int>(remote_tmp_file_size_ / read_buffer_block_size_);
  max_read_buffer_size_ =
      ParseUtil::ParseMemSpec(FLAGS_remote_read_memory_buffer_size, &is_percent, 0);
  if (max_read_buffer_size_ <= 0) {
    return Status(Substitute("Invalid value of remote_read_memory_buffer_size '$0'",
        FLAGS_remote_read_memory_buffer_size));
  }
  // Calculate the max allowed bytes for the read buffer.
  int64_t max_allow_bytes = CalcMaxReadBufferBytes();
  DCHECK_GE(max_allow_bytes, 0);
  if (max_read_buffer_size_ > max_allow_bytes) {
    max_read_buffer_size_ = max_allow_bytes;
    LOG(WARNING) << "The remote read memory buffer size exceeds the maximum "
                    "allowed and is reduced to "
                 << max_allow_bytes << " bytes.";
  }
  LOG(INFO) << "Using " << max_read_buffer_size_
            << " bytes for the batch reading buffer of TmpFileMgr.";
  return Status::OK();
}

Status TmpDir::ParseByteLimit(const string& byte_limit) {
  bool is_percent;
  bytes_limit_ = ParseUtil::ParseMemSpec(byte_limit, &is_percent, 0);
  if (bytes_limit_ < 0 || is_percent) {
    return Status(Substitute(
        "Malformed scratch directory capacity configuration '$0'", raw_path_));
  } else if (bytes_limit_ == 0) {
    // Interpret -1, 0 or empty string as no limit.
    bytes_limit_ = numeric_limits<int64_t>::max();
  }
  return Status::OK();
}

Status TmpDir::ParsePriority(const string& priority) {
  if (!priority.empty()) {
    StringParser::ParseResult result;
    priority_ = StringParser::StringToInt<int>(
        priority.c_str(), priority.size(), &result);
    if (result != StringParser::PARSE_SUCCESS) {
      return Status(Substitute(
          "Malformed scratch directory priority configuration '$0'", raw_path_));
    }
  }
  return Status::OK();
}

Status TmpDir::Parse() {
  DCHECK(parsed_raw_path_.empty() && path_.empty());

  vector<string> toks;
  RETURN_IF_ERROR(ParsePathTokens(toks));

  constexpr int max_num_tokens = 3;
  if (toks.size() > max_num_tokens) {
    return Status(Substitute(
        "Could not parse temporary dir specifier, too many colons: '$0'", raw_path_));
  }

  // Construct the complete scratch directory path.
  toks[0] = trim_right_copy_if(toks[0], is_any_of("/"));
  parsed_raw_path_ = toks[0];
  path_ = (boost::filesystem::path(toks[0]) / TMP_SUB_DIR_NAME).string();

  // The scratch path may have two options "bytes limit" and "priority".
  // The bytes limit should be the first option.
  if (toks.size() > 1) {
    RETURN_IF_ERROR(ParseByteLimit(toks[1]));
  }
  // The priority should be the second option.
  if (toks.size() > 2) {
    RETURN_IF_ERROR(ParsePriority(toks[2]));
  }
  return Status::OK();
}

Status TmpDirLocal::ParsePathTokens(vector<string>& toks) {
  // The ordinary format of the directory input after split by colon is
  // {path, [bytes_limit, [priority]]}.
  split(toks, raw_path_, is_any_of(":"), token_compress_off);
  toks[0] = absolute(toks[0]).string();
  return Status::OK();
}

Status TmpDirLocal::VerifyAndCreate(MetricGroup* metrics,
    vector<bool>* is_tmp_dir_on_disk, bool need_local_buffer_dir, TmpFileMgr* tmp_mgr) {
  DCHECK(!parsed_raw_path_.empty());
  // The path must be a writable directory.
  Status status = FileSystemUtil::VerifyIsDirectory(parsed_raw_path_);
  if (!status.ok()) {
    LOG(WARNING) << "Cannot use directory " << parsed_raw_path_
                 << " for scratch: " << status.msg().msg();
    return status;
  }

  // Find the disk id of path. Add the scratch directory if there isn't another directory
  // on the same disk (or if we don't know which disk it is on).
  int disk_id = DiskInfo::disk_id(parsed_raw_path_.c_str());
  if (!tmp_mgr->one_dir_per_device_ || disk_id < 0 || !(*is_tmp_dir_on_disk)[disk_id]) {
    uint64_t available_space;
    RETURN_IF_ERROR(
        FileSystemUtil::GetSpaceAvailable(parsed_raw_path_, &available_space));
    if (available_space < AVAILABLE_SPACE_THRESHOLD_MB * 1024 * 1024) {
      LOG(WARNING) << "Filesystem containing scratch directory " << parsed_raw_path_
                   << " has less than " << AVAILABLE_SPACE_THRESHOLD_MB
                   << "MB available.";
    }
    RETURN_IF_ERROR(CreateLocalDirectory(
        metrics, is_tmp_dir_on_disk, need_local_buffer_dir, disk_id, tmp_mgr));
    if (tmp_mgr->punch_holes_) {
      // Make sure hole punching is supported for the directory.
      // IMPALA-9798: this file should *not* be created inside impala-scratch
      // subdirectory to avoid races with multiple impalads starting up.
      RETURN_IF_ERROR(FileSystemUtil::CheckHolePunch(parsed_raw_path_));
    }
  } else {
    return Status(Substitute(
        "The scratch directory $0 is on the same disk with another directory or on "
        "an unknown disk.",
        parsed_raw_path_));
  }
  return Status::OK();
}

void TmpDirLocal::LogScratchLocalDirectoryInfo(bool is_local_buffer_dir, int disk_id) {
  LOG(INFO) << (is_local_buffer_dir ? "Using local buffer directory for scratch space " :
                                      "Using scratch directory ")
            << path_ << " on "
            << "disk " << disk_id << " limit: " << PrettyPrinter::PrintBytes(bytes_limit_)
            << ", priority: " << priority_;
}

Status TmpDirLocal::CreateLocalDirectory(MetricGroup* metrics,
    vector<bool>* is_tmp_dir_on_disk, bool need_local_buffer_dir, int disk_id,
    TmpFileMgr* tmp_mgr) {
  DCHECK(!path_.empty());
  // Create the directory, destroying if already present. If this succeeds, we will
  // have an empty writable scratch directory.
  Status status = FileSystemUtil::RemoveAndCreateDirectory(path_);
  if (status.ok()) {
    if (need_local_buffer_dir) {
      // Add the first local dir as local buffer, the dir is only served as the buffer
      // for spill to remote filesystem. At least we need the dir to have two default
      // file size space.
      if (bytes_limit_ < tmp_mgr->tmp_dirs_remote_ctrl_.remote_tmp_file_size_ * 2) {
        return Status(Substitute(
            "Local buffer directory $0 configured for remote scratch "
            "space has a size limit of $1 bytes, should be at least twice as the "
            "temporary file size "
            "$2 bytes",
            path_, bytes_limit_, tmp_mgr->tmp_dirs_remote_ctrl_.remote_tmp_file_size_));
      }
      bytes_used_metric_ =
          metrics->AddGauge(LOCAL_BUFF_BYTES_USED_FORMAT, 0, Substitute("$0", 0));
      LogScratchLocalDirectoryInfo(true /*is_local_buffer_dir*/, disk_id);
      return Status::OK();
    }
    if (disk_id >= 0) (*is_tmp_dir_on_disk)[disk_id] = true;
    LogScratchLocalDirectoryInfo(false /*is_local_buffer_dir*/, disk_id);
    bytes_used_metric_ = metrics->AddGauge(
        SCRATCH_DIR_BYTES_USED_FORMAT, 0, Substitute("$0", tmp_mgr->tmp_dirs_.size()));
  } else {
    LOG(WARNING) << "Could not remove and recreate directory " << path_
                 << ": cannot use it for scratch. "
                 << "Error was: " << status.msg().msg();
  }
  return status;
}

Status TmpDirS3::ParsePathTokens(vector<string>& toks) {
  // The ordinary format of the directory input after split by colon is
  // {scheme, path, [bytes_limit, [priority]]}. Combine scheme and path.
  split(toks, raw_path_, is_any_of(":"), token_compress_off);
  // Only called on paths starting with `s3a://`, so there will always be at least 2.
  DCHECK(toks.size() >= 2);
  toks[0] = Substitute("$0:$1", toks[0], toks[1]);
  toks.erase(toks.begin()+1);
  return Status::OK();
}

Status TmpDirS3::VerifyAndCreate(MetricGroup* metrics, vector<bool>* is_tmp_dir_on_disk,
    bool need_local_buffer_dir, TmpFileMgr* tmp_mgr) {
  // For the S3 path, it doesn't need to create the directory for the uploading
  // as long as the S3 address is correct.
  DCHECK(!path_.empty());
  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      path_, &hdfs_conn, &(tmp_mgr->hdfs_conns_), tmp_mgr->s3a_options()));
  return Status::OK();
}

Status TmpDirHdfs::ParsePathTokens(vector<string>& toks) {
  // HDFS scratch path can include an optional port number; URI without path and port
  // number is ambiguous so in that case we error. Format after split by colon is
  // {scheme, path, port_num?, [bytes_limit, [priority]]}. Coalesce the URI from tokens.
  split(toks, raw_path_, is_any_of(":"), token_compress_off);
  // Only called on paths starting with `hdfs://` or `ofs://`.
  DCHECK(toks.size() >= 2);
  if (toks[1].rfind('/') > 1) {
    // Contains a slash after the scheme, so port number was omitted.
    toks[0] = Substitute("$0:$1", toks[0], toks[1]);
    toks.erase(toks.begin()+1);
  } else if (toks.size() < 3) {
    return Status(
        Substitute("The scratch URI must have a path or port number: '$0'", raw_path_));
  } else {
    toks[0] = Substitute("$0:$1:$2", toks[0], toks[1], toks[2]);
    toks.erase(toks.begin()+1, toks.begin()+3);
  }
  return Status::OK();
}

Status TmpDirHdfs::VerifyAndCreate(MetricGroup* metrics, vector<bool>* is_tmp_dir_on_disk,
    bool need_local_buffer_dir, TmpFileMgr* tmp_mgr) {
  DCHECK(!path_.empty());
  hdfsFS hdfs_conn;
  // If the HDFS path doesn't exist, it would fail while uploading, so we
  // create the HDFS path if it doesn't exist.
  RETURN_IF_ERROR(
      HdfsFsCache::instance()->GetConnection(path_, &hdfs_conn, &(tmp_mgr->hdfs_conns_)));
  if (hdfsExists(hdfs_conn, path_.c_str()) != 0) {
    if (hdfsCreateDirectory(hdfs_conn, path_.c_str()) != 0) {
      return Status(GetHdfsErrorMsg("HDFS create path failed: ", path_));
    }
  }
  return Status::OK();
}

TmpFile::TmpFile(
    TmpFileGroup* file_group, DeviceId device_id, const string& path, bool expected_local)
  : file_group_(file_group),
    path_(path),
    device_id_(device_id),
    disk_id_(DiskInfo::disk_id(path.c_str())),
    expected_local_(expected_local),
    blacklisted_(false) {}

int TmpFile::AssignDiskQueue(bool is_local_buffer) const {
  // The file paths of TmpFiles are absolute paths, doesn't support default fs.
  if (is_local_buffer) {
    // Assign a disk queue for a local buffer, which is associated with a remote file.
    return file_group_->io_mgr_->AssignQueue(local_buffer_path_.c_str(),
        /* disk_id */ -1, /* expected_local */ true, /* check_default_fs */ false);
  }
  return file_group_->io_mgr_->AssignQueue(
      path_.c_str(), disk_id_, expected_local_, /* check_default_fs */ false);
}

bool TmpFile::Blacklist(const ErrorMsg& msg) {
  LOG(ERROR) << "Error for temporary file '" << path_ << "': " << msg.msg();
  if (!blacklisted_) {
    blacklisted_ = true;
    return true;
  } else {
    return false;
  }
}

TmpDir* TmpFile::GetDir() {
  auto tmp_file_mgr = file_group_->tmp_file_mgr_;
  if (device_id_ >= tmp_file_mgr->tmp_dirs_.size()) {
    // Only one remote directory supported.
    DCHECK(device_id_ - tmp_file_mgr->tmp_dirs_.size() == 0);
    return tmp_file_mgr->tmp_dirs_remote_.get();
  }
  return tmp_file_mgr->tmp_dirs_[device_id_].get();
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
  GetDir()->bytes_used_metric()->Increment(-len);
  VLOG(3) << "Punched hole in " << path_ << " " << offset << " " << len;
  return Status::OK();
}

string TmpFile::DebugString() {
  return Substitute(
      "File $0 path '$1' device id $2 disk id $3 allocation offset $4 blacklisted $5",
      this, path_, device_id_, disk_id_, allocation_offset_, blacklisted_);
}

TmpFileLocal::TmpFileLocal(TmpFileGroup* file_group, TmpFileMgr::DeviceId device_id,
    const std::string& path, bool expected_local)
  : TmpFile(file_group, device_id, path, expected_local) {
  DCHECK(file_group != nullptr);
  disk_file_ = make_unique<io::DiskFile>(path_, file_group->io_mgr_);
  disk_type_ = io::DiskFileType::LOCAL;
}

bool TmpFileLocal::AllocateSpace(int64_t num_bytes, int64_t* offset) {
  DCHECK_GT(num_bytes, 0);
  TmpDir* dir = GetDir();
  // Increment optimistically and roll back if the limit is exceeded.
  if (dir->bytes_used_metric()->Increment(num_bytes) > dir->bytes_limit()) {
    dir->bytes_used_metric()->Increment(-num_bytes);
    return false;
  }
  *offset = allocation_offset_;
  allocation_offset_ += num_bytes;
  return true;
}

io::DiskFile* TmpFileLocal::GetWriteFile() {
  return disk_file_.get();
}

Status TmpFileLocal::Remove() {
  // Remove the file if present (it may not be present if no writes completed).
  Status status = FileSystemUtil::RemovePaths({path_});
  int64_t bytes_in_use = file_group_->tmp_file_mgr_->punch_holes() ?
      allocation_offset_ - bytes_reclaimed_.Load() :
      allocation_offset_;
  GetDir()->bytes_used_metric()->Increment(-bytes_in_use);
  return status;
}

TmpFileRemote::TmpFileRemote(TmpFileGroup* file_group, TmpFileMgr::DeviceId device_id,
    const std::string& path, const std::string& local_buffer_path, bool expected_local,
    const char* hdfs_url)
  : TmpFile(file_group, device_id, path, expected_local) {
  DCHECK(hdfs_url != nullptr);
  hdfs_conn_ = nullptr;
  const HdfsFsCache::HdfsConnOptions* options = nullptr;
  if (IsHdfsPath(hdfs_url, false)) {
    disk_type_ = io::DiskFileType::DFS;
    disk_id_ = file_group->io_mgr_->RemoteDfsDiskId();
    disk_id_file_op_ = file_group->io_mgr_->RemoteDfsDiskFileOperId();
  } else if (IsOzonePath(hdfs_url, false)) {
    disk_type_ = io::DiskFileType::DFS;
    disk_id_ = file_group->io_mgr_->RemoteOzoneDiskId();
    disk_id_file_op_ = file_group->io_mgr_->RemoteDfsDiskFileOperId();
  } else if (IsS3APath(hdfs_url, false)) {
    disk_type_ = io::DiskFileType::S3;
    disk_id_ = file_group->io_mgr_->RemoteS3DiskId();
    disk_id_file_op_ = file_group->io_mgr_->RemoteS3DiskFileOperId();
    options = file_group_->tmp_file_mgr_->s3a_options();
  }
  Status status = HdfsFsCache::instance()->GetConnection(
      hdfs_url, &hdfs_conn_, &file_group_->tmp_file_mgr_->hdfs_conns_, options);
  file_size_ = file_group_->tmp_file_mgr_->GetRemoteTmpFileSize();
  local_buffer_path_ = local_buffer_path;
  disk_file_ = make_unique<io::DiskFile>(path_, file_group->io_mgr_,
      file_group_->tmp_file_mgr_->GetRemoteTmpFileSize(), disk_type_, &hdfs_conn_);
  if (file_group_->tmp_file_mgr_->IsRemoteBatchReadingEnabled()) {
    read_buffer_block_size_ = file_group_->tmp_file_mgr_->GetReadBufferBlockSize();
    int num_of_read_buffers = file_group_->tmp_file_mgr_->GetNumReadBuffersPerFile();
    disk_buffer_file_ = make_unique<io::DiskFile>(local_buffer_path_,
        file_group_->io_mgr_, file_group_->tmp_file_mgr_->GetRemoteTmpFileSize(),
        io::DiskFileType::LOCAL_BUFFER, read_buffer_block_size_, num_of_read_buffers);
    disk_read_page_cnts_ = std::make_unique<int64_t[]>(num_of_read_buffers);
    DCHECK(disk_read_page_cnts_.get() != nullptr);
    memset(disk_read_page_cnts_.get(), 0, num_of_read_buffers * sizeof(int64_t));
    for (int i = 0; i < num_of_read_buffers; i++) {
      fetch_ranges_.emplace_back(nullptr);
    }
  } else {
    disk_buffer_file_ = make_unique<io::DiskFile>(local_buffer_path_,
        file_group_->io_mgr_, file_group_->tmp_file_mgr_->GetRemoteTmpFileSize(),
        io::DiskFileType::LOCAL_BUFFER);
  }
}

TmpFileRemote::~TmpFileRemote() {
  // Need to return the buffer before deconstruction if buffer space is reserved.
  if (DiskBufferFile()->IsSpaceReserved()) DCHECK(is_buffer_returned());
}

bool TmpFileRemote::AllocateSpace(int64_t num_bytes, int64_t* offset) {
  DCHECK_GT(num_bytes, 0);
  if (at_capacity_) return false;
  *offset = allocation_offset_;
  allocation_offset_ += num_bytes;
  // The actual size could be a little over the file size.
  if (allocation_offset_ >= file_size_) {
    // Set the actual file size of the disk file for the use of writing.
    GetWriteFile()->SetActualFileSize(allocation_offset_);
    at_capacity_ = true;
  }
  return true;
}

io::DiskFile* TmpFileRemote::GetWriteFile() {
  return disk_buffer_file_.get();
}

int TmpFileRemote::GetReadBufferIndex(int64_t offset) {
  DCHECK(disk_buffer_file_ != nullptr);
  return disk_buffer_file_->GetReadBufferIndex(offset);
}

void TmpFileRemote::AsyncFetchReadBufferBlock(io::DiskFile* read_buffer_file,
    io::MemBlock* read_buffer_block, int read_buffer_idx, bool* fetched) {
  DCHECK(fetched != nullptr);
  *fetched = false;
  {
    shared_lock<shared_mutex> read_file_lock(*(read_buffer_file->GetFileLock()));
    unique_lock<SpinLock> mem_bloc_lock(*(read_buffer_block->GetLock()));
    // Check the block status.
    // If the block is disabled, the caller won't be able to use this buffer block.
    // If the block is written, the block is already fetched, set the fetched flag and
    // return immediately.
    // If the block is uninitialized, we will fetch the block immediately but without
    // waiting for the fetch, so that it won't block the current page reading.
    // If the block is in reserved or alloc status, means one other thread is handling
    // the block, here we don't wait because the blocking could be expensive.
    if (read_buffer_file->IsReadBufferBlockStatus(read_buffer_block,
            io::MemBlockStatus::DISABLED, read_file_lock, &mem_bloc_lock)) {
      return;
    } else if (read_buffer_file->IsReadBufferBlockStatus(read_buffer_block,
                   io::MemBlockStatus::WRITTEN, read_file_lock, &mem_bloc_lock)) {
      *fetched = true;
      return;
    } else if (read_buffer_file->IsReadBufferBlockStatus(read_buffer_block,
                   io::MemBlockStatus::UNINIT, read_file_lock, &mem_bloc_lock)) {
      bool dofetch = true;
      int64_t mem_size_limit =
          file_group_->tmp_file_mgr()->GetRemoteMaxTotalReadBufferSize();
      auto read_mem_counter =
          file_group_->tmp_file_mgr()->scratch_read_memory_buffer_used_metric_;
      if (read_mem_counter->Increment(read_buffer_file->read_buffer_block_size())
          > mem_size_limit) {
        read_mem_counter->Increment(-1 * read_buffer_file->read_buffer_block_size());
        dofetch = false;
      }
      if (dofetch) {
        read_buffer_file->SetReadBufferBlockStatus(read_buffer_block,
            io::MemBlockStatus::RESERVED, read_file_lock, &mem_bloc_lock);
        RemoteOperRange::RemoteOperDoneCallback fetch_callback =
            [read_buffer_block, tmp_file = this](const Status& fetch_status) {
              if (!fetch_status.ok()) {
                // Disable the read buffer if fails to fetch.
                tmp_file->TryDeleteReadBufferExcl(read_buffer_block->block_id());
              }
            };
        fetch_ranges_[read_buffer_idx].reset(new RemoteOperRange(disk_file_.get(),
            read_buffer_file, file_group_->tmp_file_mgr()->GetRemoteTmpBlockSize(),
            disk_id(true), RequestType::FILE_FETCH, file_group_->io_mgr_, fetch_callback,
            GetReadBuffStartOffset(read_buffer_idx)));
        Status add_status = file_group_->io_ctx_->AddRemoteOperRange(
            fetch_ranges_[read_buffer_idx].get());
        if (!add_status.ok()) {
          read_buffer_file->SetReadBufferBlockStatus(read_buffer_block,
              io::MemBlockStatus::DISABLED, read_file_lock, &mem_bloc_lock);
        }
      } else {
        read_buffer_file->SetReadBufferBlockStatus(read_buffer_block,
            io::MemBlockStatus::DISABLED, read_file_lock, &mem_bloc_lock);
      }
    }
  }
  *fetched = true;
  return;
}

io::DiskFile* TmpFileRemote::GetReadBufferFile(int64_t offset) {
  // If the local buffer file exists, return the file directly.
  // If it is deleted (probably due to eviction), and batch reading is enabled, would
  // try to fetch the current block asynchronously if it is not present in the memory
  // buffer.
  // If the local buffer file is deleted and the read memory buffer doesn't have the
  // block right now, then return a nullptr to indicate there is no buffer available.
  io::DiskFile* read_buffer_file = disk_buffer_file_.get();
  if (disk_buffer_file_->GetFileStatus() != io::DiskFileStatus::DELETED) {
    return read_buffer_file;
  }
  if (!file_group_->tmp_file_mgr()->IsRemoteBatchReadingEnabled()) return nullptr;
  int read_buffer_idx = GetReadBufferIndex(offset);
  io::MemBlock* read_buffer_block = disk_buffer_file_->GetBufferBlock(read_buffer_idx);
  bool fetched = false;
  io::MemBlockStatus block_status = read_buffer_block->GetStatus();
  if (block_status == io::MemBlockStatus::DISABLED) {
    // do nothing
  } else if (block_status == io::MemBlockStatus::WRITTEN) {
    fetched = true;
  } else {
    AsyncFetchReadBufferBlock(
        read_buffer_file, read_buffer_block, read_buffer_idx, &fetched);
  }
  return fetched ? read_buffer_file : nullptr;
}

bool TmpFileRemote::IncrementReadPageCount(int buffer_idx) {
  int64_t read_count = 0;
  int64_t total_num = 0;
  DCheckReadBufferIdx(buffer_idx);
  total_num = GetReadBuffPageCount(buffer_idx);
  {
    lock_guard<SpinLock> lock(lock_);
    read_count = ++disk_read_page_cnts_[buffer_idx];
  }
  // Return true if all the pages have been read of the block.
  return read_count == total_num;
}

template <typename T>
void TmpFileRemote::TryDeleteReadBuffer(const T& lock, int buffer_idx) {
  DCheckReadBufferIdx(buffer_idx);
  bool reserved = false;
  bool allocated = false;
  DCHECK(disk_buffer_file_->IsBatchReadEnabled());
  DCHECK(lock.mutex() == disk_buffer_file_->GetFileLock() && lock.owns_lock());
  disk_buffer_file_->DeleteReadBuffer(
      disk_buffer_file_->GetBufferBlock(buffer_idx), &reserved, &allocated, lock);
  if (reserved || allocated) {
    // Because the reservation will increase the current allocated read buffer usage
    // ahead of the real allocation, we need to decrease it if the block is reserved
    // or allocated.
    file_group_->tmp_file_mgr_->scratch_read_memory_buffer_used_metric_->Increment(
        -1 * read_buffer_block_size_);
  }
}

TmpDir* TmpFileRemote::GetLocalBufferDir() const {
  return file_group_->tmp_file_mgr_->GetLocalBufferDir();
}

Status TmpFileRemote::Remove() {
  Status status = Status::OK();
  // If True, we need to enqueue the file back to the pool after deletion.
  bool to_return_the_buffer = false;

  // Set a flag to notify other threads which are holding the file lock to release,
  // since the remove process needs a unique lock, it accelerates acquiring the mutex.
  SetToDeleteFlag();

  {
    // The order of acquiring the lock must be from local to remote to avoid deadlocks.
    unique_lock<shared_mutex> buffer_file_lock(*(disk_buffer_file_->GetFileLock()));
    unique_lock<shared_mutex> file_lock(*(disk_file_->GetFileLock()));

    // Delete the local buffer file if exists.
    if (disk_buffer_file_->GetFileStatus() != io::DiskFileStatus::DELETED) {
      status = disk_buffer_file_->Delete(buffer_file_lock);
      if (!status.ok()) {
        // If the physical file is failed to delete, log a warning, and set a deleted flag
        // anyway.
        LOG(WARNING) << "Delete file: " << disk_buffer_file_->path() << " failed.";
        disk_buffer_file_->SetStatus(io::DiskFileStatus::DELETED);
      } else if (disk_file_->GetFileStatus() != io::DiskFileStatus::PERSISTED
          && disk_buffer_file_->IsSpaceReserved()) {
        // If the file is not uploaded and the buffer space is reserved, we need to return
        // the buffer to the pool after deletion of the TmpFile. The buffer of a uploaded
        // file should have been returned to the pool after upload operation completes.
        to_return_the_buffer = true;
      } else {
        // Do nothing.
      }
    }

    // Set the remote file status to deleted. The physical remote files would be deleted
    // during deconstruction of TmpFileGroup by deleting the entire remote
    // directory for efficiency consideration.
    disk_file_->SetStatus(io::DiskFileStatus::DELETED);

    // Try to delete all the read buffers.
    if (file_group_->tmp_file_mgr_->IsRemoteBatchReadingEnabled()) {
      file_group_->tmp_file_mgr_->ReleaseTmpFileReadBuffer(buffer_file_lock, this);
    }
  }

  // Update the metrics.
  GetDir()->bytes_used_metric()->Increment(-file_size_);

  // Return the file to the pool if it hasn't been enqueued.
  if (to_return_the_buffer) {
    file_group_->tmp_file_mgr()->EnqueueTmpFilesPool(
        file_group_->FindTmpFileSharedPtr(this), true);
  }

  return status;
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
    read_use_mem_counter_(ADD_COUNTER(profile, "ScratchReadsUseMem", TUnit::UNIT)),
    bytes_read_use_mem_counter_(
        ADD_COUNTER(profile, "ScratchBytesReadUseMem", TUnit::BYTES)),
    read_use_local_disk_counter_(
        ADD_COUNTER(profile, "ScratchReadsUseLocalDisk", TUnit::UNIT)),
    bytes_read_use_local_disk_counter_(
        ADD_COUNTER(profile, "ScratchBytesReadUseLocalDisk", TUnit::BYTES)),
    scratch_space_bytes_used_counter_(
        ADD_COUNTER(profile, "ScratchFileUsedBytes", TUnit::BYTES)),
    disk_read_timer_(ADD_TIMER(profile, "TotalReadBlockTime")),
    encryption_timer_(ADD_TIMER(profile, "TotalEncryptionTime")),
    compression_timer_(tmp_file_mgr->compression_enabled() ?
            ADD_TIMER(profile, "TotalCompressionTime") :
            nullptr),
    num_blacklisted_files_(0),
    spilling_disk_faulty_(false),
    current_bytes_allocated_(0),
    current_bytes_allocated_remote_(0),
    next_allocation_index_(0),
    free_ranges_(64) {
  DCHECK(tmp_file_mgr != nullptr);
  io_ctx_ = io_mgr_->RegisterContext();
  io_ctx_->set_read_use_mem_counter(read_use_mem_counter_);
  io_ctx_->set_bytes_read_use_mem_counter(bytes_read_use_mem_counter_);
  io_ctx_->set_read_use_local_disk_counter(read_use_local_disk_counter_);
  io_ctx_->set_bytes_read_use_local_disk_counter(bytes_read_use_local_disk_counter_);
  // Populate the priority based index ranges.
  const std::vector<std::unique_ptr<TmpDir>>& tmp_dirs = tmp_file_mgr_->tmp_dirs_;
  if (tmp_dirs.size() > 0) {
    int start_index = 0;
    int priority = tmp_dirs[0]->priority();
    for (int i = 0; i < tmp_dirs.size() - 1; ++i) {
      priority = tmp_dirs[i]->priority();
      const int next_priority = tmp_dirs[i + 1]->priority();
      if (next_priority != priority) {
        tmp_files_index_range_.emplace(priority, TmpFileIndexRange(start_index, i));
        start_index = i + 1;
        priority = next_priority;
      }
    }
    tmp_files_index_range_.emplace(priority,
      TmpFileIndexRange(start_index, tmp_dirs.size() - 1));
  }
}

TmpFileGroup::~TmpFileGroup() {
  DCHECK_EQ(tmp_files_.size(), 0);
}

Status TmpFileGroup::CreateFiles() {
  lock_.DCheckLocked();
  DCHECK(tmp_files_.empty());
  vector<DeviceId> tmp_devices = tmp_file_mgr_->ActiveTmpDevices();
  DCHECK(tmp_file_mgr_->NumActiveTmpDevicesLocal() <= tmp_devices.size());
  int files_allocated = 0;
  // Initialize the tmp files and the initial file to use.
  for (int i = 0; i < tmp_file_mgr_->NumActiveTmpDevicesLocal(); ++i) {
    DeviceId device_id = tmp_devices[i];
    unique_ptr<TmpFile> tmp_file;
    tmp_file_mgr_->NewFile(this, device_id, &tmp_file);
    tmp_files_.emplace_back(std::move(tmp_file));
    ++files_allocated;
  }
  DCHECK_EQ(tmp_file_mgr_->NumActiveTmpDevicesLocal(), files_allocated);
  DCHECK_EQ(tmp_file_mgr_->NumActiveTmpDevicesLocal(), tmp_files_.size());
  if (tmp_files_.size() == 0) return ScratchAllocationFailedStatus({});
  // Initialize the next allocation index for each priority.
  for (const auto& entry: tmp_files_index_range_) {
    const int priority = entry.first;
    const int start = entry.second.start;
    const int end = entry.second.end;
    // Start allocating on a random device to avoid overloading the first device.
    next_allocation_index_.emplace(priority, start + rand() % (end - start + 1));
  }
  return Status::OK();
}

template <typename T>
void TmpFileGroup::CloseInternal(vector<T>& tmp_files) {
  for (auto& file : tmp_files) {
    Status status = file->Remove();
    if (!status.ok()) {
      LOG(WARNING) << "Error removing scratch file '" << file->path()
                   << "': " << status.msg().msg();
    }
  }
  tmp_files.clear();
}

void TmpFileGroup::Close() {
  // Cancel writes before deleting the files, since in-flight writes could re-create
  // deleted files.
  if (io_ctx_ != nullptr) {
    if (tmp_file_mgr_->HasRemoteDir()) {
      // Remove all the writes using the io_ctx and waiting for buffer reservation in
      // the pool.
      DCHECK(tmp_file_mgr_->tmp_dirs_remote_ctrl_.tmp_file_pool_ != nullptr);
      tmp_file_mgr_->tmp_dirs_remote_ctrl_.tmp_file_pool_->RemoveWriteRanges(
          io_ctx_.get());
    }
    io_mgr_->UnregisterContext(io_ctx_.get());
  }
  CloseInternal<std::unique_ptr<TmpFile>>(tmp_files_);
  CloseInternal<std::shared_ptr<TmpFile>>(tmp_files_remote_);
  tmp_file_mgr_->RemoveRemoteDir(this, 0);
  tmp_file_mgr_->scratch_bytes_used_metric_->Increment(
      -1 * scratch_space_bytes_used_counter_->value());
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

void TmpFileGroup::UpdateScratchSpaceMetrics(int64_t num_bytes, bool is_remote) {
  scratch_space_bytes_used_counter_->Add(num_bytes);
  tmp_file_mgr_->scratch_bytes_used_metric_->Increment(num_bytes);
  current_bytes_allocated_.Add(num_bytes);
  if (is_remote) current_bytes_allocated_remote_.Add(num_bytes);
}

string TmpFileGroup::GenerateNewPath(const string& dir, const string& unique_name) {
  stringstream file_name;
  file_name << TMP_SUB_DIR_NAME << "-" << unique_name;
  path new_file_path(dir);
  new_file_path /= file_name.str();
  return new_file_path.string();
}

std::shared_ptr<TmpFile>& TmpFileGroup::FindTmpFileSharedPtr(TmpFile* tmp_file) {
  DCHECK(tmp_file != nullptr);
  DCHECK(tmp_file->DiskFile()->disk_type() != io::DiskFileType::LOCAL);
  lock_guard<SpinLock> lock(tmp_files_remote_ptrs_lock_);
  auto shared_file_it = tmp_files_remote_ptrs_.find(tmp_file);
  DCHECK(shared_file_it != tmp_files_remote_ptrs_.end());
  return shared_file_it->second;
}

Status TmpFileGroup::AllocateRemoteSpace(int64_t num_bytes, TmpFile** tmp_file,
    int64_t* file_offset, vector<int>* at_capacity_dirs) {
  // Only one remote dir supported currently.
  string dir = tmp_file_mgr_->tmp_dirs_remote_->path();
  // It is not supposed to have a remote directory other than HDFS, Ozone, or S3.
  DCHECK(IsHdfsPath(dir.c_str(), false) || IsOzonePath(dir.c_str(), false)
      || IsS3APath(dir.c_str(), false));

  // Look for the space from a previous created file.
  if (!tmp_files_remote_.empty()) {
    TmpFile* tmp_file_cur = tmp_files_remote_.back().get();
    // If the file is blocklisted or is at capacity, we will create a new file instead.
    if (!tmp_file_cur->is_blacklisted()) {
      if (tmp_file_cur->AllocateSpace(num_bytes, file_offset)) {
        *tmp_file = tmp_file_cur;
        return Status::OK();
      }
    }
  }

  // Return an error if the new bytes is over the bytes limit of the query or the remote
  // directory.
  int64_t new_bytes =
      current_bytes_allocated_.Load() + tmp_file_mgr_->GetRemoteTmpFileSize();
  if (bytes_limit_ != -1 && new_bytes > bytes_limit_) {
    return Status(TErrorCode::SCRATCH_LIMIT_EXCEEDED, bytes_limit_, GetBackendString());
  }

  int64_t remote_dir_bytes_limit = tmp_file_mgr_->tmp_dirs_remote_->bytes_limit();
  if (remote_dir_bytes_limit != -1 && new_bytes > remote_dir_bytes_limit) {
    return Status(
        TErrorCode::SCRATCH_LIMIT_EXCEEDED, remote_dir_bytes_limit, GetBackendString());
  }

  // The device id of remote directory is defined as the max local device id
  // plus the index of the remote dir. Since we only support one remote dir now,
  // the id is the max local device id plus one.
  DeviceId dev_id = tmp_file_mgr_->tmp_dirs_.size();
  string unique_name = lexical_cast<string>(random_generator()());
  stringstream file_name;
  dir = dir + "/" + PrintId(ExecEnv::GetInstance()->backend_id(), "_") + "_"
      + PrintId(unique_id(), "_");

  string new_file_path = GenerateNewPath(dir, unique_name);
  const string& local_buffer_dir = tmp_file_mgr_->local_buff_dir_->path();
  string new_file_path_local = GenerateNewPath(local_buffer_dir, unique_name);

  TmpFileRemote* tmp_file_r = new TmpFileRemote(
      this, dev_id, new_file_path, new_file_path_local, false, dir.c_str());
  if (tmp_file_r == nullptr) {
    return Status("Failed to allocate temporary file object.");
  }
  if (tmp_file_r->hdfs_conn_ == nullptr) {
    return Status(Substitute("Failed to connect to FS: $0.", dir));
  }
  shared_ptr<TmpFile> tmp_file_remote(move(tmp_file_r));
  int64_t file_size = tmp_file_mgr_->GetRemoteTmpFileSize();
  TmpDir* tmp_dir_remote = tmp_file_remote->GetDir();
  if (tmp_dir_remote->bytes_limit() != -1
      && tmp_dir_remote->bytes_used_metric()->Increment(file_size)
          > tmp_dir_remote->bytes_limit()) {
    tmp_dir_remote->bytes_used_metric()->Increment(-file_size);
    at_capacity_dirs->push_back(dev_id);
    return Status(Substitute("Reach the size limit $0 of dir: $1",
        tmp_dir_remote->bytes_limit(), tmp_dir_remote->path()));
  }
  UpdateScratchSpaceMetrics(file_size, true);
  tmp_files_remote_.emplace_back(move(tmp_file_remote));
  *tmp_file = tmp_files_remote_.back().get();
  // It should be a successful return to allocate the first range from the new file.
  if (!(*tmp_file)->AllocateSpace(num_bytes, file_offset)) {
    DCHECK(false) << "Should be a successful allocation for the first write range.";
  }
  DCHECK_EQ(*file_offset, 0);
  {
    lock_guard<SpinLock> lock(tmp_files_remote_ptrs_lock_);
    tmp_files_remote_ptrs_.emplace(*tmp_file, tmp_files_remote_.back());
  }

  // Try to reserve the space for local buffer with a quick return to avoid
  // a long wait, if failed, caller should do the reservation for the buffer.
  Status reserve_status = tmp_file_mgr_->ReserveLocalBufferSpace(true);
  if (reserve_status.ok()) (*tmp_file)->GetWriteFile()->SetSpaceReserved();

  return Status::OK();
}

Status TmpFileGroup::AllocateLocalSpace(int64_t num_bytes, TmpFile** tmp_file,
    int64_t* file_offset, vector<int>* at_capacity_dirs, bool* alloc_full) {
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
      && current_bytes_allocated_.Load() + scratch_range_bytes > bytes_limit_) {
    return Status(TErrorCode::SCRATCH_LIMIT_EXCEEDED, bytes_limit_, GetBackendString());
  }

  // Lazily create the files on the first write.
  if (tmp_files_.empty()) RETURN_IF_ERROR(CreateFiles());

  // Find the next physical file in priority based round-robin order and allocate a range
  // from it.
  for (const auto& entry: tmp_files_index_range_) {
    const int priority = entry.first;
    const int start = entry.second.start;
    const int end = entry.second.end;
    DCHECK (0 <= start && start <= end && end < tmp_files_.size())
      << "Invalid index range: [" << start << ", " << end << "] "
      << "tmp_files_.size(): " << tmp_files_.size();
    for (int index = start; index <= end; ++index) {
      const int idx = next_allocation_index_[priority];
      next_allocation_index_[priority] = start + (idx - start + 1) % (end - start + 1);
      *tmp_file = tmp_files_[idx].get();
      if ((*tmp_file)->is_blacklisted()) continue;
      // Check the per-directory limit.
      if (!(*tmp_file)->AllocateSpace(scratch_range_bytes, file_offset)) {
        at_capacity_dirs->push_back(idx);
        continue;
      }
      UpdateScratchSpaceMetrics(scratch_range_bytes);
      return Status::OK();
    }
  }

  // Using a bool to notify there is no more space left, could cost less overhead than
  // using a Status, because we want the error reporting as fast as possible for the
  // case of mixing use of remote and local scratch space, so that it can keep trying to
  // allocate from the remote after this.
  *alloc_full = true;
  return Status::OK();
}

Status TmpFileGroup::AllocateSpace(
    int64_t num_bytes, TmpFile** tmp_file, int64_t* file_offset) {
  // Since in eviction, it probably waits for the async upload task if it
  // reaches bytes limit, so it can be slow here.
  lock_guard<SpinLock> lock(lock_);

  // Track the indices of any directories where we failed due to capacity. This is
  // required for error reporting if we are totally out of capacity so that it's clear
  // that some disks were at capacity.
  vector<int> at_capacity_dirs;

  if (!tmp_file_mgr_->tmp_dirs_.empty()) {
    // If alloc_full is set true, meaning all of the local directories are at capacity.
    bool alloc_full = false;
    Status status = AllocateLocalSpace(
        num_bytes, tmp_file, file_offset, &at_capacity_dirs, &alloc_full);
    // If the all of the dirs are at capacity, try remote scratch space.
    // Otherwise, return the status (could be an okay or error).
    if (!status.ok() || !alloc_full) return status;
  }

  // If can't find any space locally, allocate from remote scratch space.
  if (tmp_file_mgr_->tmp_dirs_remote_ != nullptr) {
    Status remote_status =
        AllocateRemoteSpace(num_bytes, tmp_file, file_offset, &at_capacity_dirs);
    if (remote_status.ok() || at_capacity_dirs.empty()) return remote_status;
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
    current_bytes_allocated_.Add(-space_used_bytes);
  } else {
    // For the remote files, we don't recycle the file and range because the remote file
    // is not allowed to in-place modification.
    if (file->DiskFile()->disk_type() == io::DiskFileType::LOCAL) {
      int free_ranges_idx = BitUtil::Log2Ceiling64(space_used_bytes);
      lock_guard<SpinLock> lock(lock_);
      free_ranges_[free_ranges_idx].emplace_back(file, handle->write_range_->offset());
    }
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
  int64_t offset = handle->write_range_->offset();
  if (handle->file_ != nullptr && !handle->file_->is_local()) {
    TmpFileRemote* tmp_file = static_cast<TmpFileRemote*>(handle->file_);
    DiskFile* local_read_buffer_file = tmp_file->GetReadBufferFile(offset);
    DiskFile* remote_file = tmp_file->DiskFile();
    // Reset the read_range, use the remote filesystem's disk id.
    handle->read_range_->Reset(
        ScanRange::FileInfo{
            remote_file->path().c_str(), tmp_file->hdfs_conn_, tmp_file->mtime_},
        handle->write_range_->len(), offset, tmp_file->disk_id(), false,
        BufferOpts::ReadInto(
            read_buffer.data(), read_buffer.len(), BufferOpts::NO_CACHING),
        nullptr, remote_file, local_read_buffer_file);
  } else {
    // Read from local.
    handle->read_range_->Reset(
        ScanRange::FileInfo{handle->write_range_->file()},
        handle->write_range_->len(), offset, handle->write_range_->disk_id(), false,
        BufferOpts::ReadInto(
            read_buffer.data(), read_buffer.len(), BufferOpts::NO_CACHING));
  }

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
  if (handle->file_ != nullptr && !handle->file_->is_local()) {
    auto tmp_file = static_cast<TmpFileRemote*>(handle->file_);
    // If all the pages of specific read buffer have been read, try delete the read
    // buffer.
    if (tmp_file_mgr()->IsRemoteBatchReadingEnabled()) {
      int buffer_idx = tmp_file->GetReadBufferIndex(handle->write_range_->offset());
      bool all_read = tmp_file->IncrementReadPageCount(buffer_idx);
      if (all_read) tmp_file->TryDeleteMemReadBufferShared(buffer_idx);
    }
  }
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
  // Debug action for simulating disk write error. To use, specify in query options as:
  // 'debug_action': 'IMPALA_TMP_FILE_WRITE:<hostname>:<port>:<action>'
  // where <hostname> and <port> represent the impalad which execute the fragment
  // instances, <port> is the BE krpc port (default 27000).
  const Status* p_write_status = &write_status;
  Status debug_status = DebugAction(debug_action_, "IMPALA_TMP_FILE_WRITE",
      {ExecEnv::GetInstance()->krpc_address().hostname(),
          SimpleItoa(ExecEnv::GetInstance()->krpc_address().port())});
  if (UNLIKELY(!debug_status.ok())) p_write_status = &debug_status;

  if (!p_write_status->ok()) {
    status = RecoverWriteError(handle, *p_write_status);
    if (status.ok()) return;
  } else {
    status = *p_write_status;
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

  // We don't recover the errors generated during spilling to a remote file.
  if (handle->file_->disk_type() != io::DiskFileType::LOCAL) {
    return write_status;
  }

  // Save and report the error before retrying so that the failure isn't silent.
  {
    lock_guard<SpinLock> lock(lock_);
    scratch_errors_.push_back(write_status);
    if (handle->file_->Blacklist(write_status.msg())) {
      DCHECK_LT(num_blacklisted_files_, tmp_files_.size());
      ++num_blacklisted_files_;
      if (num_blacklisted_files_ == tmp_files_.size()) {
        // Check if all errors are 'blacklistable'.
        bool are_all_blacklistable_errors = true;
        for (Status& err : scratch_errors_) {
          if (!ErrorConverter::IsBlacklistableError(err)) {
            are_all_blacklistable_errors = false;
            break;
          }
        }
        if (are_all_blacklistable_errors) spilling_disk_faulty_ = true;
      }
    }
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
  for (std::unique_ptr<TmpDir>& tmp_dir : tmp_file_mgr_->tmp_dirs_) {
    tmp_dir_paths.push_back(tmp_dir->path());
  }
  vector<string> at_capacity_dir_paths;
  for (int dir_idx : at_capacity_dirs) {
    if (dir_idx >= tmp_file_mgr_->tmp_dirs_.size()) {
      at_capacity_dir_paths.push_back(tmp_file_mgr_->tmp_dirs_remote_->path());
    } else {
      at_capacity_dir_paths.push_back(tmp_file_mgr_->tmp_dirs_[dir_idx]->path());
    }
  }
  Status status(TErrorCode::SCRATCH_ALLOCATION_FAILED, join(tmp_dir_paths, ","),
      GetBackendString(),
      PrettyPrinter::PrintBytes(
          tmp_file_mgr_->scratch_bytes_used_metric_->current_value()->GetValue()),
      PrettyPrinter::PrintBytes(current_bytes_allocated_.Load()),
      join(at_capacity_dir_paths, ","));
  // Include all previous errors that may have caused the failure.
  for (Status& err : scratch_errors_) status.MergeStatus(err);
  return status;
}

bool TmpFileGroup::IsSpillingDiskFaulty() {
  lock_guard<SpinLock> lock(lock_);
  return spilling_disk_faulty_;
}

string TmpFileGroup::DebugString() {
  lock_guard<SpinLock> lock(lock_);
  stringstream ss;
  ss << "TmpFileGroup " << this << " bytes limit " << bytes_limit_
     << " current bytes allocated " << current_bytes_allocated_.Load()
     << " next allocation index [ ";
  // Get priority based allocation index.
  for (const auto& entry: next_allocation_index_) {
    ss << " (priority: " << entry.first << ", index: " << entry.second << "), ";
  }
  ss << "] writes "
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

string TmpWriteHandle::TmpFileBufferPath() const {
  if (file_ == nullptr) return "";
  return file_->LocalBuffPath();
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
  Status status = Status::OK();

  // For the second unpin of a page, it will be written to a new file since the
  // content should be changed
  RETURN_IF_ERROR(parent_->AllocateSpace(buffer_to_write.len(), &tmp_file, &file_offset));

  if (FLAGS_disk_spill_encryption) {
    RETURN_IF_ERROR(EncryptAndHash(buffer_to_write, counters));
  }

  // Set all member variables before calling AddWriteRange(): after it succeeds,
  // WriteComplete() may be called concurrently with the remainder of this function.
  // If the TmpFile is not local, the disk queue assigned should be for the
  // buffer.
  data_len_ = buffer.len();
  file_ = tmp_file;
  write_range_.reset(new WriteRange(tmp_file->path(), file_offset,
      tmp_file->AssignDiskQueue(!tmp_file->is_local()), callback));
  write_range_->SetData(buffer_to_write.data(), buffer_to_write.len());
  // For remote files, we write the range to the local buffer.
  write_range_->SetDiskFile(tmp_file->GetWriteFile());
  VLOG(3) << "Write " << tmp_file->path() << " " << file_offset << " "
          << buffer_to_write.len();
  write_in_flight_ = true;

  write_range_->SetRequestContext(io_ctx);
  // Add the write range asyncly to the DiskQueue for writing.
  status = parent_->tmp_file_mgr()->AsyncWriteRange(write_range_.get(), tmp_file);

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
  write_range_->SetDiskFile(file->GetWriteFile());
  Status status = io_ctx->AddWriteRange(write_range_.get());
  if (!status.ok()) {
    // The write will not be in flight if we returned with an error.
    write_in_flight_ = false;
    return status;
  }
  return Status::OK();
}

void TmpWriteHandle::UploadComplete(TmpFile* tmp_file, const Status& upload_status) {
  if (upload_status.ok()) {
    // If uploaded, the local buffer is available to be evicted, so enqueue it to the
    // pool.
    DCHECK(tmp_file != nullptr);
    TmpFileGroup* file_group = tmp_file->file_group_;
    file_group->tmp_file_mgr_->EnqueueTmpFilesPool(
        file_group->FindTmpFileSharedPtr(tmp_file),
        file_group->tmp_file_mgr_->GetRemoteTmpFileBufferPoolLifo());
  } else {
    LOG(WARNING) << "Upload temporary file: '" << tmp_file->path() << " failed";
  }
}

void TmpWriteHandle::WriteComplete(const Status& write_status) {
  WriteDoneCallback cb;
  Status status = write_status;
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

    if (status.ok() && !file_->expected_local_) {
      // Do file upload if the local buffer file is finished.
      if (write_range_->is_full()) {
        TmpFileRemote* tmp_file = static_cast<TmpFileRemote*>(file_);
        RemoteOperRange::RemoteOperDoneCallback u_callback =
            [this, tmp_file](
                const Status& upload_status) { UploadComplete(tmp_file, upload_status); };
        tmp_file->upload_range_.reset(
            new RemoteOperRange(tmp_file->DiskBufferFile(), tmp_file->DiskFile(),
                parent_->tmp_file_mgr()->GetRemoteTmpBlockSize(), tmp_file->disk_id(true),
                RequestType::FILE_UPLOAD, parent_->io_mgr_, u_callback));
        status = parent_->io_ctx_->AddRemoteOperRange(tmp_file->upload_range_.get());
      }
    }

    // Notify before releasing the lock - after the lock is released 'this' may be
    // destroyed.
    write_complete_cv_.NotifyAll();
  }
  // Call 'cb' last - once 'cb' is called client code may call Read() or destroy this
  // handle.
  cb(status);
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

TmpFileBufferPool::TmpFileBufferPool(TmpFileMgr* tmp_file_mgr)
  : tmp_file_mgr_(tmp_file_mgr) {
  tmp_file_dummy_.reset(new TmpFileDummy());
}

TmpFileBufferPool::~TmpFileBufferPool() {
  DCHECK(shut_down_);
}

void TmpFileBufferPool::ShutDown() {
  {
    unique_lock<mutex> l(lock_);
    shut_down_ = true;
  }
  // Wake up the waiting thread.
  work_available_.NotifyAll();
}

void TmpFileBufferPool::TmpFileSpaceReserveThreadLoop() {
  while (true) {
    {
      unique_lock<mutex> l(lock_);
      while (!shut_down_ && write_ranges_.empty()) {
        // Wait if there are no ranges in the queue.
        work_available_.Wait(l);
      }
      if (shut_down_) return;
      DCHECK(!write_ranges_.empty());
      cur_write_range_ = write_ranges_.front();
      write_ranges_.pop_front();
      DCHECK(cur_write_range_ != nullptr);

      // Find out the TmpFile which the current range is associated with, and store the
      // shared_ptr of the file to cur_tmp_file_ in case it is deconstructed while waiting
      // for reservation.
      auto it = write_ranges_iterator_.find(cur_write_range_);
      DCHECK(it != write_ranges_iterator_.end());
      TmpFile* tmp_file = it->second.second;
      cur_tmp_file_ = tmp_file->FileGroup()->FindTmpFileSharedPtr(tmp_file);
      DCHECK(cur_tmp_file_ != nullptr);
      DCHECK_EQ(cur_write_range_->disk_file(), cur_tmp_file_->GetWriteFile());
      write_ranges_iterator_.erase(it);
    }

    // Reserve space from the tmp_files_avail_pool_. The process could need a long wait.
    Status status = tmp_file_mgr_->ReserveLocalBufferSpace(false);
    vector<TmpFileMgr::WriteDoneCallback> write_callbacks;
    {
      unique_lock<mutex> lock(lock_);
      if (status.ok()) {
        DCHECK(cur_tmp_file_ != nullptr);
        cur_tmp_file_->GetWriteFile()->SetSpaceReserved();
        if (cur_write_range_ != nullptr) {
          // Send all of the writes of the same disk file to the disk queue.
          status = MoveWriteRangesHelper(
              cur_write_range_->disk_file(), &write_callbacks, false);
        } else {
          // If the current range becomes a nullptr, it must be set by
          // RemoveWriteRanges(). In this case, the io_ctx which the range belongs to is
          // cancelled, and all the writes using that io_ctx are already cancelled. So, we
          // are safe to return the TmpFile to the pool to recycle the buffer space.
          EnqueueTmpFilesPool(cur_tmp_file_, true);
        }
      } else if (!status.ok() && cur_write_range_ != nullptr) {
        // Cancel the spilling if fails to reserve the buffer.
        RemoveWriteRangesInternal(cur_write_range_->io_ctx(), &write_callbacks);
        status = Status::CancelledInternal(
            Substitute("TmpFileBufferPool because: $0", status.GetDetail()).c_str());
      }
      cur_write_range_ = nullptr;
      cur_tmp_file_.reset();
    }
    for (const TmpFileMgr::WriteDoneCallback& write_callback : write_callbacks) {
      write_callback(status);
    }
  }
}

Status TmpFileBufferPool::MoveWriteRangesHelper(DiskFile* disk_file,
    vector<TmpFileMgr::WriteDoneCallback>* write_callbacks, bool is_cancelled) {
  Status status = Status::OK();
  auto write_ranges_it = write_ranges_to_add_.find(disk_file);
  if (write_ranges_it != write_ranges_to_add_.end()) {
    auto write_range_it = write_ranges_it->second.begin();
    while (write_range_it != write_ranges_it->second.end()) {
      auto range = *write_range_it;
      DCHECK(range != nullptr);
      if (status.ok() && !is_cancelled) {
        status = range->io_ctx()->AddWriteRange(range);
      } else {
        write_callbacks->push_back(range->callback());
        if (is_cancelled && range->offset() == 0) {
          // If is_cancelled is set, try to remove the range from the write_ranges list.
          // If the range hasn't been popped, it must still be in the write_ranges list.
          if (cur_write_range_ != range) {
            auto key_range_it = write_ranges_iterator_.find(range);
            DCHECK(key_range_it != write_ranges_iterator_.end());
            DCHECK_EQ(*(key_range_it->second.first), range);
            write_ranges_.erase(key_range_it->second.first);
            write_ranges_iterator_.erase(key_range_it);
          }
        }
      }
      write_range_it = write_ranges_it->second.erase(write_range_it);
    }
    write_ranges_to_add_.erase(write_ranges_it);
  }
  return status;
}

Status TmpFileBufferPool::EnqueueWriteRange(io::WriteRange* range, TmpFile* tmp_file) {
  Status status = Status::OK();
  {
    unique_lock<mutex> write_range_list_lock(lock_);
    DCHECK(range != nullptr);
    DCHECK(range->disk_file() != nullptr);
    DCHECK(range->io_ctx() != nullptr);
    if (range->disk_file()->IsSpaceReserved()) {
      // If the space is reserved, send the range to the DiskQueue.
      return range->io_ctx()->AddWriteRange(range);
    } else if (range->io_ctx()->IsCancelled()) {
      // If the io_ctx is cancelled, nofity the caller to cancel the query.
      return TMP_FILE_BUFFER_POOL_CONTEXT_CANCELLED;
    } else {
      io_ctx_to_file_set_map_[range->io_ctx()].insert(range->disk_file());
      write_ranges_to_add_[range->disk_file()].emplace_back(range);
    }
    // Put the first range of a file to the queue for waiting for the available space,
    // the ranges in the queue would be popped one by one, when the space is reserved,
    // all ranges of the file are added to the DiskQueue by io_ctx.
    if (range->offset() == 0) {
      write_ranges_.emplace_back(range);
      DCHECK(tmp_file != nullptr);
      write_ranges_iterator_[range] =
          std::make_pair(prev(write_ranges_.cend()), tmp_file);
    }
  }
  work_available_.NotifyAll();
  return status;
}

void TmpFileBufferPool::RemoveWriteRangesInternal(
    RequestContext* io_ctx, vector<TmpFileMgr::WriteDoneCallback>* write_callbacks) {
  auto file_set_it = io_ctx_to_file_set_map_.find(io_ctx);
  if (file_set_it != io_ctx_to_file_set_map_.end()) {
    auto file_it = file_set_it->second.begin();
    while (file_it != file_set_it->second.end()) {
      DCHECK(*file_it != nullptr);
      // Remove all the ranges belonging to the file, and fetch the callback
      // functions of the ranges.
      Status status = MoveWriteRangesHelper(*file_it, write_callbacks, true);
      DCHECK_OK(status);
      if (cur_write_range_ != nullptr && *file_it == cur_write_range_->disk_file()) {
        // Set the current write range to nullptr if the TmpFileGroup is closing to
        // notify the reservation thread (it is waiting for the reservation) that the
        // space is no longer needed for the write range.
        cur_write_range_ = nullptr;
      }
      file_it = file_set_it->second.erase(file_it);
    }
    io_ctx_to_file_set_map_.erase(file_set_it);
  }
}

void TmpFileBufferPool::RemoveWriteRanges(RequestContext* io_ctx) {
  DCHECK(io_ctx != nullptr);
  vector<TmpFileMgr::WriteDoneCallback> write_callbacks;
  {
    unique_lock<mutex> lock(lock_);
    RemoveWriteRangesInternal(io_ctx, &write_callbacks);
  }
  for (const TmpFileMgr::WriteDoneCallback& write_callback : write_callbacks) {
    write_callback(TMP_FILE_BUFFER_POOL_CONTEXT_CANCELLED);
  }
}

void TmpFileBufferPool::EnqueueTmpFilesPool(shared_ptr<TmpFile>& tmp_file, bool front) {
  DCHECK(tmp_file != nullptr);
  {
    unique_lock<mutex> buffer_lock(tmp_files_avail_pool_lock_);
    if (tmp_file->disk_type() != io::DiskFileType::DUMMY) {
      TmpFileRemote* tmp_file_remote = static_cast<TmpFileRemote*>(tmp_file.get());
      if (tmp_file_remote->is_enqueued()) return;
      tmp_file_remote->SetEnqueued(true);
      tmp_file_remote->SetBufferReturned();
    }
    if (front) {
      tmp_files_avail_pool_.push_front(tmp_file);
    } else {
      tmp_files_avail_pool_.push_back(tmp_file);
    }
    tmp_file_mgr_->GetLocalBufferDir()->bytes_used_metric()->Increment(
        -1 * tmp_file_mgr_->GetRemoteTmpFileSize());
  }
  tmp_files_available_cv_.NotifyOne();
}

Status TmpFileBufferPool::DequeueTmpFilesPool(
    shared_ptr<TmpFile>* tmp_file, bool quick_return) {
  DCHECK(tmp_file != nullptr);
  DCHECK(dequeue_timer_metric_ != nullptr);
  ScopedHistogramTimer wait_timer(dequeue_timer_metric_);
  unique_lock<mutex> buffer_lock(tmp_files_avail_pool_lock_);
  // If quick return is set and no buffer is available, return immediately.
  if (quick_return && tmp_files_avail_pool_.empty()) {
    return TMP_FILE_MGR_NO_AVAILABLE_FILE_TO_EVICT;
  }
  while (tmp_files_avail_pool_.empty()) {
    // Wait if there is no temporary file on the queue.
    // If timeout, return immediately.
    if (!tmp_files_available_cv_.WaitFor(
            buffer_lock, tmp_file_mgr_->GetSpillBufferWaitTimeout())) {
      return Status(Substitute("Timeout waiting for a local buffer in $0 seconds",
          tmp_file_mgr_->GetSpillBufferWaitTimeout() / MICROS_PER_SEC));
    };
  }
  DCHECK(!tmp_files_avail_pool_.empty());
  *tmp_file = tmp_files_avail_pool_.front();
  tmp_files_avail_pool_.pop_front();
  DCHECK(*tmp_file != nullptr);
  if ((*tmp_file)->disk_type() != io::DiskFileType::DUMMY) {
    TmpFileRemote* tmp_file_remote = static_cast<TmpFileRemote*>(tmp_file->get());
    // Assert the default size remains the same in case the object is corrupted.
    DCHECK_EQ(tmp_file_remote->file_size_, tmp_file_mgr_->GetRemoteTmpFileSize());
    DCHECK(tmp_file_remote->is_enqueued());
    tmp_file_remote->SetEnqueued(false);
  }
  tmp_file_mgr_->GetLocalBufferDir()->bytes_used_metric()->Increment(
      tmp_file_mgr_->GetRemoteTmpFileSize());
  return Status::OK();
}

} // namespace impala
