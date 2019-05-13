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

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/locks.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gutil/strings/join.h>
#include <gutil/strings/substitute.h>

#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/runtime-state.h"
#include "runtime/tmp-file-mgr-internal.h"
#include "util/bit-util.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/filesystem-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

DEFINE_bool(disk_spill_encryption, true,
    "Set this to encrypt and perform an integrity "
    "check on all data spilled to disk during a query");
DEFINE_string(scratch_dirs, "/tmp", "Writable scratch directories");
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
using namespace strings;

namespace impala {

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

TmpFileMgr::TmpFileMgr()
  : initialized_(false),
    num_active_scratch_dirs_metric_(nullptr),
    active_scratch_dirs_metric_(nullptr),
    scratch_bytes_used_metric_(nullptr) {}

Status TmpFileMgr::Init(MetricGroup* metrics) {
  string tmp_dirs_spec = FLAGS_scratch_dirs;
  vector<string> all_tmp_dirs;
  // Empty string should be interpreted as no scratch
  if (!tmp_dirs_spec.empty()) {
    split(all_tmp_dirs, tmp_dirs_spec, is_any_of(","), token_compress_on);
  }
  return InitCustom(all_tmp_dirs, !FLAGS_allow_multiple_scratch_dirs_per_device, metrics);
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
      status = FileSystemUtil::RemoveAndCreateDirectory(scratch_subdir_path.string());
      if (status.ok()) {
        if (disk_id >= 0) is_tmp_dir_on_disk[disk_id] = true;
        LOG(INFO) << "Using scratch directory " << scratch_subdir_path.string() << " on "
                  << "disk " << disk_id;
        tmp_dirs_.push_back(scratch_subdir_path.string());
      } else {
        LOG(WARNING) << "Could not remove and recreate directory "
                     << scratch_subdir_path.string() << ": cannot use it for scratch. "
                     << "Error was: " << status.msg().msg();
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
    active_scratch_dirs_metric_->Add(tmp_dirs_[i]);
  }
  scratch_bytes_used_metric_ =
      metrics->AddHWMGauge(TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED_HIGH_WATER_MARK,
          TMP_FILE_MGR_SCRATCH_SPACE_BYTES_USED, 0);

  initialized_ = true;

  if (tmp_dirs_.empty() && !tmp_dirs.empty()) {
    LOG(ERROR) << "Running without spill to disk: could not use any scratch "
               << "directories in list: " << join(tmp_dirs, ",")
               << ". See previous warnings for information on causes.";
  }
  return Status::OK();
}

void TmpFileMgr::NewFile(
    FileGroup* file_group, DeviceId device_id, unique_ptr<File>* new_file) {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  DCHECK(file_group != nullptr);
  // Generate the full file path.
  string unique_name = lexical_cast<string>(random_generator()());
  stringstream file_name;
  file_name << PrintId(file_group->unique_id()) << "_" << unique_name;
  path new_file_path(tmp_dirs_[device_id]);
  new_file_path /= file_name.str();

  new_file->reset(new File(file_group, device_id, new_file_path.string()));
}

string TmpFileMgr::GetTmpDirPath(DeviceId device_id) const {
  DCHECK(initialized_);
  DCHECK_GE(device_id, 0);
  DCHECK_LT(device_id, tmp_dirs_.size());
  return tmp_dirs_[device_id];
}

int TmpFileMgr::NumActiveTmpDevices() {
  DCHECK(initialized_);
  return tmp_dirs_.size();
}

vector<TmpFileMgr::DeviceId> TmpFileMgr::ActiveTmpDevices() {
  vector<TmpFileMgr::DeviceId> devices;
  for (DeviceId device_id = 0; device_id < tmp_dirs_.size(); ++device_id) {
    devices.push_back(device_id);
  }
  return devices;
}

TmpFileMgr::File::File(FileGroup* file_group, DeviceId device_id, const string& path)
  : file_group_(file_group),
    path_(path),
    device_id_(device_id),
    disk_id_(DiskInfo::disk_id(path.c_str())),
    bytes_allocated_(0),
    blacklisted_(false) {
  DCHECK(file_group != nullptr);
}

void TmpFileMgr::File::AllocateSpace(int64_t num_bytes, int64_t* offset) {
  DCHECK_GT(num_bytes, 0);
  *offset = bytes_allocated_;
  bytes_allocated_ += num_bytes;
}

int TmpFileMgr::File::AssignDiskQueue() const {
  return file_group_->io_mgr_->AssignQueue(path_.c_str(), disk_id_, false);
}

void TmpFileMgr::File::Blacklist(const ErrorMsg& msg) {
  LOG(ERROR) << "Error for temporary file '" << path_ << "': " << msg.msg();
  blacklisted_ = true;
}

Status TmpFileMgr::File::Remove() {
  // Remove the file if present (it may not be present if no writes completed).
  return FileSystemUtil::RemovePaths({path_});
}

string TmpFileMgr::File::DebugString() {
  return Substitute("File $0 path '$1' device id $2 disk id $3 bytes allocated $4 "
      "blacklisted $5", this, path_, device_id_, disk_id_, bytes_allocated_,
      blacklisted_);
}

TmpFileMgr::FileGroup::FileGroup(TmpFileMgr* tmp_file_mgr, DiskIoMgr* io_mgr,
    RuntimeProfile* profile, const TUniqueId& unique_id, int64_t bytes_limit)
  : tmp_file_mgr_(tmp_file_mgr),
    io_mgr_(io_mgr),
    io_ctx_(nullptr),
    unique_id_(unique_id),
    bytes_limit_(bytes_limit),
    write_counter_(ADD_COUNTER(profile, "ScratchWrites", TUnit::UNIT)),
    bytes_written_counter_(ADD_COUNTER(profile, "ScratchBytesWritten", TUnit::BYTES)),
    read_counter_(ADD_COUNTER(profile, "ScratchReads", TUnit::UNIT)),
    bytes_read_counter_(ADD_COUNTER(profile, "ScratchBytesRead", TUnit::BYTES)),
    scratch_space_bytes_used_counter_(
        ADD_COUNTER(profile, "ScratchFileUsedBytes", TUnit::BYTES)),
    disk_read_timer_(ADD_TIMER(profile, "TotalReadBlockTime")),
    encryption_timer_(ADD_TIMER(profile, "TotalEncryptionTime")),
    current_bytes_allocated_(0),
    next_allocation_index_(0),
    free_ranges_(64) {
  DCHECK(tmp_file_mgr != nullptr);
  io_ctx_ = io_mgr_->RegisterContext();
}

TmpFileMgr::FileGroup::~FileGroup() {
  DCHECK_EQ(tmp_files_.size(), 0);
}

Status TmpFileMgr::FileGroup::CreateFiles() {
  lock_.DCheckLocked();
  DCHECK(tmp_files_.empty());
  vector<DeviceId> tmp_devices = tmp_file_mgr_->ActiveTmpDevices();
  int files_allocated = 0;
  // Initialize the tmp files and the initial file to use.
  for (int i = 0; i < tmp_devices.size(); ++i) {
    TmpFileMgr::DeviceId device_id = tmp_devices[i];
    unique_ptr<TmpFileMgr::File> tmp_file;
    tmp_file_mgr_->NewFile(this, device_id, &tmp_file);
    tmp_files_.emplace_back(std::move(tmp_file));
    ++files_allocated;
  }
  DCHECK_EQ(tmp_files_.size(), files_allocated);
  if (tmp_files_.size() == 0) return ScratchAllocationFailedStatus();
  // Start allocating on a random device to avoid overloading the first device.
  next_allocation_index_ = rand() % tmp_files_.size();
  return Status::OK();
}

void TmpFileMgr::FileGroup::Close() {
  // Cancel writes before deleting the files, since in-flight writes could re-create
  // deleted files.
  if (io_ctx_ != nullptr) io_mgr_->UnregisterContext(io_ctx_.get());
  for (std::unique_ptr<TmpFileMgr::File>& file : tmp_files_) {
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

Status TmpFileMgr::FileGroup::AllocateSpace(
    int64_t num_bytes, File** tmp_file, int64_t* file_offset) {
  lock_guard<SpinLock> lock(lock_);
  int64_t scratch_range_bytes = max<int64_t>(1L, BitUtil::RoundUpToPowerOfTwo(num_bytes));
  int free_ranges_idx = BitUtil::Log2Ceiling64(scratch_range_bytes);
  if (!free_ranges_[free_ranges_idx].empty()) {
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

  // Find the next physical file in round-robin order and allocate a range from it.
  for (int attempt = 0; attempt < tmp_files_.size(); ++attempt) {
    *tmp_file = tmp_files_[next_allocation_index_].get();
    next_allocation_index_ = (next_allocation_index_ + 1) % tmp_files_.size();
    if ((*tmp_file)->is_blacklisted()) continue;
    (*tmp_file)->AllocateSpace(scratch_range_bytes, file_offset);
    scratch_space_bytes_used_counter_->Add(scratch_range_bytes);
    tmp_file_mgr_->scratch_bytes_used_metric_->Increment(scratch_range_bytes);
    current_bytes_allocated_ += num_bytes;
    return Status::OK();
  }
  return ScratchAllocationFailedStatus();
}

void TmpFileMgr::FileGroup::RecycleFileRange(unique_ptr<WriteHandle> handle) {
  int64_t scratch_range_bytes =
      max<int64_t>(1L, BitUtil::RoundUpToPowerOfTwo(handle->len()));
  int free_ranges_idx = BitUtil::Log2Ceiling64(scratch_range_bytes);
  lock_guard<SpinLock> lock(lock_);
  free_ranges_[free_ranges_idx].emplace_back(
      handle->file_, handle->write_range_->offset());
}

Status TmpFileMgr::FileGroup::Write(
    MemRange buffer, WriteDoneCallback cb, unique_ptr<TmpFileMgr::WriteHandle>* handle) {
  DCHECK_GE(buffer.len(), 0);

  File* tmp_file;
  int64_t file_offset;
  RETURN_IF_ERROR(AllocateSpace(buffer.len(), &tmp_file, &file_offset));

  unique_ptr<WriteHandle> tmp_handle(new WriteHandle(encryption_timer_, cb));
  WriteHandle* tmp_handle_ptr = tmp_handle.get(); // Pass ptr by value into lambda.
  WriteRange::WriteDoneCallback callback = [this, tmp_handle_ptr](
      const Status& write_status) { WriteComplete(tmp_handle_ptr, write_status); };
  RETURN_IF_ERROR(
      tmp_handle->Write(io_ctx_.get(), tmp_file, file_offset, buffer, callback));
  write_counter_->Add(1);
  bytes_written_counter_->Add(buffer.len());
  *handle = move(tmp_handle);
  return Status::OK();
}

Status TmpFileMgr::FileGroup::Read(WriteHandle* handle, MemRange buffer) {
  RETURN_IF_ERROR(ReadAsync(handle, buffer));
  return WaitForAsyncRead(handle, buffer);
}

Status TmpFileMgr::FileGroup::ReadAsync(WriteHandle* handle, MemRange buffer) {
  DCHECK(handle->write_range_ != nullptr);
  DCHECK(!handle->is_cancelled_);
  DCHECK_EQ(buffer.len(), handle->len());
  Status status;

  // Don't grab 'write_state_lock_' in this method - it is not necessary because we
  // don't touch any members that it protects and could block other threads for the
  // duration of the synchronous read.
  DCHECK(!handle->write_in_flight_);
  DCHECK(handle->read_range_ == nullptr);
  DCHECK(handle->write_range_ != nullptr);
  // Don't grab handle->write_state_lock_, it is safe to touch all of handle's state
  // since the write is not in flight.
  handle->read_range_ = scan_range_pool_.Add(new ScanRange);
  handle->read_range_->Reset(nullptr, handle->write_range_->file(),
      handle->write_range_->len(), handle->write_range_->offset(),
      handle->write_range_->disk_id(), false, false,
      BufferOpts::ReadInto(buffer.data(), buffer.len()));
  read_counter_->Add(1);
  bytes_read_counter_->Add(buffer.len());
  bool needs_buffers;
  RETURN_IF_ERROR(io_ctx_->StartScanRange(handle->read_range_, &needs_buffers));
  DCHECK(!needs_buffers) << "Already provided a buffer";
  return Status::OK();
}

Status TmpFileMgr::FileGroup::WaitForAsyncRead(WriteHandle* handle, MemRange buffer) {
  DCHECK(handle->read_range_ != nullptr);
  // Don't grab handle->write_state_lock_, it is safe to touch all of handle's state
  // since the write is not in flight.
  SCOPED_TIMER(disk_read_timer_);
  unique_ptr<BufferDescriptor> io_mgr_buffer;
  Status status = handle->read_range_->GetNext(&io_mgr_buffer);
  if (!status.ok()) goto exit;
  DCHECK(io_mgr_buffer != NULL);
  DCHECK(io_mgr_buffer->eosr());
  DCHECK_LE(io_mgr_buffer->len(), buffer.len());
  if (io_mgr_buffer->len() < buffer.len()) {
    // The read was truncated - this is an error.
    status = Status(TErrorCode::SCRATCH_READ_TRUNCATED, buffer.len(),
        handle->write_range_->file(), GetBackendString(), handle->write_range_->offset(),
        io_mgr_buffer->len());
    goto exit;
  }
  DCHECK_EQ(io_mgr_buffer->buffer(), buffer.data());

  if (FLAGS_disk_spill_encryption) {
    status = handle->CheckHashAndDecrypt(buffer);
    if (!status.ok()) goto exit;
  }
exit:
  // Always return the buffer before exiting to avoid leaking it.
  if (io_mgr_buffer != nullptr) handle->read_range_->ReturnBuffer(move(io_mgr_buffer));
  handle->read_range_ = nullptr;
  return status;
}

Status TmpFileMgr::FileGroup::RestoreData(
    unique_ptr<WriteHandle> handle, MemRange buffer) {
  DCHECK_EQ(handle->write_range_->data(), buffer.data());
  DCHECK_EQ(handle->len(), buffer.len());
  DCHECK(!handle->write_in_flight_);
  DCHECK(handle->read_range_ == nullptr);
  // Decrypt after the write is finished, so that we don't accidentally write decrypted
  // data to disk.
  Status status;
  if (FLAGS_disk_spill_encryption) {
    status = handle->CheckHashAndDecrypt(buffer);
  }
  RecycleFileRange(move(handle));
  return status;
}

void TmpFileMgr::FileGroup::DestroyWriteHandle(unique_ptr<WriteHandle> handle) {
  handle->Cancel();
  handle->WaitForWrite();
  RecycleFileRange(move(handle));
}

void TmpFileMgr::FileGroup::WriteComplete(
    WriteHandle* handle, const Status& write_status) {
  Status status;
  if (!write_status.ok()) {
    status = RecoverWriteError(handle, write_status);
    if (status.ok()) return;
  } else {
    status = write_status;
  }
  handle->WriteComplete(status);
}

Status TmpFileMgr::FileGroup::RecoverWriteError(
    WriteHandle* handle, const Status& write_status) {
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
  }
  handle->file_->Blacklist(write_status.msg());

  // Do not retry cancelled writes or propagate the error, simply return CANCELLED.
  if (handle->is_cancelled_) return Status::CancelledInternal("TmpFileMgr write");

  TmpFileMgr::File* tmp_file;
  int64_t file_offset;
  // Discard the scratch file range - we will not reuse ranges from a bad file.
  // Choose another file to try. Blacklisting ensures we don't retry the same file.
  // If this fails, the status will include all the errors in 'scratch_errors_'.
  RETURN_IF_ERROR(AllocateSpace(handle->len(), &tmp_file, &file_offset));
  return handle->RetryWrite(io_ctx_.get(), tmp_file, file_offset);
}

Status TmpFileMgr::FileGroup::ScratchAllocationFailedStatus() {
  Status status(TErrorCode::SCRATCH_ALLOCATION_FAILED,
        join(tmp_file_mgr_->tmp_dirs_, ","), GetBackendString(),
        PrettyPrinter::PrintBytes(scratch_space_bytes_used_counter_->value()),
        PrettyPrinter::PrintBytes(current_bytes_allocated_));
  // Include all previous errors that may have caused the failure.
  for (Status& err : scratch_errors_) status.MergeStatus(err);
  return status;
}

string TmpFileMgr::FileGroup::DebugString() {
  lock_guard<SpinLock> lock(lock_);
  stringstream ss;
  ss << "FileGroup " << this << " bytes limit " << bytes_limit_
     << " current bytes allocated " << current_bytes_allocated_
     << " next allocation index " << next_allocation_index_ << " writes "
     << write_counter_->value() << " bytes written " << bytes_written_counter_->value()
     << " reads " << read_counter_->value() << " bytes read "
     << bytes_read_counter_->value() << " scratch bytes used "
     << scratch_space_bytes_used_counter_ << " dist read timer "
     << disk_read_timer_->value() << " encryption timer " << encryption_timer_->value()
     << endl
     << "  " << tmp_files_.size() << " files:" << endl;
  for (unique_ptr<File>& file : tmp_files_) {
    ss << "    " << file->DebugString() << endl;
  }
  return ss.str();
}

TmpFileMgr::WriteHandle::WriteHandle(
    RuntimeProfile::Counter* encryption_timer, WriteDoneCallback cb)
  : cb_(cb),
    encryption_timer_(encryption_timer),
    file_(nullptr),
    read_range_(nullptr),
    is_cancelled_(false),
    write_in_flight_(false) {}

TmpFileMgr::WriteHandle::~WriteHandle() {
  DCHECK(!write_in_flight_);
  DCHECK(read_range_ == nullptr);
}

string TmpFileMgr::WriteHandle::TmpFilePath() const {
  if (file_ == nullptr) return "";
  return file_->path();
}

int64_t TmpFileMgr::WriteHandle::len() const {
  return write_range_->len();
}

Status TmpFileMgr::WriteHandle::Write(RequestContext* io_ctx,
    File* file, int64_t offset, MemRange buffer,
    WriteRange::WriteDoneCallback callback) {
  DCHECK(!write_in_flight_);

  if (FLAGS_disk_spill_encryption) RETURN_IF_ERROR(EncryptAndHash(buffer));

  // Set all member variables before calling AddWriteRange(): after it succeeds,
  // WriteComplete() may be called concurrently with the remainder of this function.
  file_ = file;
  write_range_.reset(
      new WriteRange(file->path(), offset, file->AssignDiskQueue(), callback));
  write_range_->SetData(buffer.data(), buffer.len());
  write_in_flight_ = true;
  Status status = io_ctx->AddWriteRange(write_range_.get());
  if (!status.ok()) {
    // The write will not be in flight if we returned with an error.
    write_in_flight_ = false;
    // We won't return this WriteHandle to the client of FileGroup, so it won't be
    // cancelled in the normal way. Mark the handle as cancelled so it can be
    // cleanly destroyed.
    is_cancelled_ = true;
    return status;
  }
  return Status::OK();
}

Status TmpFileMgr::WriteHandle::RetryWrite(
    RequestContext* io_ctx, File* file, int64_t offset) {
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

void TmpFileMgr::WriteHandle::WriteComplete(const Status& write_status) {
  WriteDoneCallback cb;
  {
    lock_guard<mutex> lock(write_state_lock_);
    DCHECK(write_in_flight_);
    write_in_flight_ = false;
    // Need to extract 'cb_' because once 'write_in_flight_' is false and we release
    // 'write_state_lock_', 'this' may be destroyed.
    cb = move(cb_);

    // Notify before releasing the lock - after the lock is released 'this' may be
    // destroyed.
    write_complete_cv_.NotifyAll();
  }
  // Call 'cb' last - once 'cb' is called client code may call Read() or destroy this
  // handle.
  cb(write_status);
}

void TmpFileMgr::WriteHandle::Cancel() {
  CancelRead();
  {
    unique_lock<mutex> lock(write_state_lock_);
    is_cancelled_ = true;
    // TODO: in future, if DiskIoMgr supported write cancellation, we could cancel it
    // here.
  }
}

void TmpFileMgr::WriteHandle::CancelRead() {
  if (read_range_ != nullptr) {
    read_range_->Cancel(Status::CancelledInternal("TmpFileMgr read"));
    read_range_ = nullptr;
  }
}

void TmpFileMgr::WriteHandle::WaitForWrite() {
  unique_lock<mutex> lock(write_state_lock_);
  while (write_in_flight_) write_complete_cv_.Wait(lock);
}

Status TmpFileMgr::WriteHandle::EncryptAndHash(MemRange buffer) {
  DCHECK(FLAGS_disk_spill_encryption);
  SCOPED_TIMER(encryption_timer_);
  // Since we're using GCM/CTR/CFB mode, we must take care not to reuse a
  // key/IV pair. Regenerate a new key and IV for every data buffer we write.
  key_.InitializeRandom();
  RETURN_IF_ERROR(key_.Encrypt(buffer.data(), buffer.len(), buffer.data()));

  if (!key_.IsGcmMode()) {
    hash_.Compute(buffer.data(), buffer.len());
  }
  return Status::OK();
}

Status TmpFileMgr::WriteHandle::CheckHashAndDecrypt(MemRange buffer) {
  DCHECK(FLAGS_disk_spill_encryption);
  DCHECK(write_range_ != nullptr);
  SCOPED_TIMER(encryption_timer_);

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

string TmpFileMgr::WriteHandle::DebugString() {
  unique_lock<mutex> lock(write_state_lock_);
  stringstream ss;
  ss << "Write handle " << this << " file '" << file_->path() << "'"
     << " is cancelled " << is_cancelled_ << " write in flight " << write_in_flight_;
  if (write_range_ != NULL) {
    ss << " data " << write_range_->data() << " len " << write_range_->len()
       << " file offset " << write_range_->offset()
       << " disk id " << write_range_->disk_id();
  }
  return ss.str();
}
} // namespace impala
