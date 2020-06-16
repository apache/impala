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


#ifndef IMPALA_COMMON_STATUS_H
#define IMPALA_COMMON_STATUS_H

#include <iosfwd>
#include <string>
#include <vector>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "gen-cpp/Status_types.h"  // for TStatus
#include "gen-cpp/ErrorCodes_types.h"  // for TErrorCode
#include "gen-cpp/TCLIService_types.h" // for HS2 TStatus
#include "util/error-util.h" // for ErrorMessage

#define STATUS_API_VERSION 2

namespace impala {

class StatusPB;

/// Status is used as a function return type to indicate success, failure or cancellation
/// of the function. In case of successful completion, it only occupies sizeof(void*)
/// statically allocated memory and therefore no more members should be added to this
/// class.
//
/// A Status may either be OK (represented by passing a default constructed Status
/// instance, created via Status::OK()), or it may represent an error condition. In the
/// latter case, a Status has both an error code, which belongs to the TErrorCode enum,
/// and an error string, which may be presented to clients or logged to disk.
///
/// An error Status may also have one or more optional 'detail' strings which provide
/// further context. These strings are intended for internal consumption only - and
/// therefore will not be sent to clients.
///
/// The state associated with an error Status is encapsulated in an ErrorMsg instance to
/// ensure that the size of Status is kept very small, as it is passed around on the stack
/// as a return value. See ErrorMsg for more details on how error strings are constructed.
///
/// Example Usage:
/// Status fnB(int x) {
//
///   // Status as return value
///   Status status = fnA(x);
///   if (!status.ok()) {
///     status.AddDetail("fnA(x) went wrong");
///     return status;
///   }
//
///   int r = Read(fid);
///   // Attaches an ErrorMsg with type GENERAL to the status
///   if (r == -1) return Status("String Constructor");
//
///   int x = MoreRead(x);
///   if (x == 4711) {
///     // Specific error messages with one param
///     Status s = Status(ERROR_4711_HAS_NO_BLOCKS, x);
///     // Optional detail
///     s.AddDetail("rotation-disk-broken due to weather");
///   }
///
///   return Status::OK();
/// }
class [[nodiscard]] Status {
 public:
  typedef strings::internal::SubstituteArg ArgType;

  ALWAYS_INLINE Status(): msg_(NULL) {}

  // Return a default constructed Status instance in the OK case.
  static ALWAYS_INLINE Status OK() { return Status(); }

  // Return a MEM_LIMIT_EXCEEDED error status.
  static Status MemLimitExceeded();
  static Status MemLimitExceeded(const std::string& details);

  static const Status CANCELLED;
  static const Status DEPRECATED_RPC;

  /// Return a CANCELLED_INTERNALLY error. This should be used for "internal"
  /// cancellation messages that were not user-initiated and are not expected to be shown
  /// to the client. 'subsystem' is the name of the subsystem in which the cancellation
  /// occurred, so that we can determine where the cancellation came from.
  static Status CancelledInternal(const char* subsystem);

  /// Copy c'tor makes copy of error detail so Status can be returned by value.
  ALWAYS_INLINE Status(const Status& status) : msg_(NULL) {
    if (UNLIKELY(status.msg_ != NULL)) CopyMessageFrom(status);
  }

  /// Move constructor that moves the error message (if any) and resets 'other' to the
  /// default OK Status.
  ALWAYS_INLINE Status(Status&& other) noexcept : msg_(other.msg_) { other.msg_ = NULL; }

  /// Status using only the error code as a parameter. This can be used for error messages
  /// that don't take format parameters.
  explicit Status(TErrorCode::type code) : Status(false, code) {}

  /// These constructors are used if the caller wants to indicate a non-successful
  /// execution and supply a client-facing error message. Using TErrorCode and
  /// parameterised error messages is the preferred way of instantiating a
  /// non-successful Status instead of the std::string constructor. These constructors
  /// log the error message and a traceback, which can be expensive, so these should only
  /// be used for rare, low-frequency errors. Status::Expected() does not log a traceback
  /// and should be used for higher-frequency errors.
  Status(TErrorCode::type error, const ArgType& arg0) : Status(false, error, arg0) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1)
    : Status(false, error, arg0, arg1) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2)
    : Status(false, error, arg0, arg1, arg2) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3)
    : Status(false, error, arg0, arg1, arg2, arg3) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4)
    : Status(false, error, arg0, arg1, arg2, arg3, arg4) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5)
    : Status(false, error, arg0, arg1, arg2, arg3, arg4, arg5) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6)
    : Status(false, error, arg0, arg1, arg2, arg3, arg4, arg5, arg6) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6, const ArgType& arg7)
    : Status(false, error, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6, const ArgType& arg7, const ArgType& arg8)
    : Status(false, error, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8) {}
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6, const ArgType& arg7, const ArgType& arg8, const ArgType& arg9)
    : Status(false, error, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9) {}

  /// Used when the ErrorMsg is created as an intermediate value that is either passed to
  /// the Status or to the RuntimeState.
  explicit Status(const ErrorMsg& e);

  /// This constructor creates a Status with a default error code of GENERAL and is not
  /// intended for statuses that might be client-visible.
  /// TODO: deprecate
  explicit Status(const std::string& error_msg);

  /// "Copy" c'tor from TStatus.
  /// Retains the TErrorCode value and the message
  explicit Status(const TStatus& status);

  /// "Copy" c'tor from StatusPB (a protobuf serialized version of Status object).
  /// Retains the TErrorCode value and the message
  explicit Status(const StatusPB& status);

  /// "Copy c'tor from HS2 TStatus.
  /// Retains the TErrorCode value and the message
  explicit Status(const apache::hive::service::cli::thrift::TStatus& hs2_status);

  /// The below Status::Expected() functions create a status instance that represents
  /// an expected error. They behave the same as the constructors with the same
  /// argument types, except they do not log the error message and traceback.
  static Status Expected(const ErrorMsg& e);
  static Status Expected(const std::string& error_msg);
  static Status Expected(TErrorCode::type error);
  static Status Expected(TErrorCode::type error, const ArgType& arg0);
  static Status Expected(
      TErrorCode::type error, const ArgType& arg0, const ArgType& arg1);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7, const ArgType& arg8);
  static Status Expected(TErrorCode::type error, const ArgType& arg0,
      const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7, const ArgType& arg8,
      const ArgType& arg9);

  /// same as copy c'tor
  ALWAYS_INLINE Status& operator=(const Status& status) {
    // Take the slow path if either Status objects have non-NULL messages (unless they
    // are aliases).
    if (UNLIKELY(msg_ != status.msg_)) CopyMessageFrom(status);
    return *this;
  }

  /// Move assignment that moves the error message (if any) and resets 'other' to the
  /// default OK Status.
  ALWAYS_INLINE Status& operator=(Status&& other) {
    if (UNLIKELY(msg_ != NULL)) FreeMessage();
    msg_ = other.msg_;
    other.msg_ = NULL;
    return *this;
  }

  ALWAYS_INLINE ~Status() {
    // The UNLIKELY and inlining here are important hints for the compiler to
    // streamline the common case of Status::OK(). Use FreeMessage() which is
    // not inlined to free the message. This avoids potential code bloat due
    // to duplicated code from the delete statement.
    if (UNLIKELY(msg_ != NULL)) FreeMessage();
  }

  /// Retains the TErrorCode value and the message
  Status& operator=(const TStatus& status);

  bool ALWAYS_INLINE ok() const { return msg_ == NULL; }

  /// Return true if this is a user-initiated or internal cancellation.
  bool IsCancelled() const {
    return msg_ != NULL && (msg_->error() == TErrorCode::CANCELLED
                               || msg_->error() == TErrorCode::CANCELLED_INTERNALLY);
  }

  bool IsMemLimitExceeded() const {
    return msg_ != NULL
        && msg_->error() == TErrorCode::MEM_LIMIT_EXCEEDED;
  }

  bool IsInternalError() const {
    return msg_ != NULL
        && msg_->error() == TErrorCode::INTERNAL_ERROR;
  }

  bool IsRecoverableError() const {
    return msg_ != NULL
        && msg_->error() == TErrorCode::RECOVERABLE_ERROR;
  }

  bool IsDiskIoError() const {
    return msg_ != NULL
        && msg_->error() == TErrorCode::DISK_IO_ERROR;
  }

  bool IsThreadPoolError() const {
    return msg_ != NULL
      && (msg_->error() == TErrorCode::THREAD_POOL_SUBMIT_FAILED ||
          msg_->error() == TErrorCode::THREAD_POOL_TASK_TIMED_OUT);
  }

  /// Returns the error message associated with a non-successful status.
  const ErrorMsg& msg() const {
    DCHECK(msg_ != NULL);
    return *msg_; // NOLINT: clang-tidy thinks this might deref a nullptr
  }

  /// Add a detail string. Calling this method is only defined on a non-OK message
  void AddDetail(const std::string& msg);

  /// Does nothing if status.ok().
  /// Otherwise: if 'this' is an error status, adds the error msg from 'status';
  /// otherwise assigns 'status'.
  void MergeStatus(const Status& status);

  /// Convert into TStatus. Call this if 'status_container' contains an optional TStatus
  /// field named 'status'. This also sets status_container->__isset.status.
  template <typename T> void SetTStatus(T* status_container) const {
    ToThrift(&status_container->status);
    status_container->__isset.status = true;
  }

  /// Convert into TStatus.
  void ToThrift(TStatus* status) const;

  /// Serialize into StatusPB
  void ToProto(StatusPB* status) const;

  /// Returns the formatted message of the error message and the individual details of the
  /// additional messages as a single string. This should only be called internally and
  /// not to report an error back to the client.
  const std::string GetDetail() const;

  TErrorCode::type code() const {
    return msg_ == NULL ? TErrorCode::OK : msg_->error();
  }

  static const char* LLVM_CLASS_NAME;
 private:
  // Status constructors that can suppress logging via 'silent' parameter
  Status(const ErrorMsg& error_msg, bool silent);
  Status(const std::string& error_msg, bool silent);
  Status(bool silent, TErrorCode::type code);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6, const ArgType& arg7);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6, const ArgType& arg7, const ArgType& arg8);
  Status(bool silent, TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
      const ArgType& arg6, const ArgType& arg7, const ArgType& arg8, const ArgType& arg9);

  // A non-inline function for copying status' message.
  void CopyMessageFrom(const Status& status) noexcept;

  // A non-inline function for freeing status' message.
  void FreeMessage() noexcept;

  /// A non-inline function for unwrapping a TStatus object.
  void FromThrift(const TStatus& status);

  /// A non-inline function for unwrapping a StatusPB object.
  void FromProto(const StatusPB& status);

  /// Status uses a naked pointer to ensure the size of an instance on the stack is only
  /// the sizeof(ErrorMsg*). Every Status owns its ErrorMsg instance.
  ErrorMsg* msg_;
};

/// for debugging
std::ostream& operator<<(std::ostream& os, const Status& status);

/// some generally useful macros
#define RETURN_IF_ERROR(stmt)                          \
  do {                                                 \
    const ::impala::Status& _status = (stmt);       \
    if (UNLIKELY(!_status.ok())) return _status; \
  } while (false)

#define LOG_AND_RETURN_IF_ERROR(stmt) \
  do { \
    const ::impala::Status& _status = (stmt); \
    if (UNLIKELY(!_status.ok()))  { \
      LOG(INFO) << _status.GetDetail(); \
      return _status; \
    } \
  } while (false)

#define RETURN_VOID_IF_ERROR(stmt)                     \
  do {                                                 \
    if (UNLIKELY(!(stmt).ok())) return;                \
  } while (false)

#define ABORT_IF_ERROR(stmt) \
  do { \
    const ::impala::Status& _status = (stmt); \
    if (UNLIKELY(!_status.ok())) { \
      ABORT_WITH_ERROR(_status.GetDetail()); \
    } \
  } while (false)

// Log to FATAL and abort process, generating a core dump if enabled. This should be used
// for unexpected error cases where we want a core dump.
// LOG(FATAL) will call abort().
#define ABORT_WITH_ERROR(msg) \
  do { \
    LOG(FATAL) << msg << ". Impalad exiting.\n"; \
  } while (false)

// Log to ERROR and exit process with status 1 without calling abort() or dumping core.
// This should be used for expected error cases, e.g. bad command-line arguments where
// we just want to exit cleanly without generating core dumps.
#define CLEAN_EXIT_WITH_ERROR(msg) \
  do { \
    LOG(ERROR) << msg << ". Impalad exiting.\n"; \
    google::FlushLogFiles(google::ERROR); \
    exit(1); \
  } while (false)

/// This macro can be appended to a function definition to generate a compiler warning
/// if the result is ignored.
/// TODO: when we upgrade gcc from 4.9.2, we may be able to apply this to the Status
/// type to get this automatically for all Status-returning functions.
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
}

#endif
