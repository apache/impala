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


#ifndef IMPALA_COMMON_STATUS_H
#define IMPALA_COMMON_STATUS_H

#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

#include "common/logging.h"
#include "common/compiler-util.h"
#include "gen-cpp/Status_types.h"  // for TStatus
#include "gen-cpp/ErrorCodes_types.h"  // for TErrorCode
#include "gen-cpp/TCLIService_types.h" // for HS2 TStatus
#include "util/error-util.h" // for ErrorMessage

#define STATUS_API_VERSION 2

namespace impala {

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
///
/// TODO: macros:
/// RETURN_IF_ERROR(status) << "msg"
/// MAKE_ERROR() << "msg"
class Status {
 public:
  typedef strings::internal::SubstituteArg ArgType;

  Status(): msg_(NULL) {}

  // Return a default constructed Status instance in the OK case.
  inline static Status OK() { return Status(); }

  // Return a MEM_LIMIT_EXCEEDED error status.
  static Status MemLimitExceeded();

  static const Status CANCELLED;
  static const Status DEPRECATED_RPC;

  /// Copy c'tor makes copy of error detail so Status can be returned by value.
  Status(const Status& status)
    : msg_(status.msg_ != NULL ? new ErrorMsg(*status.msg_) : NULL) { }

  /// Status using only the error code as a parameter. This can be used for error messages
  /// that don't take format parameters.
  Status(TErrorCode::type code);

  /// These constructors are used if the caller wants to indicate a non-successful
  /// execution and supply a client-facing error message. This is the preferred way of
  /// instantiating a non-successful Status.
  Status(TErrorCode::type error, const ArgType& arg0);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
      const ArgType& arg8);
  Status(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
      const ArgType& arg8, const ArgType& arg9);

  /// Used when the ErrorMsg is created as an intermediate value that is either passed to
  /// the Status or to the RuntimeState.
  Status(const ErrorMsg& e);

  /// This constructor creates a Status with a default error code of GENERAL and is not
  /// intended for statuses that might be client-visible.
  /// TODO: deprecate
  Status(const std::string& error_msg);

  /// Create a status instance that represents an expected error and will not be logged
  static Status Expected(const std::string& error_msg);

  ~Status() {
    if (msg_ != NULL) delete msg_;
  }

  /// same as copy c'tor
  Status& operator=(const Status& status) {
    delete msg_;
    if (LIKELY(status.msg_ == NULL)) {
      msg_ = NULL;
    } else {
      msg_ = new ErrorMsg(*status.msg_);
    }
    return *this;
  }

  /// "Copy" c'tor from TStatus.
  /// Retains the TErrorCode value and the message
  Status(const TStatus& status);

  /// same as previous c'tor
  /// Retains the TErrorCode value and the message
  Status& operator=(const TStatus& status);

  /// "Copy c'tor from HS2 TStatus.
  /// Retains the TErrorCode value and the message
  Status(const apache::hive::service::cli::thrift::TStatus& hs2_status);

  /// same as previous c'tor
  /// Retains the TErrorCode value and the message
  Status& operator=(const apache::hive::service::cli::thrift::TStatus& hs2_status);

  bool ok() const { return msg_ == NULL; }

  bool IsCancelled() const {
    return msg_ != NULL && msg_->error() == TErrorCode::CANCELLED;
  }

  bool IsMemLimitExceeded() const {
    return msg_ != NULL
        && msg_->error() == TErrorCode::MEM_LIMIT_EXCEEDED;
  }

  bool IsRecoverableError() const {
    return msg_ != NULL
        && msg_->error() == TErrorCode::RECOVERABLE_ERROR;
  }

  /// Returns the error message associated with a non-successful status.
  const ErrorMsg& msg() const {
    DCHECK(msg_ != NULL);
    return *msg_;
  }

  /// Sets the ErrorMessage on the detail of the status. Calling this method is only valid
  /// if an error was reported.
  /// TODO: deprecate, error should be immutable
  void SetErrorMsg(const ErrorMsg& m) {
    DCHECK(msg_ != NULL);
    delete msg_;
    msg_ = new ErrorMsg(m);
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

  /// Returns the formatted message of the error message and the individual details of the
  /// additional messages as a single string. This should only be called internally and
  /// not to report an error back to the client.
  const std::string GetDetail() const;

  TErrorCode::type code() const {
    return msg_ == NULL ? TErrorCode::OK : msg_->error();
  }

 private:

  /// Silent general error, this cannot be used with typed error messages as it would defy
  /// the cause of a useful error message.
  Status(const std::string& error_msg, bool silent);

  /// Status uses a naked pointer to ensure the size of an instance on the stack is only
  /// the sizeof(ErrorMsg*). Every Status owns its ErrorMsg instance.
  ErrorMsg* msg_;
};

/// some generally useful macros
#define RETURN_IF_ERROR(stmt) \
  do { \
    Status __status__ = (stmt); \
    if (UNLIKELY(!__status__.ok())) return __status__; \
  } while (false)

#define EXIT_IF_ERROR(stmt) \
  do { \
    Status __status__ = (stmt); \
    if (UNLIKELY(!__status__.ok())) { \
      EXIT_WITH_ERROR(__status__.GetDetail()); \
    } \
  } while (false)

#define EXIT_WITH_ERROR(msg) \
  do { \
    LOG(ERROR) << msg; \
    exit(1); \
  } while (false)

}

#endif
