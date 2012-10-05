// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_STATUS_H
#define IMPALA_COMMON_STATUS_H

#include <string>
#include <vector>

#include "common/logging.h"
#include "common/compiler-util.h"
#include "gen-cpp/Status_types.h"  // for TStatus

namespace impala {

// Status is used as a function return type to indicate success, failure or cancellation
// of the function. In case of successful completion, it only occupies sizeof(void*)
// statically allocated memory. In the error case, it records a stack of error messages.
//
// example:
// Status fnB(int x) {
//   Status status = fnA(x);
//   if (!status.ok()) {
//     status.AddErrorMsg("fnA(x) went wrong");
//     return status;
//   }
// }
//
// TODO: macros:
// RETURN_IF_ERROR(status) << "msg"
// MAKE_ERROR() << "msg"

class Status {
 public:
  Status(): error_detail_(NULL) {}

  static const Status OK;
  static const Status CANCELLED;

  // copy c'tor makes copy of error detail so Status can be returned by value
  Status(const Status& status)
    : error_detail_(
        status.error_detail_ != NULL
          ? new ErrorDetail(*status.error_detail_)
          : NULL) {
  }

  // c'tor for error case - is this useful for anything other than CANCELLED?
  Status(TStatusCode::type code)
    : error_detail_(new ErrorDetail(code)) {
  }

  // c'tor for error case
  Status(TStatusCode::type code, const std::string& error_msg)
    : error_detail_(new ErrorDetail(code, error_msg)) {
    VLOG(2) << error_msg;
  }

  // c'tor for internal error
  Status(const std::string& error_msg);

  ~Status() {
    if (error_detail_ != NULL) delete error_detail_;
  }


  // same as copy c'tor
  Status& operator=(const Status& status);

  // "Copy" c'tor from TStatus.
  Status(const TStatus& status);

  // same as previous c'tor
  Status& operator=(const TStatus& status);

  // assign from stringstream
  Status& operator=(const std::stringstream& stream);

  bool ok() const { return error_detail_ == NULL; }

  bool IsCancelled() const {
    return error_detail_ != NULL
        && error_detail_->error_code == TStatusCode::CANCELLED;
  }

  // Add an error message and set the code if no code has been set yet.
  // If a code has already been set, 'code' is ignored.
  void AddErrorMsg(TStatusCode::type code, const std::string& msg);

  // Add an error message and set the code to INTERNAL_ERROR if no code has been
  // set yet. If a code has already been set, it is left unchanged.
  void AddErrorMsg(const std::string& msg);

  // Does nothing if status.ok().
  // Otherwise: if 'this' is an error status, adds the error msg from 'status;
  // otherwise assigns 'status'.
  void AddError(const Status& status);

  // Return all accumulated error msgs.
  void GetErrorMsgs(std::vector<std::string>* msgs) const;

  // Convert into TStatus. Call this if 'status_container' contains an optional
  // TStatus field named 'status'. This also sets __isset.status.
  template <typename T> void SetTStatus(T* status_container) const {
    ToThrift(&status_container->status);
    status_container->__isset.status = true;
  }

  // Convert into TStatus.
  void ToThrift(TStatus* status) const;

  // Return all accumulated error msgs in a single string.
  void GetErrorMsg(std::string* msg) const;

  std::string GetErrorMsg() const;

  TStatusCode::type code() const {
    return error_detail_ == NULL ? TStatusCode::OK : error_detail_->error_code;
  }

 private:
  struct ErrorDetail {
    TStatusCode::type error_code;  // anything other than OK
    std::vector<std::string> error_msgs;

    ErrorDetail(const TStatus& status);
    ErrorDetail(TStatusCode::type code)
      : error_code(code) {}
    ErrorDetail(TStatusCode::type code, const std::string& msg)
      : error_code(code), error_msgs(1, msg) {}
  };

  ErrorDetail* error_detail_;
};

// some generally useful macros
#define RETURN_IF_ERROR(stmt) \
  do { \
    Status __status__ = (stmt); \
    if (UNLIKELY(!__status__.ok())) return __status__; \
  } while (false)

#define EXIT_IF_ERROR(stmt) \
  do { \
    Status __status__ = (stmt); \
    if (UNLIKELY(!__status__.ok())) { \
      string msg; \
      __status__.GetErrorMsg(&msg); \
      LOG(ERROR) << msg;            \
      exit(1); \
    } \
  } while (false)

}

#endif
