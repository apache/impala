// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_STATUS_H
#define IMPALA_COMMON_STATUS_H

#include <string>
#include <vector>

namespace impala {

// Status is used as a function return type to indicate success or failure
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

  // c'tor for error case
  Status(const std::string& error_msg);

  // copy c'tor makes copy of error detail so Status can be returned by value
  Status(const Status& status);

  // assign from stringstream
  Status& operator=(const std::stringstream& stream);

  ~Status();

  bool ok() { return error_detail_ == NULL; }

  void AddErrorMsg(const std::string& msg);

  // Return all accumulated error msgs.
  void GetErrorMsgs(std::vector<std::string>* msgs) const;

  // Return all accumulated error msgs in a single string.
  void GetErrorMsg(std::string* msg) const;

 private:
  struct ErrorDetail;
  ErrorDetail* error_detail_;
};

// some generally useful macros
#define RETURN_IF_ERROR(stmt) \
  do { Status status = (stmt); if (!status.ok()) return status; } while (false)

}

#endif
