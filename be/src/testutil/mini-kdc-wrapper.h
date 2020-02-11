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

#ifndef IMPALA_MINI_KDC_WRAPPER_H_
#define IMPALA_MINI_KDC_WRAPPER_H_

#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/status.h"
#include "kudu/security/test/mini_kdc.h"

namespace impala {

enum KerberosSwitch {
  KERBEROS_OFF,
  KERBEROS_ON
};

/// This class allows tests to easily start and stop a KDC and configure Impala's auth
/// layer. A MiniKdc which is a wrapper around the 'krb5kdc' process, is configured and
/// started.
class MiniKdcWrapper {
 public:
  /// This function creates the 'unique_test_dir_' path, starts the KDC and sets the
  /// appropriate flags that Impala requires to run with Kerberos. The newly created
  /// KDC is stored in 'kdc_ptr'. Return error status on failure.
  static Status SetupAndStartMiniKDC(std::string realm,
      std::string ticket_lifetime, std::string renew_lifetime,
      int kdc_port, std::unique_ptr<MiniKdcWrapper>* kdc_ptr);

  /// Undoes everything done by SetupAndStartMiniKDC().
  Status TearDownMiniKDC();

  /// Kinit a user to the mini KDC.
  Status Kinit(const std::string& username);

  /// Creates a new user with the given username.
  /// The password is the same as the username.
  Status CreateUserPrincipal(const std::string& username);

  /// Creates a keytab file under the 'unique_test_dir_' path which is configured to
  /// authenticate the service principal 'spn'. The path to the file is returned as a
  /// string in 'kt_path'.
  Status CreateServiceKeytab(const std::string& spn, std::string* kt_path);

  /// Returns the environment variable ""KRB5CCNAME" configured in the setup of mini-kdc.
  const std::string GetKrb5CCname() const {
    return kdc_->GetEnvVars()["KRB5CCNAME"];
  }

 private:
  boost::scoped_ptr<kudu::MiniKdc> kdc_;

  /// The name of the kerberos realm to setup.
  const std::string realm_;

  /// The lifetime of the kerberos ticket.
  const std::string ticket_lifetime_;

  /// The renew lifetime of the kerberos ticket.
  const std::string renew_lifetime_;

  /// Port to have the KDC process listen on.
  const int kdc_port_;

  /// Create a unique directory for this test to store its files in.
  boost::filesystem::path unique_test_dir_ = boost::filesystem::unique_path();

  /// Called by SetupAndStartMiniKDC() only.
  MiniKdcWrapper(std::string&& realm, std::string&& ticket_lifetime,
      std::string&& renew_lifetime, int kdc_port)
    : realm_(std::move(realm)),
      ticket_lifetime_(std::move(ticket_lifetime)),
      renew_lifetime_(std::move(renew_lifetime)),
      kdc_port_(kdc_port) {
  }

  /// Starts the KDC and configures it to use 'keytab_dir' as the location to store the
  /// keytab. The 'keytab_dir' will not be cleaned up by this class.
  Status StartKdc(std::string keytab_dir);

  /// Stops the KDC by terminating the krb5kdc subprocess.
  Status StopKdc();
};

}

#endif /* IMPALA_MINI_KDC_WRAPPER_H_ */
