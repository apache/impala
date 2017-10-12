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
  USE_KUDU_KERBEROS,    // FLAGS_use_kudu_kinit = true
  USE_IMPALA_KERBEROS   // FLAGS_use_kudu_kinit = false
};

/// This class allows tests to easily start and stop a KDC and configure Impala's auth
/// layer.
/// If the mode is USE_KUDU_KERBEROS or USE_IMPALA_KERBEROS, the MiniKdc which is a
/// wrapper around the 'krb5kdc' process, is configured and started.
/// If the mode is KERBEROS_OFF, Impala's auth layer is configured to use plain SASL and
/// the KDC is not started.
class MiniKdcWrapper {
 public:
  MiniKdcWrapper(std::string spn, std::string realm, std::string ticket_lifetime,
    std::string renew_lifetime,int kdc_port) :
      spn_(spn),
      realm_(realm),
      ticket_lifetime_(ticket_lifetime),
      renew_lifetime_(renew_lifetime),
      kdc_port_(kdc_port) {
  }

  /// If 'k' is 'USE_KUDU_KERBEROS' or 'USE_IMPALA_KERBEROS', this function creates the
  /// 'unique_test_dir_' path, starts the KDC and sets the appropriate flags that Impala
  /// requires to run with Kerberos.
  Status SetupAndStartMiniKDC(KerberosSwitch k);

  /// Undoes everything done by SetupAndStartMiniKDC().
  Status TearDownMiniKDC(KerberosSwitch k);

 private:
  boost::scoped_ptr<kudu::MiniKdc> kdc_;

  /// The service's principal name.
  const std::string spn_;

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

  /// Starts the KDC and configures it to use 'keytab_dir' as the location to store the
  /// keytab. The 'keytab_dir' will not be cleaned up by this class.
  Status StartKdc(string keytab_dir);

  /// Stops the KDC by terminating the krb5kdc subprocess.
  Status StopKdc();

  /// Creates a keytab file under the 'unique_test_dir_' path which is configured to
  /// authenticate the service principal 'spn_'. The path to the file is returned as a
  /// string in 'kt_path'.
  Status CreateServiceKeytab(const string& spn, string* kt_path);
};

}

#endif /* IMPALA_MINI_KDC_WRAPPER_H_ */
