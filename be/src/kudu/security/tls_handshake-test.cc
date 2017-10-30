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

#include "kudu/security/tls_handshake.h"

#include <atomic>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/optional.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/security/ca/cert_management.h"
#include "kudu/security/crypto.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

DECLARE_int32(ipki_server_key_size);

namespace kudu {
namespace security {

using ca::CertSigner;

struct Case {
  PkiConfig client_pki;
  TlsVerificationMode client_verification;
  PkiConfig server_pki;
  TlsVerificationMode server_verification;
  Status expected_status;
};

// Beautifies CLI test output.
std::ostream& operator<<(std::ostream& o, Case c) {
  auto verification_mode_name = [] (const TlsVerificationMode& verification_mode) {
    switch (verification_mode) {
      case TlsVerificationMode::VERIFY_NONE: return "NONE";
      case TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST: return "REMOTE_CERT_AND_HOST";
    }
    return "unreachable";
  };

  o << "{client-pki: " << c.client_pki << ", "
    << "client-verification: " << verification_mode_name(c.client_verification) << ", "
    << "server-pki: " << c.server_pki << ", "
    << "server-verification: " << verification_mode_name(c.server_verification) << ", "
    << "expected-status: " << c.expected_status.ToString() << "}";

  return o;
}

class TestTlsHandshakeBase : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    ASSERT_OK(client_tls_.Init());
    ASSERT_OK(server_tls_.Init());
  }

 protected:
  // Run a handshake using 'client_tls_' and 'server_tls_'. The client and server
  // verification modes are set to 'client_verify' and 'server_verify' respectively.
  Status RunHandshake(TlsVerificationMode client_verify,
                      TlsVerificationMode server_verify) {
    TlsHandshake client, server;
    RETURN_NOT_OK(client_tls_.InitiateHandshake(TlsHandshakeType::CLIENT, &client));
    RETURN_NOT_OK(server_tls_.InitiateHandshake(TlsHandshakeType::SERVER, &server));

    client.set_verification_mode(client_verify);
    server.set_verification_mode(server_verify);

    bool client_done = false, server_done = false;
    string to_client;
    string to_server;
    while (!client_done || !server_done) {
      if (!client_done) {
        Status s = client.Continue(to_client, &to_server);
        VLOG(1) << "client->server: " << to_server.size() << " bytes";
        if (s.ok()) {
          client_done = true;
        } else if (!s.IsIncomplete()) {
          CHECK(s.IsRuntimeError());
          return s.CloneAndPrepend("client error");
        }
      }
      if (!server_done) {
        CHECK(!client_done);
        Status s = server.Continue(to_server, &to_client);
        VLOG(1) << "server->client: " << to_client.size() << " bytes";
        if (s.ok()) {
          server_done = true;
        } else if (!s.IsIncomplete()) {
          CHECK(s.IsRuntimeError());
          return s.CloneAndPrepend("server error");
        }
      }
    }
    return Status::OK();
  }

  TlsContext client_tls_;
  TlsContext server_tls_;

  string cert_path_;
  string key_path_;
};

class TestTlsHandshake : public TestTlsHandshakeBase,
                   public ::testing::WithParamInterface<Case> {};

class TestTlsHandshakeConcurrent : public TestTlsHandshakeBase,
                   public ::testing::WithParamInterface<int> {};

// Test concurrently running handshakes while changing the certificates on the TLS
// context. We parameterize across different numbers of threads, because surprisingly,
// fewer threads seems to trigger issues more easily in some cases.
INSTANTIATE_TEST_CASE_P(NumThreads, TestTlsHandshakeConcurrent, ::testing::Values(1, 2, 4, 8));
TEST_P(TestTlsHandshakeConcurrent, TestConcurrentAdoptCert) {
  const int kNumThreads = GetParam();

  ASSERT_OK(server_tls_.GenerateSelfSignedCertAndKey());
  std::atomic<bool> done(false);
  vector<std::thread> handshake_threads;
  for (int i = 0; i < kNumThreads; i++) {
    handshake_threads.emplace_back([&]() {
        while (!done) {
          RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE);
        }
      });
  }
  auto c = MakeScopedCleanup([&](){
      done = true;
      for (std::thread& t : handshake_threads) {
        t.join();
      }
    });

  SleepFor(MonoDelta::FromMilliseconds(10));
  {
    PrivateKey ca_key;
    Cert ca_cert;
    ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));
    Cert cert;
    ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(*server_tls_.GetCsrIfNecessary(), &cert));
    ASSERT_OK(server_tls_.AddTrustedCertificate(ca_cert));
    ASSERT_OK(server_tls_.AdoptSignedCert(cert));
  }
  SleepFor(MonoDelta::FromMilliseconds(10));
}

TEST_F(TestTlsHandshake, TestHandshakeSequence) {
  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  // Both client and server have certs and CA.
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, &client_tls_));
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, &server_tls_));

  TlsHandshake server;
  TlsHandshake client;
  ASSERT_OK(client_tls_.InitiateHandshake(TlsHandshakeType::SERVER, &server));
  ASSERT_OK(server_tls_.InitiateHandshake(TlsHandshakeType::CLIENT, &client));

  string buf1;
  string buf2;

  // Client sends Hello
  ASSERT_TRUE(client.Continue(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Hello, and sends server Hello
  ASSERT_TRUE(server.Continue(buf2, &buf1).IsIncomplete());
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Hello and sends client Finished
  ASSERT_TRUE(client.Continue(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Finished and sends server Finished
  ASSERT_OK(server.Continue(buf2, &buf1));
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Finished
  ASSERT_OK(client.Continue(buf1, &buf2));
  ASSERT_EQ(buf2.size(), 0);
}

// Tests that the TlsContext can transition from self signed cert to signed
// cert, and that it rejects invalid certs along the way. We are testing this
// here instead of in a dedicated TlsContext test because it requires completing
// handshakes to fully validate.
TEST_F(TestTlsHandshake, TestTlsContextCertTransition) {
  ASSERT_FALSE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_EQ(boost::none, server_tls_.GetCsrIfNecessary());

  ASSERT_OK(server_tls_.GenerateSelfSignedCertAndKey());
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_NE(boost::none, server_tls_.GetCsrIfNecessary());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));
  ASSERT_STR_MATCHES(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                                  TlsVerificationMode::VERIFY_NONE).ToString(),
                     "client error:.*certificate verify failed");

  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  Cert cert;
  ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(*server_tls_.GetCsrIfNecessary(), &cert));

  // Try to adopt the cert without first trusting the CA.
  ASSERT_STR_MATCHES(server_tls_.AdoptSignedCert(cert).ToString(),
                     "could not verify certificate chain");

  // Check that we can still do (unverified) handshakes.
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));

  // Trust the root cert.
  ASSERT_OK(server_tls_.AddTrustedCertificate(ca_cert));

  // Generate a bogus cert and attempt to adopt it.
  Cert bogus_cert;
  {
    TlsContext bogus_tls;
    ASSERT_OK(bogus_tls.Init());
    ASSERT_OK(bogus_tls.GenerateSelfSignedCertAndKey());
    ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(*bogus_tls.GetCsrIfNecessary(), &bogus_cert));
  }
  ASSERT_STR_MATCHES(server_tls_.AdoptSignedCert(bogus_cert).ToString(),
                     "certificate public key does not match the CSR public key");

  // Check that we can still do (unverified) handshakes.
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));

  // Adopt the legitimate signed cert.
  ASSERT_OK(server_tls_.AdoptSignedCert(cert));

  // Check that we can do verified handshakes.
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_TRUE(server_tls_.has_signed_cert());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));
  ASSERT_OK(client_tls_.AddTrustedCertificate(ca_cert));
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                         TlsVerificationMode::VERIFY_NONE));
}

TEST_P(TestTlsHandshake, TestHandshake) {
  Case test_case = GetParam();

  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  ASSERT_OK(ConfigureTlsContext(test_case.client_pki, ca_cert, ca_key, &client_tls_));
  ASSERT_OK(ConfigureTlsContext(test_case.server_pki, ca_cert, ca_key, &server_tls_));

  Status s = RunHandshake(test_case.client_verification, test_case.server_verification);

  EXPECT_EQ(test_case.expected_status.CodeAsString(), s.CodeAsString());
  ASSERT_STR_MATCHES(s.ToString(), test_case.expected_status.message().ToString());
}

INSTANTIATE_TEST_CASE_P(CertCombinations,
                        TestTlsHandshake,
                        ::testing::Values(

        // We don't test any cases where the server has no cert or the client
        // has a self-signed cert, since we don't expect those to occur in
        // practice.

        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },

        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               // OpenSSL 1.0.0 returns "no certificate returned" for this case,
               // which appears to be a bug.
               Status::RuntimeError("server error:.*(certificate verify failed|"
                                                    "no certificate returned)") },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::OK() }
));

} // namespace security
} // namespace kudu
