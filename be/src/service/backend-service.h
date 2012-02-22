// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_BACKEND_SERVICE_H
#define IMPALA_SERVICE_BACKEND_SERVICE_H

namespace apache { namespace thrift { namespace server { class TServer; } } }

namespace impala {

class ExecEnv;

// Start a Thrift server exporting ImpalaBackendService on given port.
apache::thrift::server::TServer* StartImpalaBackendService(ExecEnv* exec_env, int port);

}

#endif
