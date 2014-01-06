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

#include "statestore/statestore-subscriber.h"
#include "common/init.h"

using namespace impala;

DECLARE_int32(state_store_port);
DECLARE_int32(state_store_subscriber_port);

// Used by statestore tests to connect a subscriber to an existing statestore. Exits with
// an error if a connection could not be made.
int main(int argc, char **argv) {
  InitCommonRuntime(argc, argv, false);
  Metrics metrics;
  StatestoreSubscriber subscriber("subscriber1",
      MakeNetworkAddress("localhost", FLAGS_state_store_subscriber_port),
      MakeNetworkAddress("localhost", FLAGS_state_store_port), &metrics);
  EXIT_IF_ERROR(subscriber.Start());
  exit(0);
}
