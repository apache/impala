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

//
/// This file contains type definitions that are used throughout the code base.

#pragma once

namespace cctz {
class time_zone;
};

namespace impala {

class UniqueIdPB;

// Used to uniquely identify individual impalads.
typedef UniqueIdPB BackendIdPB;

typedef cctz::time_zone Timezone;

// There is also a "proper" Timezone for UTC, but using nullptr makes some
// optimizations more convenient.
constexpr const Timezone * UTCPTR = nullptr;

/// for now, these are simply ints; if we find we need to generate ids in the
/// backend, we can also introduce separate classes for these to make them
/// assignment-incompatible
typedef int TupleId;
typedef int SlotId;
typedef int TableId;
typedef int PlanNodeId;
typedef int FragmentIdx;
};
