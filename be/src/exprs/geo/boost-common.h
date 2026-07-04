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

#pragma once

#include <boost/geometry/geometry.hpp>

#include "exprs/geo/common.h"

namespace impala::geo {

namespace bg = boost::geometry;

using point2d = bg::model::d2::point_xy<double>;
using box2d = bg::model::box<point2d>;
using linestring2d = bg::model::linestring<point2d>;
using polygon2d = bg::model::polygon<point2d, true>;
using multipoint2d = bg::model::multi_point<point2d>;
using multi_linestring2d = bg::model::multi_linestring<linestring2d>;
using multi_polygon2d = bg::model::multi_polygon<polygon2d>;

} // namespace impala::geo
