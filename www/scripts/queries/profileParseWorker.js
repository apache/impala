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

importScripts("../../pako.min.js");
importScripts("../common_util.js");

self.onmessage = (e) => {
  var query = {};
  try {
    var profile = JSON.parse(e.data).contents;
    var val = profile.profile_name;
    query.id = val.substring(val.indexOf("=") + 1, val.length - 1);
    query.user = profile.child_profiles[0].info_strings
        .find(({key}) => key === "User").value;
    query.default_db = profile.child_profiles[0].info_strings
        .find(({key}) => key === "Default Db").value;
    query.type = profile.child_profiles[0].info_strings
        .find(({key}) => key === "Query Type").value;
    query.start_time = profile.child_profiles[0].info_strings
        .find(({key}) => key === "Start Time").value;
    query.end_time = profile.child_profiles[0].info_strings
        .find(({key}) => key === "End Time").value;
    query.bytes_read = profile.child_profiles[2].counters
        .find(({counter_name}) => counter_name === "TotalBytesRead").value;
    query.bytes_read = getReadableSize(query.bytes_read, 2);
    query.bytes_sent = profile.child_profiles[2].counters
        .find(({counter_name}) => counter_name === "TotalBytesSent").value;
    query.bytes_sent = getReadableSize(query.bytes_sent, 2);
    query.state = profile.child_profiles[0].info_strings
        .find(({key}) => key === "Query State").value;
    query.rows_fetched = profile.child_profiles[1].counters
        .find(({counter_name}) => counter_name === "NumRowsFetched").value;
    query.resource_pool = profile.child_profiles[0].info_strings
        .find(({key}) => key === "Request Pool").value;
    query.statement = profile.child_profiles[0].info_strings
        .find(({key}) => key === "Sql Statement").value;
    query.statement = query.statement.substring(0, 250) + "...";
    query.profile = pako.deflate(e.data, {level : 3});
  } catch (err) {
    query.error = err;
  }
  self.postMessage(query);
}