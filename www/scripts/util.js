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

function getReadableSize(bytes) {
    var units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB'];
    if (bytes <= 0) return bytes + ' B';
    var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
    return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

/*
 * Useful render function used in DataTable. It renders the size
 * value into human readable format in 'display' and 'filter' modes,
 * and uses the original value in 'sort' mode.
 */
function renderSize(data, type, row) {
    // If display or filter data is requested, format the data
    if (type === 'display' || type === 'filter') {
        return getReadableSize(data);
    }
    return data;
}

