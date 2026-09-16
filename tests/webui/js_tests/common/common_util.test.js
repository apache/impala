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

import {describe, test, expect} from "@jest/globals";
import {getReadableSize, renderSize} from "scripts/common_util.js";

describe("webui.js_tests.common_util.getReadableSize", () => {
  // DataTable render callbacks read cell content from the DOM as strings, so
  // getReadableSize must accept numeric strings of any magnitude.
  test("accepts_numeric_strings", () => {
    expect(getReadableSize("0")).toBe("0.00 B");
    expect(getReadableSize("512")).toBe("512.00 B");
    expect(getReadableSize("999")).toBe("999.00 B");
    expect(getReadableSize("1000")).toBe("1.00 KB");
    expect(getReadableSize("212434233")).toBe("212.43 MB");
  });

  test("accepts_numbers", () => {
    expect(getReadableSize(0)).toBe("0.00 B");
    expect(getReadableSize(512)).toBe("512.00 B");
    expect(getReadableSize(1500)).toBe("1.50 KB");
  });
});
