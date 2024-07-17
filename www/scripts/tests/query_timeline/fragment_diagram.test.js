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

import {describe, test, expect} from '@jest/globals';
import {exportedForTest} from "../../query_timeline/fragment_diagram.js";

describe("Test getSvg*", () => {
  // Test whether getSvg* methods correctly set attributes and return expected elements
  var {getSvgRect, getSvgLine, getSvgText, getSvgTitle, getSvgGroup} = exportedForTest;
  var stroke_fill_colors = { black : "#000000", dark_grey : "#505050",
      light_grey : "#F0F0F0", transperent : "rgba(0, 0, 0, 0)" };

  test("Test getSvgRect", () => {
    expect(getSvgRect(stroke_fill_colors.transperent, 0, 0, 100, 100, "2 2",
        stroke_fill_colors.black).outerHTML).toBe(
          '<rect x="0" y="0" width="100" height="100"'
        + ` fill="${stroke_fill_colors.transperent}"`
        + ` stroke-width="0.5"`
        + ` stroke="${stroke_fill_colors.black}"`
        + ` stroke-dasharray="2 2"></rect>`);
  });

  test("Test getSvgLine", () => {
    expect(getSvgLine(stroke_fill_colors.black, 0, 0, 100, 100, true).outerHTML).toBe(
          '<line x1="0" y1="0" x2="100" y2="100"'
        + ` stroke="${stroke_fill_colors.black}"`
        + ' stroke-dasharray="2 2"></line>');
  });

  test("Test getSvgText", () => {
    expect(getSvgText("Text", stroke_fill_colors.black, 0, 0, 15, true, 300)
        .outerHTML).toBe(
        '<text x="0" y="0" style="font-size: 10px;" dominant-baseline="middle" '
        + `text-anchor="middle" fill="${stroke_fill_colors.black}" textLength="300" `
        + 'lengthAdjust="spacingAndGlyphs">Text</text>');
  });

  test("Test getSvgTitle", () => {
    expect(getSvgTitle("Title").outerHTML).toBe("<title>Title</title>");
  });

  test("Test getSvgGroup", () => {
    expect(getSvgGroup().outerHTML).toBe("<g></g>");
  });
});
