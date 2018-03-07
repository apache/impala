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


// Note: The results do not include the pre-processing in the prepare function that is
// necessary for SetLookup but not Iterate. None of the values searched for are in the
// fabricated IN list (i.e. hit rate is 0).

//Machine Info: Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
//tinyInt n=1:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           3.29e+03 3.34e+03 3.38e+03         1X         1X         1X
//                        Iterate n=1           9.48e+03 9.56e+03 9.64e+03      2.88X      2.87X      2.85X
//
//tinyInt n=2:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           2.91e+03 2.99e+03 3.02e+03         1X         1X         1X
//                        Iterate n=2           6.95e+03 7.08e+03 7.14e+03      2.39X      2.37X      2.37X
//
//tinyInt n=3:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           2.54e+03  2.6e+03 2.65e+03         1X         1X         1X
//                        Iterate n=3           4.89e+03 5.01e+03 5.08e+03      1.93X      1.93X      1.92X
//
//tinyInt n=4:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           2.89e+03    3e+03 3.04e+03         1X         1X         1X
//                        Iterate n=4           3.71e+03 3.79e+03 3.84e+03      1.28X      1.26X      1.26X
//
//tinyInt n=5:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           2.64e+03 2.74e+03 2.78e+03         1X         1X         1X
//                        Iterate n=5            3.5e+03 3.57e+03 3.61e+03      1.32X       1.3X       1.3X
//
//tinyInt n=6:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           2.62e+03 2.67e+03 2.69e+03         1X         1X         1X
//                        Iterate n=6           2.73e+03 2.76e+03 2.79e+03      1.04X      1.04X      1.04X
//
//tinyInt n=7:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           2.39e+03 2.44e+03 2.46e+03         1X         1X         1X
//                        Iterate n=7           2.48e+03 2.51e+03 2.54e+03      1.04X      1.03X      1.03X
//
//tinyInt n=8:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           2.81e+03 2.85e+03 2.89e+03         1X         1X         1X
//                        Iterate n=8           2.08e+03 2.12e+03 2.14e+03     0.741X     0.743X     0.741X
//
//tinyInt n=9:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           2.66e+03 2.75e+03  2.8e+03         1X         1X         1X
//                        Iterate n=9           1.69e+03 1.71e+03 1.73e+03     0.635X     0.624X     0.617X
//
//tinyInt n=10:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10            2.8e+03 2.89e+03 2.92e+03         1X         1X         1X
//                       Iterate n=10           1.54e+03 1.57e+03 1.58e+03     0.548X     0.543X     0.542X
//
//tinyInt n=400:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           2.51e+03 2.61e+03 2.64e+03         1X         1X         1X
//                      Iterate n=400                 84     84.2     85.7    0.0335X    0.0323X    0.0325X
//
//smallInt n=1:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           3.16e+03 3.28e+03 3.33e+03         1X         1X         1X
//                        Iterate n=1           1.03e+04 1.04e+04 1.05e+04      3.27X      3.18X      3.16X
//
//smallInt n=2:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           2.94e+03 3.02e+03 3.06e+03         1X         1X         1X
//                        Iterate n=2           7.03e+03 7.12e+03 7.18e+03      2.39X      2.36X      2.35X
//
//smallInt n=3:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           2.64e+03 2.73e+03 2.78e+03         1X         1X         1X
//                        Iterate n=3           5.33e+03  5.4e+03 5.45e+03      2.02X      1.98X      1.96X
//
//smallInt n=4:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           2.96e+03 3.04e+03 3.08e+03         1X         1X         1X
//                        Iterate n=4           4.06e+03 4.09e+03 4.14e+03      1.37X      1.34X      1.35X
//
//smallInt n=5:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           2.72e+03 2.81e+03 2.86e+03         1X         1X         1X
//                        Iterate n=5           3.59e+03 3.63e+03 3.66e+03      1.32X      1.29X      1.28X
//
//smallInt n=6:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           2.68e+03 2.79e+03 2.85e+03         1X         1X         1X
//                        Iterate n=6            2.9e+03 2.98e+03 3.02e+03      1.08X      1.07X      1.06X
//
//smallInt n=7:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           2.35e+03 2.56e+03  2.6e+03         1X         1X         1X
//                        Iterate n=7           2.63e+03 2.73e+03 2.76e+03      1.12X      1.07X      1.06X
//
//smallInt n=8:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           2.78e+03 2.88e+03 2.91e+03         1X         1X         1X
//                        Iterate n=8           2.33e+03 2.36e+03 2.38e+03     0.839X     0.818X     0.816X
//
//smallInt n=9:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           2.76e+03 2.83e+03 2.87e+03         1X         1X         1X
//                        Iterate n=9           1.79e+03 1.82e+03 1.85e+03     0.648X     0.642X     0.644X
//
//smallInt n=10:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           2.83e+03 2.88e+03 2.91e+03         1X         1X         1X
//                       Iterate n=10           1.61e+03 1.64e+03 1.66e+03     0.567X      0.57X      0.57X
//
//smallInt n=400:            Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           2.62e+03 2.68e+03 2.71e+03         1X         1X         1X
//                      Iterate n=400               48.7     49.4     49.9    0.0186X    0.0185X    0.0184X
//
//int n=1:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           4.66e+03 4.75e+03 4.85e+03         1X         1X         1X
//                        Iterate n=1           1.03e+04 1.04e+04 1.05e+04      2.21X      2.19X      2.17X
//
//int n=2:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2            4.1e+03 4.27e+03 4.34e+03         1X         1X         1X
//                        Iterate n=2           6.62e+03 6.94e+03 7.02e+03      1.61X      1.62X      1.62X
//
//int n=3:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           3.78e+03 3.89e+03 3.94e+03         1X         1X         1X
//                        Iterate n=3           5.21e+03 5.29e+03 5.34e+03      1.38X      1.36X      1.35X
//
//int n=4:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           3.16e+03 3.28e+03 3.35e+03         1X         1X         1X
//                        Iterate n=4            3.9e+03    4e+03 4.07e+03      1.23X      1.22X      1.22X
//
//int n=5:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           3.29e+03 3.39e+03 3.45e+03         1X         1X         1X
//                        Iterate n=5           3.39e+03  3.5e+03 3.53e+03      1.03X      1.03X      1.02X
//
//int n=6:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6            3.3e+03 3.39e+03 3.44e+03         1X         1X         1X
//                        Iterate n=6            2.9e+03 2.97e+03 2.99e+03     0.881X     0.874X     0.869X
//
//int n=7:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           3.06e+03 3.18e+03 3.21e+03         1X         1X         1X
//                        Iterate n=7           2.67e+03 2.72e+03 2.74e+03     0.872X     0.855X     0.853X
//
//int n=8:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           2.69e+03 2.72e+03 2.75e+03         1X         1X         1X
//                        Iterate n=8           2.32e+03 2.34e+03 2.36e+03     0.863X     0.862X      0.86X
//
//int n=9:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           2.59e+03 2.65e+03  2.7e+03         1X         1X         1X
//                        Iterate n=9           1.69e+03 1.71e+03 1.73e+03     0.652X     0.647X     0.642X
//
//int n=10:                  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           2.76e+03 2.82e+03 2.85e+03         1X         1X         1X
//                       Iterate n=10           1.54e+03 1.56e+03 1.58e+03     0.557X     0.554X     0.555X
//
//int n=400:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           1.45e+03 1.55e+03 1.57e+03         1X         1X         1X
//                      Iterate n=400               48.4     48.6     49.3    0.0334X    0.0314X    0.0314X
//
//bigInt n=1:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           4.08e+03 4.16e+03 4.23e+03         1X         1X         1X
//                        Iterate n=1           9.06e+03 9.14e+03 9.24e+03      2.22X       2.2X      2.19X
//
//bigInt n=2:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           4.02e+03 4.15e+03 4.24e+03         1X         1X         1X
//                        Iterate n=2           6.44e+03 6.51e+03 6.58e+03       1.6X      1.57X      1.55X
//
//bigInt n=3:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           3.86e+03 3.97e+03 4.03e+03         1X         1X         1X
//                        Iterate n=3           4.63e+03  4.7e+03 4.76e+03       1.2X      1.18X      1.18X
//
//bigInt n=4:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           3.54e+03 3.62e+03  3.7e+03         1X         1X         1X
//                        Iterate n=4           4.04e+03 4.11e+03 4.13e+03      1.14X      1.13X      1.12X
//
//bigInt n=5:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           2.73e+03 2.76e+03 2.79e+03         1X         1X         1X
//                        Iterate n=5           3.27e+03  3.3e+03 3.34e+03       1.2X      1.19X       1.2X
//
//bigInt n=6:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           2.98e+03 3.01e+03 3.04e+03         1X         1X         1X
//                        Iterate n=6           2.96e+03 2.98e+03 3.02e+03     0.992X     0.991X     0.992X
//
//bigInt n=7:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           2.63e+03 2.67e+03  2.7e+03         1X         1X         1X
//                        Iterate n=7           2.52e+03 2.54e+03 2.57e+03     0.957X     0.951X     0.951X
//
//bigInt n=8:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           2.69e+03 2.77e+03 2.81e+03         1X         1X         1X
//                        Iterate n=8           2.33e+03 2.35e+03 2.38e+03     0.865X     0.849X     0.846X
//
//bigInt n=9:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           2.62e+03 2.67e+03  2.7e+03         1X         1X         1X
//                        Iterate n=9           1.84e+03 1.86e+03 1.88e+03     0.701X     0.695X     0.696X
//
//bigInt n=10:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           2.54e+03 2.58e+03 2.62e+03         1X         1X         1X
//                       Iterate n=10           1.64e+03 1.68e+03 1.69e+03     0.645X      0.65X     0.646X
//
//bigInt n=400:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           1.35e+03  1.4e+03 1.42e+03         1X         1X         1X
//                      Iterate n=400               47.7     48.6     48.8    0.0352X    0.0348X    0.0344X
//
//float n=1:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           3.36e+03 3.39e+03 3.42e+03         1X         1X         1X
//                        Iterate n=1           7.22e+03 7.27e+03 7.34e+03      2.15X      2.14X      2.14X
//
//float n=2:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           4.08e+03 4.14e+03 4.19e+03         1X         1X         1X
//                        Iterate n=2            4.7e+03 4.74e+03 4.77e+03      1.15X      1.14X      1.14X
//
//float n=3:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           2.92e+03 2.95e+03 2.98e+03         1X         1X         1X
//                        Iterate n=3           3.63e+03 3.66e+03  3.7e+03      1.24X      1.24X      1.24X
//
//float n=4:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           2.81e+03 2.84e+03 2.87e+03         1X         1X         1X
//                        Iterate n=4            2.9e+03 2.94e+03 2.97e+03      1.03X      1.03X      1.03X
//
//float n=5:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           2.97e+03 3.03e+03 3.07e+03         1X         1X         1X
//                        Iterate n=5           2.35e+03 2.37e+03  2.4e+03     0.792X     0.782X     0.782X
//
//float n=6:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           2.77e+03 2.85e+03 2.88e+03         1X         1X         1X
//                        Iterate n=6           2.01e+03 2.03e+03 2.04e+03     0.724X      0.71X     0.707X
//
//float n=7:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           2.45e+03 2.52e+03 2.61e+03         1X         1X         1X
//                        Iterate n=7           1.67e+03 1.75e+03 1.77e+03     0.683X     0.696X     0.679X
//
//float n=8:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           2.45e+03 2.53e+03 2.56e+03         1X         1X         1X
//                        Iterate n=8           1.45e+03 1.54e+03 1.56e+03      0.59X     0.607X     0.607X
//
//float n=9:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           2.45e+03 2.53e+03 2.56e+03         1X         1X         1X
//                        Iterate n=9           1.32e+03 1.38e+03  1.4e+03     0.538X     0.547X     0.546X
//
//float n=10:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           2.29e+03 2.32e+03 2.35e+03         1X         1X         1X
//                       Iterate n=10           1.21e+03 1.24e+03 1.25e+03     0.529X     0.535X     0.533X
//
//float n=400:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           1.01e+03 1.04e+03 1.08e+03         1X         1X         1X
//                      Iterate n=400               32.4     32.6       33    0.0321X    0.0313X    0.0305X
//
//double n=1:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           2.49e+03 2.52e+03 2.54e+03         1X         1X         1X
//                        Iterate n=1           7.15e+03 7.26e+03 7.31e+03      2.87X      2.88X      2.88X
//
//double n=2:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           2.05e+03 2.08e+03 2.09e+03         1X         1X         1X
//                        Iterate n=2           4.82e+03 4.97e+03 5.03e+03      2.36X      2.39X       2.4X
//
//double n=3:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3            2.1e+03 2.14e+03 2.16e+03         1X         1X         1X
//                        Iterate n=3           3.62e+03 3.67e+03  3.7e+03      1.72X      1.71X      1.71X
//
//double n=4:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           2.27e+03 2.29e+03 2.31e+03         1X         1X         1X
//                        Iterate n=4           2.91e+03 2.94e+03 2.97e+03      1.29X      1.28X      1.29X
//
//double n=5:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           2.17e+03  2.2e+03 2.22e+03         1X         1X         1X
//                        Iterate n=5           2.34e+03 2.37e+03  2.4e+03      1.08X      1.08X      1.08X
//
//double n=6:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           1.85e+03 2.04e+03 2.09e+03         1X         1X         1X
//                        Iterate n=6           1.94e+03 2.02e+03 2.04e+03      1.05X     0.988X     0.976X
//
//double n=7:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           2.04e+03 2.06e+03 2.08e+03         1X         1X         1X
//                        Iterate n=7           1.74e+03 1.75e+03 1.77e+03     0.852X      0.85X     0.851X
//
//double n=8:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8            2.2e+03 2.23e+03 2.25e+03         1X         1X         1X
//                        Iterate n=8           1.53e+03 1.55e+03 1.56e+03     0.694X     0.697X     0.695X
//
//double n=9:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           2.16e+03 2.23e+03 2.25e+03         1X         1X         1X
//                        Iterate n=9           1.34e+03 1.38e+03  1.4e+03     0.621X     0.621X     0.619X
//
//double n=10:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           2.12e+03 2.18e+03  2.2e+03         1X         1X         1X
//                       Iterate n=10           1.21e+03 1.24e+03 1.25e+03     0.573X     0.571X     0.569X
//
//double n=400:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           1.97e+03 2.01e+03 2.03e+03         1X         1X         1X
//                      Iterate n=400               31.8     32.5       33    0.0161X    0.0162X    0.0162X
//
//string n=1:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           1.69e+03 1.71e+03 1.73e+03         1X         1X         1X
//                        Iterate n=1           2.25e+03 2.26e+03 2.28e+03      1.33X      1.32X      1.32X
//
//string n=2:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2            1.5e+03 1.52e+03 1.53e+03         1X         1X         1X
//                        Iterate n=2           1.19e+03 1.21e+03 1.22e+03     0.795X     0.796X     0.796X
//
//string n=3:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           1.42e+03 1.44e+03 1.46e+03         1X         1X         1X
//                        Iterate n=3                771      797      805     0.544X     0.552X     0.553X
//
//string n=4:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4            1.5e+03 1.57e+03 1.58e+03         1X         1X         1X
//                        Iterate n=4                695      706      712     0.464X      0.45X      0.45X
//
//string n=5:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           1.43e+03 1.49e+03  1.5e+03         1X         1X         1X
//                        Iterate n=5                553      563      568     0.386X     0.378X     0.378X
//
//string n=6:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           1.28e+03 1.31e+03 1.34e+03         1X         1X         1X
//                        Iterate n=6                435      441      445     0.341X     0.337X     0.333X
//
//string n=7:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           1.33e+03  1.4e+03 1.42e+03         1X         1X         1X
//                        Iterate n=7                414      420      424     0.311X     0.299X     0.299X
//
//string n=8:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           1.52e+03 1.57e+03 1.59e+03         1X         1X         1X
//                        Iterate n=8                366      371      374     0.241X     0.236X     0.235X
//
//string n=9:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           1.49e+03 1.53e+03 1.55e+03         1X         1X         1X
//                        Iterate n=9                302      304      307     0.202X     0.199X     0.199X
//
//string n=10:               Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10            1.4e+03 1.42e+03 1.44e+03         1X         1X         1X
//                       Iterate n=10                317      320      323     0.226X     0.225X     0.224X
//
//string n=400:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           1.36e+03 1.39e+03 1.42e+03         1X         1X         1X
//                      Iterate n=400               5.38     5.38     5.39   0.00396X   0.00388X    0.0038X
//
//timestamp n=1:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           1.79e+03 1.87e+03 1.89e+03         1X         1X         1X
//                        Iterate n=1           4.33e+03 4.55e+03 4.71e+03      2.41X      2.44X      2.49X
//
//timestamp n=2:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           1.69e+03 1.73e+03 1.76e+03         1X         1X         1X
//                        Iterate n=2           3.42e+03 3.54e+03 3.61e+03      2.02X      2.04X      2.05X
//
//timestamp n=3:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           1.54e+03 1.58e+03  1.6e+03         1X         1X         1X
//                        Iterate n=3           2.49e+03 2.54e+03 2.57e+03      1.61X       1.6X       1.6X
//
//timestamp n=4:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           1.53e+03  1.6e+03 1.63e+03         1X         1X         1X
//                        Iterate n=4            1.9e+03 1.95e+03 1.96e+03      1.24X      1.21X      1.21X
//
//timestamp n=5:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           1.61e+03 1.68e+03  1.7e+03         1X         1X         1X
//                        Iterate n=5           1.57e+03 1.59e+03  1.6e+03     0.976X     0.943X     0.939X
//
//timestamp n=6:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           1.47e+03 1.49e+03 1.51e+03         1X         1X         1X
//                        Iterate n=6           1.32e+03 1.34e+03 1.35e+03     0.893X     0.895X     0.893X
//
//timestamp n=7:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           1.44e+03 1.51e+03 1.54e+03         1X         1X         1X
//                        Iterate n=7           1.12e+03 1.15e+03 1.16e+03     0.776X     0.758X     0.755X
//
//timestamp n=8:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           1.65e+03 1.67e+03 1.69e+03         1X         1X         1X
//                        Iterate n=8                992 1.01e+03 1.02e+03     0.602X     0.604X     0.606X
//
//timestamp n=9:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           1.56e+03 1.57e+03 1.59e+03         1X         1X         1X
//                        Iterate n=9                894      902      912     0.575X     0.573X     0.575X
//
//timestamp n=10:            Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           1.55e+03 1.59e+03 1.61e+03         1X         1X         1X
//                       Iterate n=10                789      814      824      0.51X     0.511X      0.51X
//
//timestamp n=400:           Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400            1.4e+03 1.48e+03  1.5e+03         1X         1X         1X
//                      Iterate n=400               20.5     20.9     20.9    0.0146X    0.0141X    0.0139X
//
//decimal(4,0) n=1:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=1           1.82e+03 1.89e+03 1.91e+03         1X         1X         1X
//                        Iterate n=1           6.26e+03 6.43e+03  6.5e+03      3.44X       3.4X       3.4X
//
//decimal(4,0) n=2:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=2           1.69e+03 1.76e+03 1.77e+03         1X         1X         1X
//                        Iterate n=2           3.99e+03 4.07e+03  4.1e+03      2.36X      2.32X      2.31X
//
//decimal(4,0) n=3:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=3           1.63e+03 1.66e+03 1.68e+03         1X         1X         1X
//                        Iterate n=3           3.19e+03 3.22e+03 3.26e+03      1.95X      1.94X      1.94X
//
//decimal(4,0) n=4:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=4           1.76e+03 1.81e+03 1.84e+03         1X         1X         1X
//                        Iterate n=4           2.46e+03 2.52e+03 2.56e+03       1.4X      1.39X      1.39X
//
//decimal(4,0) n=5:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=5           1.64e+03 1.69e+03  1.7e+03         1X         1X         1X
//                        Iterate n=5           1.93e+03 1.96e+03 1.98e+03      1.17X      1.16X      1.16X
//
//decimal(4,0) n=6:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=6           1.61e+03 1.65e+03 1.67e+03         1X         1X         1X
//                        Iterate n=6           1.71e+03 1.74e+03 1.76e+03      1.06X      1.06X      1.06X
//
//decimal(4,0) n=7:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=7           1.53e+03 1.56e+03 1.57e+03         1X         1X         1X
//                        Iterate n=7           1.53e+03 1.57e+03 1.59e+03         1X         1X      1.01X
//
//decimal(4,0) n=8:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=8           1.69e+03 1.73e+03 1.76e+03         1X         1X         1X
//                        Iterate n=8            1.4e+03 1.43e+03 1.46e+03     0.829X     0.828X      0.83X
//
//decimal(4,0) n=9:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                      SetLookup n=9           1.69e+03 1.72e+03 1.74e+03         1X         1X         1X
//                        Iterate n=9           1.28e+03 1.32e+03 1.34e+03     0.756X     0.766X     0.769X
//
//decimal(4,0) n=10:         Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                     SetLookup n=10           1.65e+03 1.69e+03 1.71e+03         1X         1X         1X
//                       Iterate n=10           1.16e+03 1.22e+03 1.25e+03     0.707X     0.723X     0.729X
//
//decimal(4,0) n=400:        Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                         (relative) (relative) (relative)
//---------------------------------------------------------------------------------------------------------
//                    SetLookup n=400           1.52e+03 1.58e+03  1.6e+03         1X         1X         1X
//                      Iterate n=400               30.6     31.3     31.3    0.0202X    0.0197X    0.0195X

#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/in-predicate.h"

#include "runtime/decimal-value.h"
#include "runtime/string-value.h"
#include "udf/udf-test-harness.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;
using namespace strings;
using std::move;

namespace impala {

template<typename T> T MakeAnyVal(int v) {
  return T(v);
}

template<> StringVal MakeAnyVal(int v) {
  // Leak these strings so we don't have to worry about them going out of scope
  string* s = new string();
  *s = lexical_cast<string>(v);
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(s->c_str())), s->size());
}

class InPredicateBenchmark {
 public:
  template<typename T, typename SetType>
  struct TestData {
    vector<T> anyvals;
    vector<AnyVal*> anyval_ptrs;
    InPredicate::SetLookupState<SetType> state;

    vector<T> search_vals;

    int total_found_set;
    int total_set;
    int total_found_iter;
    int total_iter;
  };

  template<typename T, typename SetType>
  static TestData<T, SetType> CreateTestData(int num_values,
      const FunctionContext::TypeDesc& type, int num_search_vals = 100) {
    srand(time(NULL));
    TestData<T, SetType> data;
    data.anyvals.resize(num_values);
    data.anyval_ptrs.resize(num_values);
    for (int i = 0; i < num_values; ++i) {
      data.anyvals[i] = MakeAnyVal<T>(rand());
      data.anyval_ptrs[i] = &data.anyvals[i];
    }

    for (int i = 0; i < num_search_vals; ++i) {
      data.search_vals.push_back(MakeAnyVal<T>(rand()));
    }

    FunctionContext* ctx = CreateContext(num_values, type);

    vector<AnyVal*> constant_args;
    constant_args.push_back(NULL);
    for (AnyVal* p : data.anyval_ptrs) constant_args.push_back(p);
    UdfTestHarness::SetConstantArgs(ctx, move(constant_args));

    InPredicate::SetLookupPrepare<T, SetType>(ctx, FunctionContext::FRAGMENT_LOCAL);
    data.state = *reinterpret_cast<InPredicate::SetLookupState<SetType>*>(
        ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));

    data.total_found_set = data.total_set = data.total_found_iter = data.total_iter = 0;
    return data;
  }

  template<typename T, typename SetType>
  static void TestSetLookup(int batch_size, void* d) {
    TestData<T, SetType>* data = reinterpret_cast<TestData<T, SetType>*>(d);
    for (int i = 0; i < batch_size; ++i) {
      for (const T& search_val: data->search_vals) {
        BooleanVal found = InPredicate::SetLookup(&data->state, search_val);
        if (found.val) ++data->total_found_set;
        ++data->total_set;
      }
    }
  }

  template<typename T, typename SetType>
  static void TestIterate(int batch_size, void* d) {
    TestData<T, SetType>* data = reinterpret_cast<TestData<T, SetType>*>(d);
    for (int i = 0; i < batch_size; ++i) {
      for (const T& search_val: data->search_vals) {
        BooleanVal found = InPredicate::Iterate(
            data->state.type, search_val, data->anyvals.size(), &data->anyvals[0]);
        if (found.val) ++data->total_found_iter;
        ++data->total_iter;
      }
    }
  }

  template <typename AnyValType, typename SetType, FunctionContext::Type TypeDesc>
  static void RunBenchmark(int n) {
    Benchmark suite(Substitute("$0 n=$1", GetTypeName(TypeDesc), n));
    FunctionContext::TypeDesc type;
    type.type = TypeDesc;
    InPredicateBenchmark::TestData<AnyValType, SetType> data =
        InPredicateBenchmark::CreateTestData<AnyValType, SetType>(n, type);
    suite.AddBenchmark(Substitute("SetLookup n=$0", n),
        InPredicateBenchmark::TestSetLookup<AnyValType, SetType>, &data);
    suite.AddBenchmark(Substitute("Iterate n=$0", n),
        InPredicateBenchmark::TestIterate<AnyValType, SetType>, &data);
    cout << suite.Measure() << endl;
  }

 private:
  static FunctionContext* CreateContext(
      int num_args, const FunctionContext::TypeDesc& type) {
    // Types don't matter (but number of args do)
    FunctionContext::TypeDesc ret_type;
    vector<FunctionContext::TypeDesc> arg_types(num_args + 1, type);
    return UdfTestHarness::CreateTestContext(ret_type, arg_types);
  }

  static string GetTypeName(FunctionContext::Type type){
    switch (type) {
      case FunctionContext::TYPE_TINYINT:
        return "tinyInt";
      case FunctionContext::TYPE_SMALLINT:
        return "smallInt";
      case FunctionContext::TYPE_INT:
        return "int";
      case FunctionContext::TYPE_BIGINT:
        return "bigInt";
      case FunctionContext::TYPE_FLOAT:
        return "float";
      case FunctionContext::TYPE_DOUBLE:
        return "double";
      case FunctionContext::TYPE_STRING:
        return "string";
      case FunctionContext::TYPE_TIMESTAMP:
        return "timestamp";
      default:
        return "Unsupported Type";
    }
  }

};

template <>
void InPredicateBenchmark::RunBenchmark<DecimalVal, Decimal16Value,
    FunctionContext::TYPE_DECIMAL>(int n) {
  Benchmark suite(Substitute("decimal(4,0) n=$0", n));
  FunctionContext::TypeDesc type;
  type.type = FunctionContext::TYPE_DECIMAL;
  type.precision = 4;
  type.scale = 0;
  InPredicateBenchmark::TestData<DecimalVal, Decimal16Value> data =
      InPredicateBenchmark::CreateTestData<DecimalVal, Decimal16Value>(n, type);
  suite.AddBenchmark(Substitute("SetLookup n=$0", n),
      InPredicateBenchmark::TestSetLookup<DecimalVal, Decimal16Value>, &data);
  suite.AddBenchmark(Substitute("Iterate n=$0", n),
      InPredicateBenchmark::TestIterate<DecimalVal, Decimal16Value>, &data);
  cout << suite.Measure() << endl;
}
}

#define RUN_BENCHMARK(AnyValType, SetType, type_desc) \
  for (int i = 1; i <= 10; ++i) { \
    InPredicateBenchmark::RunBenchmark<AnyValType, SetType, type_desc>(i); \
  } \
  InPredicateBenchmark::RunBenchmark<AnyValType, SetType, type_desc>(400);

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  RUN_BENCHMARK(TinyIntVal, int8_t, FunctionContext::TYPE_TINYINT)
  RUN_BENCHMARK(SmallIntVal, int16_t, FunctionContext::TYPE_SMALLINT)
  RUN_BENCHMARK(IntVal, int32_t, FunctionContext::TYPE_INT)
  RUN_BENCHMARK(BigIntVal, int64_t, FunctionContext::TYPE_BIGINT)
  RUN_BENCHMARK(FloatVal, float, FunctionContext::TYPE_FLOAT)
  RUN_BENCHMARK(DoubleVal, double, FunctionContext::TYPE_DOUBLE)
  RUN_BENCHMARK(StringVal, StringValue, FunctionContext::TYPE_STRING)
  RUN_BENCHMARK(TimestampVal, TimestampValue, FunctionContext::TYPE_TIMESTAMP)
  RUN_BENCHMARK(DecimalVal, Decimal16Value, FunctionContext::TYPE_DECIMAL)

  return 0;
}
