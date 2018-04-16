/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.madlib.correlation;

import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.madlib.jni.DrillJni;

/**
 * Functions for calling jni correlation functions
 */

@SuppressWarnings("deprecation")
public class CorrelationCPPFunctions {

  public static void corr_trans(ObjectHolder state, double[] x, double[] mean) {
    state.obj = DrillJni.JNI.correlation_transition((double[][]) state.obj, x, mean);
  }

  public static void corr_final(ObjectHolder state) {
    state.obj = DrillJni.JNI.correlation_final((double[][]) state.obj);
  }

  public static void cov_trans(ObjectHolder state, double[] x, double[] mean) {
    state.obj = DrillJni.JNI.correlation_transition((double[][]) state.obj, x, mean);
  }
}
