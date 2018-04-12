/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.madlib.linregr.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.madlib.jni.DrillJni;

import javax.inject.Inject;

/**
 * Drill UDFs for linear regression transition
 *
 */

@SuppressWarnings("unused")
public class LinregrTransitionFunctions {

  public static byte[] transition(byte[] state, double y, double[] x) {
    return DrillJni.JNI.linregr_transition(state, y, x);

  }

  @FunctionTemplate(name = "linregr_transition", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LinregrTransition implements DrillSimpleFunc {
    @Param
    VarCharHolder in_state;
    @Param
    Float8Holder in_y;
    @Param
    RepeatedFloat8Holder in_x;

    @Inject
    DrillBuf buf;
    @Output
    VarCharHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      byte[] state = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(in_state);
      double y = in_y.value;
      double[] x = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(in_x);
      byte[] result = org.apache.drill.madlib.linregr.udf.LinregrTransitionFunctions.transition(state, y, x);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, result);
      buf = out.buffer;
    }
  }

}
