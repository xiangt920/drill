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
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 * Drill UDFs for linear regression
 *
 */

@SuppressWarnings({"deprecation", "unused"})
public class LinregrFunctions {

  public static final byte[] DEFAULT_STATE = new byte[204];

  static {
    DEFAULT_STATE[0] = 16;
  }
  
  public static byte[] add(double dependent, double[] independents, byte[] stateBytes) {
    return LinregrTransitionFunctions.transition(stateBytes, dependent, independents);
  }

  @FunctionTemplate(name = "linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrFunctionDD implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Inject
    DrillBuf buf;

    @Workspace
    ObjectHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new ObjectHolder();
      state.obj = org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      state.obj = org.apache.drill.madlib.linregr.udf.LinregrFunctions.add(dependent, independents, (byte[])state.obj);
    }

    @Override
    public void output() {
      byte[] stateBytes = (byte[]) state.obj;
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrFinalFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      state.obj = org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE;
    }
  }

  @FunctionTemplate(name = "linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrFunctionNDD implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Inject
    DrillBuf buf;

    @Workspace
    ObjectHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new ObjectHolder();
      state.obj = org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE;
    }

    @Override
    public void add() {
      double dependent = 0;
      if (dependentHolder.isSet == 1) {
        dependent = dependentHolder.value;
      }
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      state.obj = org.apache.drill.madlib.linregr.udf.LinregrFunctions.add(dependent, independents, (byte[]) state.obj);
    }

    @Override
    public void output() {
      byte[] stateBytes = (byte[]) state.obj;
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrFinalFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      state.obj = org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE;
    }
  }

}
