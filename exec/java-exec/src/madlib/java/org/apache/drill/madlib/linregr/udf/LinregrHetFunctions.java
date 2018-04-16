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
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableSmallIntHolder;
import org.apache.drill.exec.expr.holders.NullableTinyIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedSmallIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedTinyIntHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.madlib.common.DataHolderUtils;
import org.apache.drill.madlib.jni.DrillJni;

import javax.inject.Inject;

import static org.apache.drill.common.Utils.parse_to_json;


/**
 * Drill UDFs for linear regression heteroskedasticity test.
 *
 */

@SuppressWarnings({"unused", "WeakerAccess"})
public class LinregrHetFunctions {

  public static byte[] transition(byte[] state, double dependent, double[] independents, double[] coef) {
    return DrillJni.JNI.hetero_linregr_transition(state, dependent, independents, coef);
  }

  public static String finalFunc(byte[] state) {
    Object result = DrillJni.JNI.hetero_linregr_final(state);
    return parse_to_json(result);
  }
  
  public static void add(
    double dependent, double[] independents,
    RepeatedFloat8Holder coefHolder, 
    VarBinaryHolder state, 
    DrillBuf buf) {
    double[] coef = DataHolderUtils.extract1DoubleArray(coefHolder);
    byte[] stateBytes = DataHolderUtils.extractBytes(state);
    stateBytes = LinregrHetFunctions.transition(stateBytes, dependent, independents, coef);
    DataHolderUtils.setBinary(buf, state, stateBytes);
  }

  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestTITI implements DrillAggFunc {

    @Param
    TinyIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestTISI implements DrillAggFunc {

    @Param
    TinyIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestTII implements DrillAggFunc {

    @Param
    TinyIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestTIBI implements DrillAggFunc {

    @Param
    TinyIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestTIF implements DrillAggFunc {

    @Param
    TinyIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestTID implements DrillAggFunc {

    @Param
    TinyIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestSITI implements DrillAggFunc {

    @Param
    SmallIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestSISI implements DrillAggFunc {

    @Param
    SmallIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestSII implements DrillAggFunc {

    @Param
    SmallIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestSIBI implements DrillAggFunc {

    @Param
    SmallIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestSIF implements DrillAggFunc {

    @Param
    SmallIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestSID implements DrillAggFunc {

    @Param
    SmallIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestITI implements DrillAggFunc {

    @Param
    IntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestISI implements DrillAggFunc {

    @Param
    IntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestII implements DrillAggFunc {

    @Param
    IntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestIBI implements DrillAggFunc {

    @Param
    IntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestIF implements DrillAggFunc {

    @Param
    IntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestID implements DrillAggFunc {

    @Param
    IntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestBITI implements DrillAggFunc {

    @Param
    BigIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestBISI implements DrillAggFunc {

    @Param
    BigIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestBII implements DrillAggFunc {

    @Param
    BigIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestBIBI implements DrillAggFunc {

    @Param
    BigIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestBIF implements DrillAggFunc {

    @Param
    BigIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestBID implements DrillAggFunc {

    @Param
    BigIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestFTI implements DrillAggFunc {

    @Param
    Float4Holder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestFSI implements DrillAggFunc {

    @Param
    Float4Holder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestFI implements DrillAggFunc {

    @Param
    Float4Holder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestFBI implements DrillAggFunc {

    @Param
    Float4Holder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestFF implements DrillAggFunc {

    @Param
    Float4Holder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestFD implements DrillAggFunc {

    @Param
    Float4Holder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestDTI implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestDSI implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestDI implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestDBI implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestDF implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestDD implements DrillAggFunc {

    @Param
    Float8Holder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.value;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }

  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNTITI implements DrillAggFunc {

    @Param
    NullableTinyIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNTISI implements DrillAggFunc {

    @Param
    NullableTinyIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNTII implements DrillAggFunc {

    @Param
    NullableTinyIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNTIBI implements DrillAggFunc {

    @Param
    NullableTinyIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNTIF implements DrillAggFunc {

    @Param
    NullableTinyIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNTID implements DrillAggFunc {

    @Param
    NullableTinyIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNSITI implements DrillAggFunc {

    @Param
    NullableSmallIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNSISI implements DrillAggFunc {

    @Param
    NullableSmallIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNSII implements DrillAggFunc {

    @Param
    NullableSmallIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNSIBI implements DrillAggFunc {

    @Param
    NullableSmallIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNSIF implements DrillAggFunc {

    @Param
    NullableSmallIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNSID implements DrillAggFunc {

    @Param
    NullableSmallIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNITI implements DrillAggFunc {

    @Param
    NullableIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNISI implements DrillAggFunc {

    @Param
    NullableIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNII implements DrillAggFunc {

    @Param
    NullableIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNIBI implements DrillAggFunc {

    @Param
    NullableIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNIF implements DrillAggFunc {

    @Param
    NullableIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNID implements DrillAggFunc {

    @Param
    NullableIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNBITI implements DrillAggFunc {

    @Param
    NullableBigIntHolder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNBISI implements DrillAggFunc {

    @Param
    NullableBigIntHolder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNBII implements DrillAggFunc {

    @Param
    NullableBigIntHolder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNBIBI implements DrillAggFunc {

    @Param
    NullableBigIntHolder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNBIF implements DrillAggFunc {

    @Param
    NullableBigIntHolder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNBID implements DrillAggFunc {

    @Param
    NullableBigIntHolder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNFTI implements DrillAggFunc {

    @Param
    NullableFloat4Holder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNFSI implements DrillAggFunc {

    @Param
    NullableFloat4Holder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNFI implements DrillAggFunc {

    @Param
    NullableFloat4Holder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNFBI implements DrillAggFunc {

    @Param
    NullableFloat4Holder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNFF implements DrillAggFunc {

    @Param
    NullableFloat4Holder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNFD implements DrillAggFunc {

    @Param
    NullableFloat4Holder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNDTI implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedTinyIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNDSI implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedSmallIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNDI implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNDBI implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedBigIntHolder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNDF implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedFloat4Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }


  @FunctionTemplate(name = "heteroskedasticity_test_linregr", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LinregrHetTestNDD implements DrillAggFunc {

    @Param
    NullableFloat8Holder dependentHolder;
    @Param
    RepeatedFloat8Holder independentsHolder;
    @Param
    RepeatedFloat8Holder coefHolder;

    @Inject
    DrillBuf buf;
    @Workspace
    VarBinaryHolder state;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      state = new VarBinaryHolder();
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }

    @Override
    public void add() {
      double dependent = dependentHolder.isSet == 1 ?dependentHolder.value:0;
      double[] independents = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(independentsHolder);
      org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.add(dependent, independents, coefHolder, state, buf);
      buf = state.buffer;
    }

    @Override
    public void output() {
      byte[] stateBytes = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrHetFunctions.finalFunc(stateBytes);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      org.apache.drill.madlib.common.DataHolderUtils.setBinary(buf, state, org.apache.drill.madlib.linregr.udf.LinregrFunctions.DEFAULT_STATE);
      buf = state.buffer;
    }
  }
}
