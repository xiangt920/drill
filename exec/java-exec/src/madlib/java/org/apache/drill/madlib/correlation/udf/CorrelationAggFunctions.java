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
package org.apache.drill.madlib.correlation.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.Utils;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.madlib.common.DataHolderUtils;
import org.apache.drill.madlib.correlation.CorrelationCPPFunctions;

import javax.inject.Inject;

/**
 * UDAs for correlation aggregation.
 */

@SuppressWarnings({"unused","deprecation"})
public class CorrelationAggFunctions {

  public static void output(Object result, DrillBuf buf, VarCharHolder out) {
    String str = Utils.parse_to_json(result);
    DataHolderUtils.resetVarcharOut(buf, out, str);
  }

  @FunctionTemplate(name = "correlation_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
    costCategory = FunctionTemplate.FunctionCostCategory.COMPLEX)
  public static class CorrelationAgg implements DrillAggFunc {
    @Param
    RepeatedFloat8Holder xHolder;
    @Param
    RepeatedFloat8Holder meanHolder;

    @Workspace
    ObjectHolder stateHolder;
    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      stateHolder = new ObjectHolder();
      stateHolder.obj = new double[0][0];
    }

    @Override
    public void add() {
      double[] x = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(xHolder);
      double[] mean = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(meanHolder);
      org.apache.drill.madlib.correlation.CorrelationCPPFunctions.corr_trans(stateHolder, x, mean);
    }

    @Override
    public void output() {
      org.apache.drill.madlib.correlation.CorrelationCPPFunctions.corr_final(stateHolder);

      org.apache.drill.madlib.correlation.udf.CorrelationAggFunctions.output(stateHolder.obj, buf, out);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      stateHolder.obj = new double[0][0];
    }
  }

  /**
   * Return the last transition or merge state as the final state.
   * This aggregate does not divide by the number of samples
   * (hence it's sum of {@code (x-mean)^2} instead of expectation)
   */
  @FunctionTemplate(name = "covariance_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class CovarianceAgg implements DrillAggFunc {
    @Param
    RepeatedFloat8Holder xHolder;
    @Param
    RepeatedFloat8Holder meanHolder;

    @Workspace
    ObjectHolder stateHolder;
    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      stateHolder = new ObjectHolder();
      stateHolder.obj = new double[0][0];
    }

    @Override
    public void add() {
      double[] x = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(xHolder);
      double[] mean = org.apache.drill.madlib.common.DataHolderUtils.extract1DoubleArray(meanHolder);
      org.apache.drill.madlib.correlation.CorrelationCPPFunctions.cov_trans(stateHolder, x, mean);
    }

    @Override
    public void output() {
      org.apache.drill.madlib.correlation.udf.CorrelationAggFunctions.output(stateHolder.obj, buf, out);
      buf = out.buffer;
    }

    @Override
    public void reset() {
      stateHolder.obj = new double[0][0];
    }
  }
}
