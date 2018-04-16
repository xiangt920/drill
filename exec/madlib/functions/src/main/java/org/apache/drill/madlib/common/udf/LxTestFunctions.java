/*
 * Copyright 1999-2012 Alibaba Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.madlib.common.udf;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;

/**
 * @author xiang
 * @date 2018-03-19 上午10:31
 */

public class LxTestFunctions {

  @FunctionTemplate(name = "lx_test", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LxTest implements DrillSimpleFunc {

    @Param
    Float8Holder p;

    @Output Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = p.value;
    }
  }

  @FunctionTemplate(name = "lx_test", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LxTestN implements DrillSimpleFunc {

    @Param
    NullableFloat8Holder p;

    @Output Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      if (p.isSet == 1) {
        out.value = p.value;
      } else {
        out.value = Double.NaN;
      }
    }
  }

  @FunctionTemplate(name = "lx_test", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LxTest2 implements DrillSimpleFunc {

    @Param
    Float8Holder p;
    @Param
    Float8Holder p2;

    @Output Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = p.value+p2.value;
    }
  }

  @FunctionTemplate(name = "lx_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class LxTest4Array implements DrillAggFunc {

    @Param
    RepeatedBigIntHolder p;

    @Workspace
    ObjectHolder stateHolder;
//    @Inject
//    DrillBuf buf;

    @Output
    RepeatedFloat8Holder out;

    @Override
    public void setup() {
      stateHolder = new ObjectHolder();
      stateHolder.obj = new double[] {1,2,3};
    }

    @Override
    public void add() {

    }

    @Override
    public void output() {
      double[] state = (double[]) stateHolder.obj;
      for (int i = 0; i < state.length; i++) {
        out.vector.getMutator().setSafe(i, state[i]);
      }
      out.end = state.length - 1;
    }

    @Override
    public void reset() {

    }

  }
}
