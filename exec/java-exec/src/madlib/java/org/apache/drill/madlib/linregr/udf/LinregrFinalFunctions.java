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
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.madlib.jni.DrillJni;

import javax.inject.Inject;

import static org.apache.drill.common.Utils.parse_to_json;


/**
 * Drill UDFs for linear regression final
 *
 */

public class LinregrFinalFunctions {

  public static String finalFunc(byte[] state) {
    Object result = DrillJni.JNI.linrger_final(state);
    return parse_to_json(result);
  }

  @FunctionTemplate(name = "linregr_final", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LinregrFinal implements DrillSimpleFunc {

    @Param
    VarCharHolder in_state;

    @Inject
    DrillBuf buf;
    @Output
    VarCharHolder out;

    @Override
    public void setup() {
      byte[] state = org.apache.drill.madlib.common.DataHolderUtils.extractBytes(in_state);
      String outStr = org.apache.drill.madlib.linregr.udf.LinregrFinalFunctions.finalFunc(state);
      org.apache.drill.madlib.common.DataHolderUtils.resetVarcharOut(buf, out, outStr);
      buf = out.buffer;
    }

    @Override
    public void eval() {

    }
  }
}
