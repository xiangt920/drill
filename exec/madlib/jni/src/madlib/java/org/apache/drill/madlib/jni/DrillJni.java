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
package org.apache.drill.madlib.jni;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

/**
 * Native functions for calling madlib functions from Drill UDFs.
 *
 */

public class DrillJni {

  public static final DrillJni JNI = new DrillJni();

  private DrillJni() {

  }

  // -------------------------------------------------------------------
  // array operations start
  // -------------------------------------------------------------------

  public native double array_dot(double[][] v1, double[][] v2);
  public native double[] array_scalar_mult(double[] v1, double v2);

  // -------------------------------------------------------------------
  // array operations end
  // -------------------------------------------------------------------

  // -------------------------------------------------------------------
  // linear regression start
  // -------------------------------------------------------------------

  public native byte[] linregr_transition(byte[] state, double y, double[] x);

  public native Object linrger_final(byte[] state);

  public native byte[] hetero_linregr_transition(byte[] state, double y, double[] x, double[] coef);

  public native Object hetero_linregr_final(byte[] state);

  public native byte[] linregr_merge_states(byte[] state1, byte[] state2);

  public native byte[] hetero_linregr_merge_states(byte[] state1, byte[] state2);

  // -------------------------------------------------------------------
  // linear regression end
  // -------------------------------------------------------------------

  // -------------------------------------------------------------------
  // logistic regression start
  // -------------------------------------------------------------------

  public native double[] logregr_cg_step_transition(double[] state, boolean y, double[] x, double[] previous_state);
  public native double[] logregr_cg_step_merge_states(double[] state1, double[] state2);
  public native double[] logregr_cg_step_final(double[] state);
  public native double   logregr_cg_step_distance(double[] state1, double[] state2);
  public native Object   logregr_cg_result(double[] state);

  public native double[] logregr_irls_step_transition(double[] state, boolean y, double[] x, double[] previous_state);
  public native double[] logregr_irls_step_merge_states(double[] state1, double[] state2);
  public native double[] logregr_irls_step_final(double[] state);
  public native double   logregr_irls_step_distance(double[] state1, double[] state2);
  public native Object   logregr_irls_result(double[] state);

  public native double[] logregr_igd_step_transition(double[] state, boolean y, double[] x, double[] previous_state);
  public native double[] logregr_igd_step_merge_states(double[] state1, double[] state2);
  public native double[] logregr_igd_step_final(double[] state);
  public native double   logregr_igd_step_distance(double[] state1, double[] state2);
  public native Object   logregr_igd_result(double[] state);


  public native boolean  logregr_predict(double[] coef, double[] col_ind_var);
  public native double   logregr_predict_prob(double[] coef, double[] col_ind_var);

  // -------------------------------------------------------------------
  // logistic regression end
  // -------------------------------------------------------------------

  // -------------------------------------------------------------------
  // correlation start
  // -------------------------------------------------------------------

  public native double[][] correlation_transition(double[][] state, double[] x, double[] mean);
  public native double[][] correlation_merge(double[][] left_state, double[][] right_state);
  public native double[][] correlation_final(double[][] state);

  // -------------------------------------------------------------------
  // correlation end
  // -------------------------------------------------------------------

  static {
    String path = "/libmadlib.so";
    try (InputStream is = DrillJni.class.getResourceAsStream(path)) {
      if (is == null) {
        throw new IOException(String.format(
          "Failure trying to libmadlib.so, tried to read on classpath location %s",
          path));
      }
      File tmpLib = File.createTempFile("JNI-libmadlib", ".so");
      FileOutputStream out = new FileOutputStream(tmpLib);
      byte[] buf = new byte[1024];
      int len;
      while ((len = is.read(buf)) > 0)
        out.write(buf, 0, len);
      out.close();
      System.load(tmpLib.getAbsolutePath());
      Files.delete(tmpLib.toPath());
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

}
