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
package org.apache.drill.madlib.linregr.result;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Entity for heteroskedasticity test result.
 *
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class HeteroskedasticityTestResult {
  private double bp_stats   ;
  private double bp_p_value ;

  public HeteroskedasticityTestResult() {
  }

  public HeteroskedasticityTestResult(double bp_stats, double bp_p_value) {
    this.bp_stats = bp_stats;
    this.bp_p_value = bp_p_value;
  }

  @JsonProperty(value = "bp_stats")
  public double getBp_stats() {
    return bp_stats;
  }

  public void setBp_stats(double bp_stats) {
    this.bp_stats = bp_stats;
  }

  public void set_bp_stats(double bp_stats) {
    this.bp_stats = bp_stats;
  }

  @JsonProperty(value = "bp_p_value")
  public double getBp_p_value() {
    return bp_p_value;
  }

  public void setBp_p_value(double bp_p_value) {
    this.bp_p_value = bp_p_value;
  }

  public void set_bp_p_value(double bp_p_value) {
    this.bp_p_value = bp_p_value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (o == null || getClass() != o.getClass())
      return false;

    HeteroskedasticityTestResult that = (HeteroskedasticityTestResult) o;

    return new EqualsBuilder()
      .append(bp_stats, that.bp_stats)
      .append(bp_p_value, that.bp_p_value)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(bp_stats)
      .append(bp_p_value)
      .toHashCode();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("HeteroskedasticityTestResult{");
    sb.append("bp_stats=").append(bp_stats);
    sb.append(", bp_p_value=").append(bp_p_value);
    sb.append('}');
    return sb.toString();
  }
}
