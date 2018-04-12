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
 * Entity for Linregr result.
 *
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LinregrResult {
  private double[] coef;

  private double r2;

  private double[] std_err;

  private double[] t_stats;

  private double[] p_values;

  private double condition_no;

  private long num_processed;

  private double[] vcov;

  public LinregrResult() {
  }

  public LinregrResult(
    double[] coef, double r2, double[] std_err,
    double[] t_stats, double[] p_values,
    double condition_no, long num_processed,
    double[] vcov) {

    this.coef = coef;
    this.r2 = r2;
    this.std_err = std_err;
    this.t_stats = t_stats;
    this.p_values = p_values;
    this.condition_no = condition_no;
    this.num_processed = num_processed;
    this.vcov = vcov;
  }

  @JsonProperty(value = "coef")
  public double[] getCoef() {
    return coef;
  }

  public void setCoef(double[] coef) {
    this.coef = coef;
  }

  public void set_coef(double[] coef) {
    this.coef = coef;
  }

  @JsonProperty(value = "r2")
  public double getR2() {
    return r2;
  }

  public void setR2(double r2) {
    this.r2 = r2;
  }

  public void set_r2(double r2) {
    this.r2 = r2;
  }

  @JsonProperty(value = "std_err")
  public double[] getStd_err() {
    return std_err;
  }

  public void setStd_err(double[] std_err) {
    this.std_err = std_err;
  }

  public void set_std_err(double[] std_err) {
    this.std_err = std_err;
  }

  @JsonProperty(value = "t_stats")
  public double[] getT_stats() {
    return t_stats;
  }

  public void setT_stats(double[] t_stats) {
    this.t_stats = t_stats;
  }

  public void set_t_stats(double[] t_stats) {
    this.t_stats = t_stats;
  }

  @JsonProperty(value = "p_values")
  public double[] getP_values() {
    return p_values;
  }

  public void setP_values(double[] p_values) {
    this.p_values = p_values;
  }

  public void set_p_values(double[] p_values) {
    this.p_values = p_values;
  }

  @JsonProperty(value = "condition_no")
  public double getCondition_no() {
    return condition_no;
  }

  public void setCondition_no(double condition_no) {
    this.condition_no = condition_no;
  }

  public void set_condition_no(double condition_no) {
    this.condition_no = condition_no;
  }

  @JsonProperty(value = "num_processed")
  public long getNum_processed() {
    return num_processed;
  }

  public void setNum_processed(long num_processed) {
    this.num_processed = num_processed;
  }

  public void set_num_processed(long num_processed) {
    this.num_processed = num_processed;
  }

  @JsonProperty(value = "vcov")
  public double[] getVcov() {
    return vcov;
  }

  public void setVcov(double[] vcov) {
    this.vcov = vcov;
  }

  public void set_vcov(double[] vcov) {
    this.vcov = vcov;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (o == null || getClass() != o.getClass())
      return false;

    LinregrResult that = (LinregrResult) o;

    return new EqualsBuilder()
      .append(coef, that.coef)
      .append(r2, that.r2)
      .append(std_err, that.std_err)
      .append(t_stats, that.t_stats)
      .append(p_values, that.p_values)
      .append(condition_no, that.condition_no)
      .append(num_processed, that.num_processed)
      .append(vcov, that.vcov)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(coef)
      .append(r2)
      .append(std_err)
      .append(t_stats)
      .append(p_values)
      .append(condition_no)
      .append(num_processed)
      .append(vcov)
      .toHashCode();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("LinregrResult{");
    sb.append("coef=").append(coef);
    sb.append(", r2=").append(r2);
    sb.append(", std_err=").append(std_err);
    sb.append(", t_stats=").append(t_stats);
    sb.append(", p_values=").append(p_values);
    sb.append(", condition_no=").append(condition_no);
    sb.append(", num_processed=").append(num_processed);
    sb.append(", vcov=").append(vcov);
    sb.append('}');
    return sb.toString();
  }
}
