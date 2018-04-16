//
// Created by Xiang on 2018/3/15.
//

#ifndef MADLIB_DRILL_LOGISTIC_HPP
#define MADLIB_DRILL_LOGISTIC_HPP

#include <common/array.h>

using namespace madlib::dbconnector::postgres;
using namespace madlib::modules::regress;
using namespace madlib::dbal::eigen_integration;

#define STEP_TRANSITION_CALL(U) step_transition<U>(env, j_state, j_y, j_x, j_previous_state, udf, &U::run)
#define STEP_MERGE_STATES_CALL(U) step_merge_states<U>(env, j_state1, j_state2, udf, &U::run)
#define STEP_FINAL_CALL(U) step_final<U>(env, j_state, udf, &U::run)
#define STEP_DISTANCE_CALL(U) step_distance<U>(env, j_state1, j_state2, udf, &U::run)
#define STEP_RESULT_CALL(U) step_result<U>(env, j_state, udf, &U::run)

template <class T>
jdoubleArray step_transition
  (JNIEnv * env, JNI_ARR_ARG(double, state),
   JNI_ARG(boolean, y), JNI_ARR_ARG(double, x),
   JNI_ARR_ARG(double, previous_state),T udf, AnyType (T::*udf_run)(AnyType& )) {
  jboolean isCopy = JNI_FALSE;
  JNI_GET_ARR_ELEMENTS(state, Double, double, env, &isCopy);
  bool in_y = j_y == JNI_TRUE;
  JNI_GET_ARR_ELEMENTS(x, Double, double, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(previous_state, Double, double, env, &isCopy);
  AnyType result = NULL;
  ArrayType* arr_state = NULL;
  ArrayType* arr_previous_state = NULL;
  DRILL_TRY
    JNI_ARRAY_LEN(state, env);
    JNI_ARRAY_LEN(previous_state, env);
    JNI_ARRAY_LEN(x, env);
    arr_state = madlib_construct_array_direct(in_state, num_state, FLOAT8OID, 8, false, 'd');

    MutableArrayHandle<double> han_state = MutableArrayHandle<double>(arr_state);
    DEFINE_MCV(double, x);
    arr_previous_state = madlib_construct_array_direct(in_previous_state, num_previous_state, FLOAT8OID, 8, false, 'd');
    ArrayHandle<double> han_previous_state = ArrayHandle<double>(arr_previous_state);
    AnyType args = AnyType();
    args << AnyType(han_state)
         << AnyType(in_y)
         << AnyType(vec_x)
         << AnyType(han_previous_state);
    result = (udf.*udf_run)(args);


  DRILL_CATCH_BEGIN

    PFREE_ARR(arr_state);
    PFREE_ARR(arr_previous_state);
    JNI_RELEASE_ARR_ELEMENTS(in_state, Double, env, j_state, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_x, Double, env, j_x, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_previous_state, Double, env, j_previous_state, 0);

  DRILL_CATCH_END_NULL


  EXTRACT_DOUBLE_ARRAY_H(out, result);
  SET_JNI_ARRAY(out, Double, double, env);


  PFREE_ARR(arr_state);
  PFREE_ARR(arr_previous_state);

  env->DeleteLocalRef(j_out);

  JNI_RELEASE_ARR_ELEMENTS(in_state, Double, env, j_state, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_x, Double, env, j_x, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_previous_state, Double, env, j_previous_state, 0);

  return j_out;
}

template <class T>
jdoubleArray step_merge_states
  (JNIEnv * env, JNI_ARR_ARG(double, state1),
   JNI_ARR_ARG(double, state2), T udf, AnyType (T::*udf_run)(AnyType& )) {
  jboolean isCopy = JNI_FALSE;
  JNI_GET_ARR_ELEMENTS(state1, Double, double, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(state2, Double, double, env, &isCopy);

  AnyType result = NULL;
  ArrayType* arr_state1 = NULL;
  ArrayType* arr_state2 = NULL;
  DRILL_TRY
    JNI_ARRAY_LEN(state1, env);
    JNI_ARRAY_LEN(state2, env);
    arr_state1 = madlib_construct_array_direct(in_state1, num_state1, FLOAT8OID, 8, false, 'd');
    arr_state2 = madlib_construct_array_direct(in_state2, num_state2, FLOAT8OID, 8, false, 'd');
    MutableArrayHandle<double> han_state1 = MutableArrayHandle<double>(arr_state1);
    ArrayHandle<double> han_state2 = ArrayHandle<double>(arr_state2);

    AnyType args = AnyType();
    args << AnyType(han_state1)
         << AnyType(han_state2);
    result = (udf.*udf_run)(args);

  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_state1);
    PFREE_ARR(arr_state2);
    JNI_RELEASE_ARR_ELEMENTS(in_state1, Double, env, j_state1, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_state2, Double, env, j_state2, 0);
  DRILL_CATCH_END_NULL

  EXTRACT_DOUBLE_ARRAY_H(out, result);
  SET_JNI_ARRAY(out, Double, double, env);


  env->DeleteLocalRef(j_out);

  PFREE_ARR(arr_state1);
  PFREE_ARR(arr_state2);
  JNI_RELEASE_ARR_ELEMENTS(in_state1, Double, env, j_state1, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_state2, Double, env, j_state2, 0);

  return j_out;

}

template <class T>
jdoubleArray step_final
  (JNIEnv * env, JNI_ARR_ARG(double, state), T udf, AnyType (T::*udf_run)(AnyType& )) {
  jboolean isCopy = JNI_FALSE;
  JNI_GET_ARR_ELEMENTS(state, Double, jdouble, env, &isCopy);

  AnyType result = NULL;
  ArrayType* arr_state = NULL;
  DRILL_TRY
    JNI_ARRAY_LEN(state, env);
    arr_state = madlib_construct_array_direct(in_state, num_state, FLOAT8OID, 8, false, 'd');
    MutableArrayHandle<double> han_state = MutableArrayHandle<double>(arr_state);

    AnyType args = AnyType();
    args << AnyType(han_state);
    result = (udf.*udf_run)(args);

  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_state);
    JNI_RELEASE_ARR_ELEMENTS(in_state, Double, env, j_state, 0);
  DRILL_CATCH_END_NULL

  EXTRACT_DOUBLE_ARRAY_H(out, result);
  SET_JNI_ARRAY(out, Double, double, env);


  env->DeleteLocalRef(j_out);

  PFREE_ARR(arr_state);
  JNI_RELEASE_ARR_ELEMENTS(in_state, Double, env, j_state, 0);

  return j_out;
}

template <class T>
jdouble step_distance
  (JNIEnv * env, JNI_ARR_ARG(double, state1),
   JNI_ARR_ARG(double, state2), T udf, AnyType (T::*udf_run)(AnyType& )) {
  jboolean isCopy = JNI_FALSE;
  JNI_GET_ARR_ELEMENTS(state1, Double, jdouble, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(state2, Double, jdouble, env, &isCopy);

  AnyType result = NULL;
  ArrayType* arr_state1 = NULL;
  ArrayType* arr_state2 = NULL;

  DRILL_TRY

    JNI_ARRAY_LEN(state1, env);
    JNI_ARRAY_LEN(state2, env);
    arr_state1 = madlib_construct_array_direct(in_state1, num_state1, FLOAT8OID, 8, false, 'd');
    arr_state2 = madlib_construct_array_direct(in_state2, num_state2, FLOAT8OID, 8, false, 'd');
    ArrayHandle<double> han_state1 = ArrayHandle<double>(arr_state1);
    ArrayHandle<double> han_state2 = ArrayHandle<double>(arr_state2);

    AnyType args = AnyType();
    args << AnyType(han_state1)
         << AnyType(han_state2);
    result = (udf.*udf_run)(args);

  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_state1);
    PFREE_ARR(arr_state2);
    JNI_RELEASE_ARR_ELEMENTS(in_state1, Double, env, j_state1, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_state2, Double, env, j_state2, 0);
  DRILL_CATCH_END_0


  PFREE_ARR(arr_state1);
  PFREE_ARR(arr_state2);
  JNI_RELEASE_ARR_ELEMENTS(in_state1, Double, env, j_state1, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_state2, Double, env, j_state2, 0);

  jdouble out = result.getAs<double>();
  return out;
}

template <class T>
jobject step_result
  (JNIEnv * env, JNI_ARR_ARG(double, state), T udf, AnyType (T::*udf_run)(AnyType& )) {
  jboolean isCopy = JNI_FALSE;
  JNI_GET_ARR_ELEMENTS(state, Double, jdouble, env, &isCopy);

  AnyType result = NULL;
  ArrayType* arr_state = NULL;
  DRILL_TRY
    JNI_ARRAY_LEN(state, env);
    arr_state = madlib_construct_array_direct(in_state, num_state, FLOAT8OID, 8, false, 'd');
    MutableArrayHandle<double> han_state = MutableArrayHandle<double>(arr_state);

    AnyType args = AnyType();
    args << AnyType(han_state);
    result = (udf.*udf_run)(args);

  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_state);
    JNI_RELEASE_ARR_ELEMENTS(in_state, Double, env, j_state, 0);
  DRILL_CATCH_END_NULL

  EXTRACT_DOUBLE_ARRAY(coef, result[0]);
  double log_likelihood = result[1].getAs<double>();
  EXTRACT_DOUBLE_ARRAY(stdErr, result[2]);
  EXTRACT_DOUBLE_ARRAY(z_stats, result[3]);
  EXTRACT_DOUBLE_ARRAY(pValues, result[4]);
  EXTRACT_DOUBLE_ARRAY(odds_ratios, result[5]);
  EXTRACT_DOUBLE_ARRAY(vcov, result[6]);
  double conditionNo = result[7].getAs<double>();
  int status = result[8].getAs<int>();
  uint64_t numRows = result[9].getAs<uint64_t>();

  SET_JNI_ARRAY(coef, Double, double, env);
  SET_JNI_ARRAY(stdErr, Double, double, env);
  SET_JNI_ARRAY(z_stats, Double, double, env);
  SET_JNI_ARRAY(pValues, Double, double, env);
  SET_JNI_ARRAY(odds_ratios, Double, double, env);
  SET_JNI_ARRAY(vcov, Double, double, env);

  jclass resultClass = env->FindClass("org/apache/drill/madlib/logregr/result/LogregrResult");
  jmethodID constructor = env->GetMethodID(resultClass, "<init>", "([DD[D[D[D[D[DDIJ)V");
  jobject resultObj = env->NewObject(
    resultClass, constructor,
    j_coef, log_likelihood, j_stdErr, j_z_stats, j_pValues,
    j_odds_ratios, j_vcov,
    conditionNo, status, (jlong)numRows);

  JNI_RELEASE_ARR_ELEMENTS(coef, Double, env, j_coef, 0);
  JNI_RELEASE_ARR_ELEMENTS(stdErr, Double, env, j_stdErr, 0);
  JNI_RELEASE_ARR_ELEMENTS(z_stats, Double, env, j_z_stats, 0);
  JNI_RELEASE_ARR_ELEMENTS(pValues, Double, env, j_pValues, 0);
  JNI_RELEASE_ARR_ELEMENTS(odds_ratios, Double, env, j_odds_ratios, 0);
  JNI_RELEASE_ARR_ELEMENTS(vcov, Double, env, j_vcov, 0);


  PFREE_ARR(arr_state);
  JNI_RELEASE_ARR_ELEMENTS(in_state, Double, env, j_state, 0);

  return resultObj;
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1cg_1step_1transition
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state),
   JNI_ARG(boolean, y), JNI_ARR_ARG(double, x),
   JNI_ARR_ARG(double, previous_state)) {
  // init_state = [0,0,0,0,0,0]
  // num_state = (num_x + 2) ^2 +2 = num_x ^ 2 + 4 * num_x + 6
  logregr_cg_step_transition udf;
  return STEP_TRANSITION_CALL(logregr_cg_step_transition);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1cg_1step_1merge_1states
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state1), JNI_ARR_ARG(double, state2)) {

  logregr_cg_step_merge_states udf;
  return STEP_MERGE_STATES_CALL(logregr_cg_step_merge_states);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1cg_1step_1final
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state)) {
  logregr_cg_step_final udf;
  return STEP_FINAL_CALL(logregr_cg_step_final);
}

JNIEXPORT jdouble JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1cg_1step_1distance
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state1), JNI_ARR_ARG(double, state2)) {
  internal_logregr_cg_step_distance udf;
  return STEP_DISTANCE_CALL(internal_logregr_cg_step_distance);
}

JNIEXPORT jobject JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1cg_1result
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state)) {
  internal_logregr_cg_result udf;
  return STEP_RESULT_CALL(internal_logregr_cg_result);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1irls_1step_1transition
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state),
   JNI_ARG(boolean, y), JNI_ARR_ARG(double, x),
   JNI_ARR_ARG(double, previous_state)) {
  // init_state = [0,0,0,0]
  // n_state = (num_x + 1) ^ 2 + 3 = num_x ^ 2 + 2 * num_x + 4
  logregr_irls_step_transition udf;
  return STEP_TRANSITION_CALL(logregr_irls_step_transition);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1irls_1step_1merge_1states
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state1), JNI_ARR_ARG(double, state2)) {

  logregr_irls_step_merge_states udf;
  return STEP_MERGE_STATES_CALL(logregr_irls_step_merge_states);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1irls_1step_1final
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state)) {
  logregr_irls_step_final udf;
  return STEP_FINAL_CALL(logregr_irls_step_final);
}

JNIEXPORT jdouble JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1irls_1step_1distance
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state1), JNI_ARR_ARG(double, state2)) {
  internal_logregr_irls_step_distance udf;
  return STEP_DISTANCE_CALL(internal_logregr_irls_step_distance);
}

JNIEXPORT jobject JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1irls_1result
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state)) {
  internal_logregr_irls_result udf;
  return STEP_RESULT_CALL(internal_logregr_irls_result);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1igd_1step_1transition
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state),
   JNI_ARG(boolean, y), JNI_ARR_ARG(double, x),
   JNI_ARR_ARG(double, previous_state)) {
  // init_state = [0,0,0,0,0]
  // num_state = num_x * (num_x + 1) + 5 = num_x ^ 2 + num_x + 5
  logregr_igd_step_transition udf;
  return STEP_TRANSITION_CALL(logregr_igd_step_transition);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1igd_1step_1merge_1states
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state1), JNI_ARR_ARG(double, state2)) {
  logregr_igd_step_merge_states udf;
  return STEP_MERGE_STATES_CALL(logregr_igd_step_merge_states);
}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1igd_1step_1final
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state)) {
  logregr_igd_step_final udf;
  return STEP_FINAL_CALL(logregr_igd_step_final);
}

JNIEXPORT jdouble JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1igd_1step_1distance
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state1), JNI_ARR_ARG(double, state2)) {
  internal_logregr_igd_step_distance udf;
  return STEP_DISTANCE_CALL(internal_logregr_igd_step_distance);
}

JNIEXPORT jobject JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1igd_1result
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, state)) {
  internal_logregr_igd_result udf;
  return STEP_RESULT_CALL(internal_logregr_igd_result);
}

JNIEXPORT jboolean JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1predict
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, coef), JNI_ARR_ARG(double, col_ind_var)) {

  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(coef, Double, double, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(col_ind_var, Double, double, env, &isCopy);
  JNI_ARRAY_LEN(coef, env);
  JNI_ARRAY_LEN(col_ind_var, env);
  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MCV(double, coef);
    DEFINE_MCV(double, col_ind_var);

    logregr_predict udf;
    AnyType args = AnyType();
    args << AnyType(vec_coef)
         << AnyType(vec_col_ind_var);
    result = udf.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_coef, Double, env, j_coef, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_col_ind_var, Double, env, j_col_ind_var, 0);
  DRILL_CATCH_END_0

  jboolean out = static_cast<jboolean>(result.getAs<bool>() ? JNI_TRUE : JNI_FALSE);

  JNI_RELEASE_ARR_ELEMENTS(in_coef, Double, env, j_coef, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_col_ind_var, Double, env, j_col_ind_var, 0);

  return out;

}

JNIEXPORT jdouble JNICALL Java_org_apache_drill_madlib_jni_DrillJni_logregr_1predict_1prob
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, coef), JNI_ARR_ARG(double, col_ind_var)) {
  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(coef, Double, double, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(col_ind_var, Double, double, env, &isCopy);
  JNI_ARRAY_LEN(coef, env);
  JNI_ARRAY_LEN(col_ind_var, env);
  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MCV(double, coef);
    DEFINE_MCV(double, col_ind_var);

    logregr_predict_prob udf;
    AnyType args = AnyType();
    args << AnyType(vec_coef)
         << AnyType(vec_col_ind_var);
    result = udf.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_coef, Double, env, j_coef, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_col_ind_var, Double, env, j_col_ind_var, 0);
  DRILL_CATCH_END_0

  jdouble out = result.getAs<double>();

  JNI_RELEASE_ARR_ELEMENTS(in_coef, Double, env, j_coef, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_col_ind_var, Double, env, j_col_ind_var, 0);

  return out;
}
#endif //MADLIB_DRILL_LOGISTIC_HPP
