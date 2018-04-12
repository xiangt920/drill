//
// Created by Xiang on 2018/3/13.
//

#ifndef MADLIB_DRILL_LINEAR_HPP
#define MADLIB_DRILL_LINEAR_HPP

using namespace madlib::dbconnector::postgres;
using namespace madlib::modules::regress;
using namespace madlib::dbal::eigen_integration;
JNIEXPORT jbyteArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_linregr_1transition
  (JNIEnv * env, jobject, jbyteArray j_state, jdouble j_y, jdoubleArray j_x) {

  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(state, Byte, jbyte, env, &isCopy);
  double in_y = j_y;
  JNI_GET_ARR_ELEMENTS(x, Double, jdouble, env, &isCopy);
  JNI_ARRAY_LEN(x, env);
  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MCV(double, x);

    DEFINE_MBS(state);
    linregr_transition lt;
    AnyType args = AnyType();
    args << AnyType(state_str)
         << AnyType(in_y)
         << AnyType(vec_x, true);
    result = lt.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_x, Double, env, j_x, 0);
  DRILL_CATCH_END_NULL

  DRILL_BYTE_ARR_RESULT(result, out, env);

  JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_x, Double, env, j_x, 0);
  return out;
}

JNIEXPORT jobject JNICALL Java_org_apache_drill_madlib_jni_DrillJni_linrger_1final
  (JNIEnv * env, jobject, jbyteArray j_state) {
  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(state, Byte, jbyte, env, &isCopy);

  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MBS(state);
    AnyType args = AnyType();
    args << AnyType(state_str);

    linregr_final lf;
    result = lf.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
  DRILL_CATCH_END_NULL

  EXTRACT_DOUBLE_ARRAY(coef, result[0]);
  double r2 = result[1].getAs<double>();
  EXTRACT_DOUBLE_ARRAY(stdErr, result[2]);
  EXTRACT_DOUBLE_ARRAY(tStats, result[3]);
  EXTRACT_DOUBLE_ARRAY(pValues, result[4]);
  double conditionNo = result[5].getAs<double>();
  int64 numRows = result[6].getAs<int64>();
  EXTRACT_DOUBLE_ARRAY(vcov, result[7]);

  SET_JNI_ARRAY(coef, Double, double, env);
  SET_JNI_ARRAY(stdErr, Double, double, env);
  SET_JNI_ARRAY(tStats, Double, double, env);
  SET_JNI_ARRAY(pValues, Double, double, env);
  SET_JNI_ARRAY(vcov, Double, double, env);

  jclass resultClass = env->FindClass("org/apache/drill/madlib/linregr/result/LinregrResult");
  jmethodID constructor = env->GetMethodID(resultClass, "<init>", "([DD[D[D[DDJ[D)V");
  jobject resultObj = env->NewObject(
    resultClass, constructor,
    j_coef, r2, j_stdErr, j_tStats, j_pValues, conditionNo, numRows, j_vcov);

  JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
  JNI_RELEASE_ARR_ELEMENTS(coef, Double, env, j_coef, 0);
  JNI_RELEASE_ARR_ELEMENTS(stdErr, Double, env, j_stdErr, 0);
  JNI_RELEASE_ARR_ELEMENTS(tStats, Double, env, j_tStats, 0);
  JNI_RELEASE_ARR_ELEMENTS(pValues, Double, env, j_pValues, 0);
  JNI_RELEASE_ARR_ELEMENTS(vcov, Double, env, j_vcov, 0);

  return resultObj;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_hetero_1linregr_1transition
  (JNIEnv * env, jobject, jbyteArray j_state, jdouble j_y, jdoubleArray j_x, jdoubleArray j_coef) {
  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(state, Byte, jbyte, env, &isCopy);
  double in_y = j_y;
  JNI_GET_ARR_ELEMENTS(x, Double, jdouble, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(coef, Double, jdouble, env, &isCopy);

  JNI_ARRAY_LEN(x, env);
  JNI_ARRAY_LEN(coef, env);

  AnyType result = NULL;
  DRILL_TRY

  DEFINE_MBS(state);

  DEFINE_MCV(double, x);
  DEFINE_MCV(double, coef);

  hetero_linregr_transition hlt;
  AnyType args = AnyType();
  args << AnyType(state_str)
       << AnyType(in_y)
       << AnyType(vec_x)
       << AnyType(vec_coef);
  result = hlt.run(args);

  DRILL_CATCH_BEGIN
  JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_x, Double, env, j_x, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_coef, Double, env, j_coef, 0);
  DRILL_CATCH_END_NULL

  DRILL_BYTE_ARR_RESULT(result, out, env);

  JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_x, Double, env, j_x, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_coef, Double, env, j_coef, 0);
  return out;

}


JNIEXPORT jobject JNICALL Java_org_apache_drill_madlib_jni_DrillJni_hetero_1linregr_1final
  (JNIEnv * env, jobject, JNI_ARR_ARG(byte, state)) {
  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(state, Byte, jbyte, env, &isCopy);

  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MBS(state);
    AnyType args = AnyType();
    args << AnyType(state_str);

    hetero_linregr_final lf;
    result = lf.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);
  DRILL_CATCH_END_NULL

  jclass resultClass = env->FindClass("org/apache/drill/madlib/linregr/result/HeteroskedasticityTestResult");
  jmethodID constructor = env->GetMethodID(resultClass, "<init>", "(DD)V");

  double bp_stats = result[0].getAs<double>();
  double bp_p_value = result[1].getAs<double>();

  jobject resultObj = env->NewObject(
    resultClass, constructor,
    bp_stats, bp_p_value);

  JNI_RELEASE_ARR_ELEMENTS(in_state, Byte, env, j_state, 0);

  return resultObj;

}

JNIEXPORT jbyteArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_linregr_1merge_1states
  (JNIEnv * env, jobject, JNI_ARR_ARG(byte, state1), JNI_ARR_ARG(byte, state2)) {
  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(state1, Byte, jbyte, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(state2, Byte, jbyte, env, &isCopy);

  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MBS(state1);
    DEFINE_MBS(state2);
    linregr_merge_states lms;
    AnyType args = AnyType();
    args << AnyType(state1_str)
         << AnyType(state2_str);
    result = lms.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_state1, Byte, env, j_state1, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_state2, Byte, env, j_state2, 0);
  DRILL_CATCH_END_NULL

  DRILL_BYTE_ARR_RESULT(result, out, env);

  JNI_RELEASE_ARR_ELEMENTS(in_state1, Byte, env, j_state1, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_state2, Byte, env, j_state2, 0);

  return out;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_hetero_1linregr_1merge_1states
  (JNIEnv * env, jobject, JNI_ARR_ARG(byte, state1), JNI_ARR_ARG(byte, state2)) {
  jboolean isCopy = 0;
  JNI_GET_ARR_ELEMENTS(state1, Byte, jbyte, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(state2, Byte, jbyte, env, &isCopy);

  AnyType result = NULL;
  DRILL_TRY
    DEFINE_MBS(state1);
    DEFINE_MBS(state2);
    linregr_merge_states lms;
    AnyType args = AnyType();
    args << AnyType(state1_str)
         << AnyType(state2_str);
    result = lms.run(args);
  DRILL_CATCH_BEGIN
    JNI_RELEASE_ARR_ELEMENTS(in_state1, Byte, env, j_state1, 0);
    JNI_RELEASE_ARR_ELEMENTS(in_state2, Byte, env, j_state2, 0);
  DRILL_CATCH_END_NULL

  DRILL_BYTE_ARR_RESULT(result, out, env);

  JNI_RELEASE_ARR_ELEMENTS(in_state1, Byte, env, j_state1, 0);
  JNI_RELEASE_ARR_ELEMENTS(in_state2, Byte, env, j_state2, 0);

  return out;

}


#endif //MADLIB_DRILL_LINEAR_HPP
