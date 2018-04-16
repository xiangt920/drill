//
// Created by Xiang on 2018/3/30.
//

#ifndef MADLIB_DRILL_CORRELATION_HPP
#define MADLIB_DRILL_CORRELATION_HPP

using namespace madlib::modules::stats;

JNIEXPORT jobjectArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_correlation_1transition
  (JNIEnv *env, jobject, JNI_ARR_ARG(object, state), JNI_ARR_ARG(double, x), JNI_ARR_ARG(double, mean)) {
  jboolean isCopy = JNI_FALSE;
  INIT_ARR_DIMS(state, 2);
  INIT_ARR_LBS(state, 2);
  JNI_GET_2D_ARR_ELEMENTS(state, Double, double, env, &isCopy, state);
  JNI_GET_ARR_ELEMENTS(x, Double, double, env, &isCopy);
  JNI_GET_ARR_ELEMENTS(mean, Double, double, env, &isCopy);

  JNI_ARRAY_LEN(state, env);
  JNI_ARRAY_LEN(x, env);
  JNI_ARRAY_LEN(mean, env);
  AnyType result = NULL;
  ArrayType *arr_state = NULL;
  DRILL_TRY

    AnyType args = AnyType();
    if (JNI_ARRAY_EMPTY(state)) {
      args << AnyType();
    } else {
      JNI_CONSTRUCT_2D_ARR_D(state, double, FLOAT8OID, 8, false, 'd', state);
      args << ANYTYPE_FROM_HANDLE(state);
    }
    DEFINE_MCV(double, x);
    DEFINE_MCV(double, mean);
    args << ANYTYPE_FROM_VECTOR(x)
         << ANYTYPE_FROM_VECTOR(mean);

    correlation_transition udf;
    result = udf.run(args);
  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_state);
    JNI_RELEASE_IN_POINGTER_ARR(state);
    JNI_RELEASE_ARR_BRIEF(x, Double, env, 0);
    JNI_RELEASE_ARR_BRIEF(mean, Double, env, 0);
    std::cerr << "exception in transition" << std::endl;
  DRILL_CATCH_END_NULL

  EXTRACT_2D_DOUBLE_ARRAY_H(out, result);
  SET_JNI_2D_ARRAY(out, "[D", Double, double, env);

  PFREE_ARR(arr_state);
  JNI_RELEASE_OUT_POINGTER_ARR(out);
  JNI_RELEASE_IN_POINGTER_ARR(state);
  JNI_RELEASE_ARR_BRIEF(x, Double, env, 0);
  JNI_RELEASE_ARR_BRIEF(mean, Double, env, 0);

  JNI_RETURN(out);

}


JNIEXPORT jobjectArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_correlation_1merge
  (JNIEnv *env, jobject, JNI_ARR_ARG(object, left_state), JNI_ARR_ARG(object, right_state)) {
  jboolean isCopy = JNI_FALSE;

  INIT_ARR_DIMS(state, 2);
  INIT_ARR_LBS(state, 2);

  JNI_GET_2D_ARR_ELEMENTS(left_state, Double, double, env, &isCopy, state);
  JNI_GET_2D_ARR_ELEMENTS(right_state, Double, double, env, &isCopy, state);

  JNI_ARRAY_LEN(left_state, env);
  JNI_ARRAY_LEN(right_state, env);

  AnyType result = NULL;
  ArrayType *arr_left_state = NULL;
  ArrayType *arr_right_state = NULL;

  DRILL_TRY
    AnyType args = AnyType();
    JNI_CONSTRUCT_2D_ARR_D(left_state, double, FLOAT8OID, 8, false, 'd', state);
    args << ANYTYPE_FROM_HANDLE(left_state);
    JNI_CONSTRUCT_2D_ARR_D(right_state, double, FLOAT8OID, 8, false, 'd', state);
    args << ANYTYPE_FROM_HANDLE(right_state);
    correlation_merge_states udf;
    result = udf.run(args);
  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_left_state);
    PFREE_ARR(arr_right_state);

    JNI_RELEASE_IN_POINGTER_ARR(left_state);
    JNI_RELEASE_IN_POINGTER_ARR(right_state);
  DRILL_CATCH_END_NULL

  EXTRACT_2D_DOUBLE_ARRAY_H(out, result);
  SET_JNI_2D_ARRAY(out, "[D", Double, double, env);

  PFREE_ARR(arr_left_state);
  PFREE_ARR(arr_right_state);

  JNI_RELEASE_OUT_POINGTER_ARR(out);
  JNI_RELEASE_IN_POINGTER_ARR(left_state);
  JNI_RELEASE_IN_POINGTER_ARR(right_state);
  JNI_RETURN(out);

}


JNIEXPORT jobjectArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_correlation_1final
  (JNIEnv *env, jobject, JNI_ARR_ARG(object, state)) {
  jboolean isCopy = JNI_FALSE;
  INIT_ARR_DIMS(state, 2);
  INIT_ARR_LBS(state, 2);
  JNI_GET_2D_ARR_ELEMENTS(state, Double, double, env, &isCopy, state);
  JNI_ARRAY_LEN(state, env);
  AnyType result = NULL;
  ArrayType *arr_state = NULL;

  DRILL_TRY
    AnyType args = AnyType();
    JNI_CONSTRUCT_2D_ARR_D(state, double, FLOAT8OID, 8, false, 'd', state);
    args << ANYTYPE_FROM_HANDLE(state);
    correlation_final udf;
    result = udf.run(args);
  DRILL_CATCH_BEGIN
    PFREE_ARR(arr_state);

    JNI_RELEASE_IN_POINGTER_ARR(state);
  DRILL_CATCH_END_NULL

  EXTRACT_2D_DOUBLE_ARRAY_H(out, result);
  SET_JNI_2D_ARRAY(out, "[D", Double, double, env);
  PFREE_ARR(arr_state);

  JNI_RELEASE_OUT_POINGTER_ARR(out);
  JNI_RELEASE_IN_POINGTER_ARR(state);
  JNI_RETURN(out);

}

#endif //MADLIB_DRILL_CORRELATION_HPP
