//
// Created by Xiang on 2018/2/28.
//

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <cmath>
#include "common.h"
#include "array.h"
#include <jni/DrillJni.h>
#include <jni/jni_ops.hpp>
#include <stdexcept>


using namespace std;

static
inline
float8
float8_dot(float8 op1, float8 op2, float8 opt_op){
  return op2 + op1 * opt_op;
}

static
inline
float8
float8_div(float8 op1, float8 op2, float8 opt_op) {
  (void) op2;
  if (opt_op == 0) {
    eerror("Arrays with element 0 can not be use in the denominator");
  }
  return op1 / opt_op;
}

static
inline
int64
int64_div(int64 num, int64 denom){
  if (denom == 0) {
    eerror("Arrays with element 0 can not be use in the denominator");
  }
  return num / denom;
}

static
inline
float8
float8_mult(float8 op1, float8 op2, float8 opt_op){
  (void) op2;
  return op1 * opt_op;
}

// ------------------------------------------------------------------------
// finalize functions
// ------------------------------------------------------------------------
static
inline
Datum
noop_finalize(Datum elt,int size,Oid element_type){
  (void) size; /* avoid warning about unused parameter */
  (void) element_type; /* avoid warning about unused parameter */
  return elt;
}

static
inline
Datum
average_finalize(Datum elt,int size,Oid element_type){
  float8 value = datum_float8_cast(elt, element_type);
  if (size == 0) {
    elog(WARNING, "Input array only contains NULL or NaN, returning 0");
    return Float8GetDatum(0);
  }
  return Float8GetDatum(value/(float8)size);
}

static
inline
Datum
average_root_finalize(Datum elt,int size,Oid element_type){
  float8 value = datum_float8_cast(elt, element_type);
  if (size == 0) {
    return Float8GetDatum(0);
  } else if (size == 1) {
    return Float8GetDatum(0);
  } else {
    return Float8GetDatum(sqrt(value/((float8)size - 1)));
  }
}

/*
 * Add elt1*elt2 to result.
 */
static
inline
Datum
element_dot(Datum element, Oid elt_type, Datum result,
            Oid result_type, Datum opt_elt, Oid opt_type){
  return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_dot);
}

static
inline
Datum
element_mult(Datum element, Oid elt_type, Datum result,
             Oid result_type, Datum opt_elt, Oid opt_type){
  return element_op(element, elt_type, result, result_type, opt_elt, opt_type, float8_mult);
}

static
inline
Datum
element_op(Datum element, Oid elt_type, Datum result,
           Oid result_type, Datum opt_elt, Oid opt_type,
           float8 (*op)(float8, float8, float8)) {
  if (op == float8_div &&
      (result_type == INT2OID ||
       result_type == INT4OID ||
       result_type == INT8OID)) {
    int64 num   = datum_int64_cast(element, elt_type);
    int64 denom = datum_int64_cast(opt_elt, opt_type);
    return int64_datum_cast(int64_div(num, denom), result_type);
  }
  float8 elt = datum_float8_cast(element, elt_type   );
  float8 res = datum_float8_cast(result , result_type);
  float8 opt = datum_float8_cast(opt_elt, opt_type   );
  return float8_datum_cast((*op)(elt, res, opt), result_type);
}

static inline jdouble * copy_double_array_from_jni(
  int * dims, int * bounds,
  JNIEnv * env, jarray j_array, jboolean * isCopy) {
  int num1 = 0;
  JNI_ARRAY_LEN(array, env);
  dims[0] = num_array;
  jdoubleArray ptrs[num_array];
  for (jsize i = 0; i < num_array; ++i) {
    jdoubleArray j_element = static_cast<jdoubleArray>(env->GetObjectArrayElement(static_cast<jobjectArray>(j_array), i));
    ptrs[i] = j_element;
    JNI_ARRAY_LEN(element, env);
    dims[1] = num_element;
    bounds[i] = 1;
    num1 += num_element;
  }
  jdouble* all_elements = static_cast<jdouble *>(palloc(sizeof(jdouble) * num1));
  jdouble *tmp = all_elements;
  for (int j = 0; j < num_array; ++j) {
    jdoubleArray j_ele = ptrs[j];
    JNI_GET_ARR_ELEMENTS(ele,Double, jdouble, env, isCopy);
    memcpy(tmp, DatumGetPointer(in_ele), dims[1] * sizeof(jdouble));
    tmp += dims[1];
    JNI_RELEASE_ARR_ELEMENTS(in_ele, Double, env, j_ele, 0);
  }
  return all_elements;
}


JNIEXPORT jdouble JNICALL Java_org_apache_drill_madlib_jni_DrillJni_array_1dot
  (JNIEnv * env, jobject obj, jobjectArray j_array1, jobjectArray j_array2) {
  JNI_ARRAY_LEN(array1, env);
  JNI_ARRAY_LEN(array2, env);
  int ndims = 2;

  jboolean isCopy = 0;

  int *dims1 = static_cast<int *>(palloc(sizeof(int) * ndims));
  int *bounds1 = static_cast<int *>(palloc(sizeof(int) * ndims));
  jdouble *d_array1 = copy_double_array_from_jni(dims1, bounds1, env, j_array1, &isCopy);

  int *dims2 = static_cast<int *>(palloc(sizeof(int) * num_array1));
  int *bounds2 = static_cast<int *>(palloc(sizeof(int) * num_array2));
  jdouble *d_array2 = copy_double_array_from_jni(dims2, bounds2, env, j_array2, &isCopy);

  ARR_FROM_PTR(a1, ndims, FLOAT8OID, dims1, bounds1, d_array1);
  ARR_FROM_PTR(a2, ndims, FLOAT8OID, dims2, bounds2, d_array2);

  try {
    Datum res = General_2Array_to_Element(a1, a2, element_dot, noop_finalize);
    PFREE_ARR(a1);
    PFREE_ARR(a2);

    jdouble d_res = DatumGetFloat8(res);
    return d_res;
  } catch (std::exception &error) {

    PFREE_ARR(a1);
    PFREE_ARR(a2);
    jclass _exp_class = env->FindClass("org/apache/drill/madlib/jni/DataProcessError");
    env->ThrowNew(_exp_class, error.what());
    return 0;
  }

}

JNIEXPORT jdoubleArray JNICALL Java_org_apache_drill_madlib_jni_DrillJni_array_1scalar_1mult
  (JNIEnv * env, jobject, JNI_ARR_ARG(double, v1), JNI_ARG(double, v2)) {
  jboolean isCopy = JNI_FALSE;
  JNI_GET_ARR_ELEMENTS(v1, Double, double, env, &isCopy);
  JNI_ARRAY_LEN(v1, env);
  int *dims = static_cast<int *>(palloc(sizeof(int)));
  int *lbs = static_cast<int *>(palloc(sizeof(int)));
  dims[0] = num_v1;
  lbs[0] = 0;

  ArrayType * result = NULL;
  ARR_FROM_PTR(v1, 1, FLOAT8OID, dims, lbs, in_v1);
  DRILL_TRY

    result = General_Array_to_Array(v1, Float8GetDatum(j_v2), element_mult);

  DRILL_CATCH_BEGIN
    PFREE_ARR(v1);

  DRILL_CATCH_END_NULL

  double* data = (double*)DatumGetPointer(result->data);
  int dataNum = ArrayGetNItems(result->ndim, result->dims);
  SET_JNI_ARRAY(data, Double, double, env);

  PFREE_ARR(result);
  PFREE_ARR(v1);
  JNI_RETURN(data);
}