//
// Created by Xiang on 2018/2/28.
//

#ifndef MADLIB_DRILL_JNI_OPS_H
#define MADLIB_DRILL_JNI_OPS_H

#include <cstring>

#define JNI_ARG(_type, _name) \
  j ## _type j_ ## _name

#define JNI_ARR_ARG(_type, _name) \
  j ## _type ## Array j_ ## _name

#define JNI_ARRAY_LEN(_name, env) \
  jsize num_ ## _name = (env)->GetArrayLength(j_ ## _name)

#define JNI_ARRAY_EMPTY(_name) num_ ## _name == 0

#define JNI_GET_ARR_ELEMENTS(_name, in_type, out_type, env, isCopy) \
  out_type* in_ ## _name = (env)->Get ## in_type ## ArrayElements(j_ ## _name, isCopy)

#define JNI_GET_2D_ARR_ELEMENTS(_name, in_type, out_type, env, isCopy, dims) \
  out_type** in_ ## _name; \
  do { \
    j ## out_type ## Array tmp_array; \
    JNI_ARRAY_LEN(_name, env); \
    dims_ ## dims[0] = num_ ## _name;\
    in_ ## _name = (out_type **)(palloc(sizeof(out_type*) * num_ ## _name)); \
    for (int i = 0; i < num_ ## _name; ++i) { \
      tmp_array = (j ## out_type ## Array)((env)->GetObjectArrayElement(j_ ## _name, i)); \
      int num_ele = (env)->GetArrayLength(tmp_array); \
      if (i == 0) dims_ ## dims[1] = num_ele; \
      in_ ## _name[i] = (out_type *)(palloc(sizeof(out_type) * num_ele)); \
      j ## out_type* tmp_eles = (env)->Get ## in_type ## ArrayElements(tmp_array, isCopy); \
      memcpy(in_ ## _name[i], tmp_eles, num_ele * sizeof(out_type)); \
      (env)->Release ## in_type ## ArrayElements(tmp_array, tmp_eles, 0); \
    } \
  } while (0)


#define JNI_RELEASE_ARR_ELEMENTS(_name, _type, env, arr, mode) \
  (env)->Release ## _type ## ArrayElements(arr, _name, mode);

#define JNI_RELEASE_ARR_BRIEF(_name, _type, env, mode) \
  JNI_RELEASE_ARR_ELEMENTS(in_ ## _name, _type, env, j_ ## _name, mode)

#define JNI_RELEASE_IN_POINGTER_ARR(_name) \
  PFREE_POINTER_ARR(in_ ## _name, num_ ## _name)

#define JNI_RELEASE_OUT_POINGTER_ARR(_name) \
  pfree(_name)

#define SET_JNI_ARRAY(_name, in_type, out_type, env) \
  j ## out_type ## Array j_ ## _name = (env)->New ## in_type ## Array(_name ## Num); \
  (env)->Set ## in_type ## ArrayRegion(j_ ## _name, 0, _name ## Num, _name)

#define JNI_DELETE_LOCAL_REF(_name, env) \
  (env)->DeleteLocalRef(j_ ## _name)

#define JNI_RETURN(_name) return j_ ## _name

#define JNI_CONSTRUCT_ARR_D(_name, _type, _oid, _len, _byval, _align) \
  arr_ ## _name = madlib_construct_array_direct(in_ ## _name, num_ ## _name, _oid, _len, _byval, _align); \
  MutableArrayHandle<_type> han_ ## _name = MutableArrayHandle<_type>(arr_ ## _name)

#define JNI_CONSTRUCT_2D_ARR_D(_name, _type, _oid, _len, _byval, _align, param) \
  do { \
    Datum *elems = (Datum *) (palloc(sizeof(Datum) * dims_ ## param[0] * dims_ ## param[1])); \
    int i = 0; \
    for (int j = 0; j < dims_ ## param[0]; ++j) { \
      for (int k = 0; k < dims_ ## param[1]; ++k) { \
        elems[i] = PointerGetDatum(in_ ## _name[j] + k); \
        i++; \
      } \
    } \
    arr_ ## _name = madlib_construct_md_array(elems, NULL, 2, dims_ ## param, lbs_ ## param, FLOAT8OID, 8, false, 'd'); \
    pfree(elems); \
  } while (0); \
  MutableArrayHandle<_type> han_ ## _name = MutableArrayHandle<_type>(arr_ ## _name)

#define ANYTYPE_FROM_HANDLE(_name) AnyType(han_ ## _name)
#define ANYTYPE_FROM_VECTOR(_name) AnyType(vec_ ## _name)

#define SET_JNI_2D_ARRAY(_name, _class_name, in_type, out_type, env) \
  jobjectArray j_ ## _name = NULL; \
  do { \
    jclass dblArrClass = (env)->FindClass(_class_name); \
    j_ ## _name = (env)->NewObjectArray(_name ## Num[0], dblArrClass, NULL); \
    for (int i = 0; i < _name ## Num[0]; ++i) { \
      j ## out_type ## Array d_arr = (env)->New ## in_type ## Array(_name ## Num[1]); \
      (env)->Set ## in_type ## ArrayRegion(d_arr, 0, _name ## Num[1], (_name)[i]); \
      (env)->SetObjectArrayElement(j_ ## _name, i, d_arr); \
      (env)->DeleteLocalRef(d_arr); \
    } \
  } while(false)

static inline jdouble * copy_double_array_from_jni(
  int * dims, int * bounds,
  JNIEnv * env, jarray array, jboolean * isCopy);

#endif //MADLIB_DRILL_JNI_OPS_H
