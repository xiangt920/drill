/* ----------------------------------------------------------------------- *//**
 *
 * @file Compatibility.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_COMPATIBILITY_HPP
#define MADLIB_POSTGRES_COMPATIBILITY_HPP

#include <iostream>
#include <common/array.h>

namespace madlib {

namespace dbconnector {

namespace postgres {

namespace {
// No need to make these function accessible outside of the postgres namespace.

#ifndef FLOAT8ARRAYOID
#define FLOAT8ARRAYOID 1022
#endif
#define TEXTARRAYOID    1009
#define INT4ARRAYOID    1007
#ifndef INT8ARRAYOID
#define INT8ARRAYOID 1016
#endif

#ifndef PG_GET_COLLATION
// See madlib_InitFunctionCallInfoData()
#define PG_GET_COLLATION()  InvalidOid
#endif

#ifndef SearchSysCache1
// See madlib_SearchSysCache1()
#define SearchSysCache1(cacheId, key1) \
  SearchSysCache(cacheId, key1, 0, 0, 0)
#endif

/*
 * In commit 2d4db3675fa7a2f4831b755bc98242421901042f,
 * by Tom Lane <tgl@sss.pgh.pa.us> Wed, 6 Jun 2007 23:00:50 +0000,
 * is_array_type was changed to type_is_array
 */
#if defined(is_array_type) && !defined(type_is_array)
#define type_is_array(x) is_array_type(x)
#endif

#if PG_VERSION_NUM < 90000

/**
 * The following has existed in PostgresSQL since commit ID
 * d5768dce10576c2fb1254c03fb29475d4fac6bb4, by
 * Tom Lane <tgl@sss.pgh.pa.us>	Mon, 8 Feb 2010 20:39:52 +0000.
 */

/* AggCheckCallContext can return one of the following codes, or 0: */
#define AGG_CONTEXT_AGGREGATE  1    /* regular aggregate */
#define AGG_CONTEXT_WINDOW    2    /* window function */

/**
 * @brief Test whether we are currently in an aggregate calling context.
 *
 * Knowing whether we are in an aggregate calling context is useful, because it
 * allows write access to the transition state of the aggregate function.
 * At all other time, modifying a pass-by-reference input is strictly forbidden:
 * http://developer.postgresql.org/pgdocs/postgres/xaggr.html
 *
 * This function is essentially a copy of AggCheckCallContext from
 * backend/executor/nodeAgg.c, which is part of PostgreSQL >= 9.0.
 */
inline
int
AggCheckCallContext(jobject fcinfo, MemoryContext *aggcontext) {

  return 0;
}


#endif // PG_VERSION_NUM < 90000

} // namespace

/**
 * @brief construct an array of zero values.
 * @note the supported types are: int2, int4, int8, float4 and float8
 *
 */
static ArrayType *construct_md_array_zero
  (
    int ndims,
    int *dims,
    int *lbs,
    Oid elmtype,
    int elmlen,
    bool elmbyval,
    char elmalign
  ) {
  ArrayType *result;
  size_t nbytes = 0;
  int32 dataoffset;
  int nelems;
  (void) elmbyval;

  if (ndims < 0) {             /* we do allow zero-dimension arrays */
    eerror("invalid number of dimensions: %d", ndims);
  }
  if (ndims > MAXDIM) {
    eerror("number of array dimensions (%d) exceeds the maximum allowed (%d)", ndims, MAXDIM);
  }

  /* fast track for empty array */
  if (ndims == 0)
    return construct_empty_array(elmtype);

  nelems = ArrayGetNItems(ndims, dims);

  /* compute required space */

  switch (elmtype) {
    case INT2OID:
      nbytes = static_cast<size_t>(std::max(elmlen, 2) * nelems);

      break;
    case INT4OID:
      nbytes = static_cast<size_t>(std::max(elmlen, 2) * nelems);
      break;
    case INT8OID:
      nbytes = static_cast<size_t>(std::max(elmlen, 2) * nelems);
      break;
    case FLOAT4OID:
      nbytes = static_cast<size_t>(std::max(elmlen, 2) * nelems);
      break;
    case FLOAT8OID:
      nbytes = static_cast<size_t>(std::max(elmlen, 2) * nelems);
      break;
    default:
      eerror("the support types are INT2, INT4, INT8, FLOAT4 and FLOAT8");
      break;
  }
  if (!AllocSizeIsValid(nbytes))
    eerror("array size exceeds the maximum allowed (%d)",
           (int) MaxAllocSize);


  dataoffset = 0;         /* marker for no null bitmap */
  size_t size = sizeof(ArrayType);
  result = (ArrayType *) palloc(size);
  result->ndim = ndims;
  result->dataoffset = static_cast<Datum>(dataoffset);
  result->elemtype = elmtype;
  result->data = PointerGetDatum(palloc0(nbytes));
  size = ndims * sizeof(int);
  result->dims = static_cast<int *>(palloc(size));
  result->bounds = static_cast<int *>(palloc(size));
  memcpy(ARR_DIMS(result), dims, size);
  memcpy(ARR_LBOUND(result), lbs, size);

  return result;
}

/**
 * @brief construct an array of zero values.
 * @note the supported types are: int2, int4, int8, float4 and float8
 */
static ArrayType *construct_array_zero
  (
    int nelems,
    Oid elmtype,
    int elmlen,
    bool elmbyval,
    char elmalign
  ) {
  int dims[1];
  int lbs[1];

  dims[0] = nelems;
  lbs[0] = 1;

  return
    construct_md_array_zero(
      1, dims, lbs, elmtype, elmlen, elmbyval, elmalign);
}

inline ArrayType *madlib_construct_md_array
  (
    Datum *elems,
    bool *nulls,
    int ndims,
    int *dims,
    int *lbs,
    Oid elmtype,
    int elmlen,
    bool elmbyval,
    char elmalign
  ) {
  return
    elems ?
    construct_md_array(
      elems, nulls, ndims, dims, lbs, elmtype, elmlen, elmbyval,
      elmalign) :
    construct_md_array_zero(
      ndims, dims, lbs, elmtype, elmlen, elmbyval, elmalign);
}

inline ArrayType *madlib_construct_array
  (
    Datum *elems,
    int nelems,
    Oid elmtype,
    int elmlen,
    bool elmbyval,
    char elmalign
  ) {
  return elems ?
         construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign) :
         construct_array_zero(nelems, elmtype, elmlen, elmbyval, elmalign);
}

inline ArrayType *construct_array_internal(void *data, int nelems, Oid elmtype, int elmlen) {
  ArrayType *result = static_cast<ArrayType *>(palloc0(sizeof(ArrayType)));
  size_t size = static_cast<size_t>(nelems * elmlen);
  void* data_cp = palloc0(size);
  memcpy(data_cp, data, size);
  result->data = PointerGetDatum(data_cp);
  result->bounds = static_cast<int *>(palloc0(sizeof(int)));
  result->bounds[0] = 1;
  result->dims = static_cast<int *>(palloc0(sizeof(int)));
  result->dims[0] = nelems;
  result->ndim = 1;
  result->dataoffset = 0;
  result->elemtype = elmtype;
  return result;
}

inline ArrayType *madlib_construct_array_direct(void *elems,
                                         int nelems,
                                         Oid elmtype,
                                         int elmlen,
                                         bool elmbyval,
                                         char elmalign) {
  return elems ?
         construct_array_internal(elems, nelems, elmtype, elmlen) :
         construct_array_zero(nelems, elmtype, elmlen, elmbyval, elmalign);
}


} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_COMPATIBILITY_HPP)
