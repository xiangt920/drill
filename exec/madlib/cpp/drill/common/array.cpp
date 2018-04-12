//
// Created by Xiang on 2018/2/27.
//

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include "common.h"
#include "array.h"

/*
 * Copy datum to *dest and return total space used (including align padding)
 *
 * Caller must have handled case of NULL element
 */
static int
ArrayCastAndSet(Datum src,
                int typlen,
                bool typbyval,
                char typalign,
                char *dest)
{
  int			inc;

  if (typlen > 0)
  {
    if (typbyval)
      store_att_byval(dest, src, typlen);
    else
      memcpy(dest, DatumGetPointer(src), typlen);
    inc = att_align_nominal(typlen, typalign);
  }
  else
  {
    inc = att_addlength_datum(0, typlen, src);
    memcpy(dest, DatumGetPointer(src), inc);
    inc = att_align_nominal(inc, typalign);
  }

  return inc;
}

void
CopyArrayEls(ArrayType *array,
             Datum *values,
             bool *nulls,
             int nitems,
             int typlen,
             bool typbyval,
             char typalign,
             bool freedata)
{
  char	   *p = ARR_DATA_PTR(array);
  int			i;

  if (typbyval)
    freedata = false;

  for (i = 0; i < nitems; i++)
  {
    if (nulls && nulls[i])
    {
      nulls[i] = true;
    }
    else
    {
      p += ArrayCastAndSet(values[i], typlen, typbyval, typalign, p);
      if (freedata)
        free(DatumGetPointer(values[i]));
    }
  }

}

ArrayType *
construct_md_array(Datum *elems,
                   bool *nulls,
                   int ndims,
                   int *dims,
                   int *lbs,
                   Oid elmtype, int elmlen, bool elmbyval, char elmalign)
{
  ArrayType  *result;
  bool            hasnulls;
  int32           nbytes;
  int32           dataoffset;
  int                     i;
  int                     nelems;

  if (ndims < 0)                          /* we do allow zero-dimension arrays */
    eerror("invalid number of dimensions: %d", ndims);
  if (ndims > MAXDIM)
    eerror("number of array dimensions (%d) exceeds the maximum allowed (%d)",
            ndims, MAXDIM);

  /* fast track for empty array */
  if (ndims == 0)
    return construct_empty_array(elmtype);

  nelems = ArrayGetNItems(ndims, dims);

  /* compute required space */
  hasnulls = false;
  nbytes = elmlen * nelems;
  if (!AllocSizeIsValid(nbytes))
    eerror("array size exceeds the maximum allowed (%d)",
           (int) MaxAllocSize);
  for (i = 0; i < nelems; i++)
  {
    if (nulls && nulls[i])
    {
      hasnulls = true;
      break;
    }

  }

  /* Allocate and initialize result array */
  if (hasnulls)
  {
    dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nelems);
  }
  else
  {
    dataoffset = 0;                 /* marker for no null bitmap */
  }
  size_t size = sizeof(ArrayType);
  result = (ArrayType *) palloc0(size);
  SET_VARSIZE(result, size);
  size = ndims * sizeof(int);
  result->ndim = ndims;
  result->data = PointerGetDatum(palloc0((size_t)nbytes));
  result->dataoffset = static_cast<Datum>(dataoffset);
  result->elemtype = elmtype;
  result->dims = static_cast<int *>(palloc0(size));
  result->bounds = static_cast<int *>(palloc0(size));
  memcpy(ARR_DIMS(result), dims, size);
  memcpy(ARR_LBOUND(result), lbs, size);

  CopyArrayEls(result,
               elems, nulls, nelems,
               elmlen, elmbyval, elmalign,
               false);

  return result;
}
ArrayType *
construct_array(Datum *elems, int nelems,
                Oid elmtype,
                int elmlen, bool elmbyval, char elmalign)
{
  int			dims[1];
  int			lbs[1];

  dims[0] = nelems;
  lbs[0] = 1;

  return construct_md_array(elems, NULL, 1, dims, lbs,
                            elmtype, elmlen, elmbyval, elmalign);
}

Oid
get_element_type(Oid typid)
{
  // TODO 需要实现获取数组中的元素类型
  return typid;
}

void
deconstruct_array(ArrayType *array,
                  Oid elmtype,
                  int elmlen, bool elmbyval, char elmalign,
                  Datum **elemsp, bool **nullsp, int *nelemsp)
{
  Datum	   *elems;
  bool	   *nulls;
  int			nelems;
  char	   *p;
  bits8	   *bitmap;
  int			bitmask;
  int			i;


  nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
  *elemsp = elems = (Datum *) palloc(nelems * sizeof(Datum));
  if (nullsp)
    *nullsp = nulls = (bool *) palloc0(nelems * sizeof(bool));
  else
    nulls = NULL;
  *nelemsp = nelems;

  p = ARR_DATA_PTR(array);
  bitmap = ARR_NULLBITMAP(array);
  bitmask = 1;

  for (i = 0; i < nelems; i++)
  {
    /* Get source element, checking for NULL */
    if (bitmap && (*bitmap & bitmask) == 0)
    {
      elems[i] = (Datum) 0;
      if (nulls)
        nulls[i] = true;
      else
      eerror("null array element not allowed in this context");
    }
    else
    {
      elems[i] = fetch_att(p, elmbyval, elmlen);
      p = att_addlength_pointer(p, elmlen, p);
      p = (char *) att_align_nominal(p, elmalign);
    }

    /* advance bitmap pointer if any */
    if (bitmap)
    {
      bitmask <<= 1;
      if (bitmask == 0x100)
      {
        bitmap++;
        bitmask = 1;
      }
    }
  }
}

int
ArrayGetNItems(int ndim, const int *dims) {
  int32 ret;
  int i;

#define MaxArraySize ((Size) (MaxAllocSize / sizeof(Datum)))

  if (ndim <= 0)
    return 0;
  ret = 1;
  for (i = 0; i < ndim; i++) {
    int64 prod;

    /* A negative dimension implies that UB-LB overflowed ... */
    if (dims[i] < 0) {
      std::cerr << "array size exceeds the maximum allowed (" << (int) MaxArraySize << ")" << std::endl;
      abort();
    }

    prod = (int64) ret * (int64) dims[i];

    ret = (int32) prod;
    if ((int64) ret != prod) {
      std::cerr << "array size exceeds the maximum allowed (" << (int) MaxArraySize << ")" << std::endl;
      abort();
    }
  }
  if ((Size) ret > MaxArraySize) {
    std::cerr << "array size exceeds the maximum allowed (" << (int) MaxArraySize << ")" << std::endl;
    abort();
  }
  return (int) ret;
}

ArrayType *
construct_empty_array(Oid elmtype)
{
  ArrayType  *result;

  result = (ArrayType *) palloc0(sizeof(ArrayType));
  SET_VARSIZE(result, sizeof(ArrayType));
  result->ndim = 0;
  result->dataoffset = 0;
  result->elemtype = elmtype;
  return result;
}

Datum
General_2Array_to_Element(
  ArrayType *v1,
  ArrayType *v2,
  Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid),
  Datum(*finalize_function)(Datum,int,Oid)) {

  // dimensions
  int ndims1 = ARR_NDIM(v1);
  int ndims2 = ARR_NDIM(v2);
  if (ndims1 != ndims2) {
    eerror("Arrays with %d and %d dimensions are not compatible for this opertation.",
           ndims1, ndims2);
  }
  if (ndims2 == 0) {
    elog(WARNING, "input are empty arrays.");
    return Float8GetDatum(0);
  }
  int *lbs1 = ARR_LBOUND(v1);
  int *lbs2 = ARR_LBOUND(v2);
  int *dims1 = ARR_DIMS(v1);
  int *dims2 = ARR_DIMS(v2);
  int i = 0;
  for (i = 0; i < ndims1; i++) {
    if (dims1[i] != dims2[i] || lbs1[i] != lbs2[i]) {
      eerror("Arrays with range [%d,%d] and [%d,%d] for dimension %d are not compatible for operations.",
             lbs1[i], lbs1[i] + dims1[i], lbs2[i], lbs2[i] + dims2[i], i);
    }
  }
  int nitems = ArrayGetNItems(ndims1, dims1);

  // nulls
  if (ARR_HASNULL(v1) || ARR_HASNULL(v2)) {
    eerror("Arrays with element value NULL are not allowed.");
  }

  // type
  // the function signature guarantees v1 and v2 are of same type
  Oid element_type = ARR_ELEMTYPE(v1);
  int type_size = sizeof(float8);
  bool typbyval = false;

  // iterate
  Datum result = Float8GetDatum(0);
  char *dat1 = ARR_DATA_PTR(v1);
  char *dat2 = ARR_DATA_PTR(v2);
  for (i = 0; i < nitems; ++i) {
    Datum elt1 = fetch_att(dat1, typbyval, type_size);
    dat1 = att_addlength_pointer(dat1, type_size, dat1);
//    dat1 = (char *) att_align_nominal(dat1, typalign);
    Datum elt2 = fetch_att(dat2, typbyval, type_size);
    dat2 = att_addlength_pointer(dat2, type_size, dat2);
//    dat2 = (char *) att_align_nominal(dat2, typalign);

    result = element_function(elt1,
                              element_type,
                              result,
                              FLOAT8OID,
                              elt2,
                              element_type);
  }

  return finalize_function(result, nitems, FLOAT8OID);
}

ArrayType*
General_Array_to_Array(
  ArrayType *v1,
  Datum elt2,
  Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid)) {
  // dimensions
  int ndims1 = ARR_NDIM(v1);
  if (ndims1 == 0) {
    elog(WARNING, "input are empty arrays.");
    return v1;
  }
  int ndims = ndims1;
  int *lbs1 = ARR_LBOUND(v1);
  int *dims1 = ARR_DIMS(v1);
  int *dims = (int *) palloc(ndims * sizeof(int));
  int *lbs = (int *) palloc(ndims * sizeof(int));
  int i = 0;
  for (i = 0; i < ndims; i ++) {
    dims[i] = dims1[i];
    lbs[i] = lbs1[i];
  }
  int nitems = ArrayGetNItems(ndims, dims);

  // type
  Oid element_type = ARR_ELEMTYPE(v1);
  int type_size = sizeof(float8);
  bool typbyval = false;
  char typalign = 'd';

  // allocate
  Datum *result = NULL;
  switch (element_type) {
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
      result = (Datum *)palloc(nitems * sizeof(Datum));break;
    default:
      eerror("Arrays with element type %s are not supported.",
             format_type_be(element_type));
      break;
  }

  // iterate
  Datum *resultp = result;
  char *dat1 = ARR_DATA_PTR(v1);
  for (i = 0; i < nitems; i ++) {
    // iterate elt1
    Datum elt1 = fetch_att(dat1, typbyval, type_size);
    dat1 = att_addlength_pointer(dat1, type_size, dat1);
//    dat1 = (char *) att_align_nominal(dat1, typalign);

    *resultp = element_function(elt1,
                                element_type,
                                elt1,         /* placeholder */
                                element_type, /* placeholder */
                                elt2,
                                element_type);
    resultp ++;
  }

  // construct return result
  ArrayType *pgarray = construct_md_array(result,
                                          NULL,
                                          ndims,
                                          dims,
                                          lbs,
                                          element_type,
                                          type_size,
                                          typbyval,
                                          typalign);

  pfree(result);
  pfree(dims);
  pfree(lbs);

  return pgarray;
}