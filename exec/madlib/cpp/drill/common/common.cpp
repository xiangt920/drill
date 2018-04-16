//
// Created by Xiang on 2018/2/27.
//


#include <stdexcept>
#include "common.h"
using namespace std;
static inline void *
malloc_internal(size_t size, int flags)
{
  void       *tmp;

  /* Avoid unportable behavior of malloc(0) */
  if (size == 0)
    size = 1;
  tmp = malloc(size);
  if (tmp == NULL)
  {
    if ((flags & MCXT_ALLOC_NO_OOM) == 0)
    {
      eerror("out of memory");
    }
    return NULL;
  }

  if ((flags & MCXT_ALLOC_ZERO) != 0)
    MemSet(tmp, 0, size);
  return tmp;
}

void *
palloc0(size_t size)
{
  return malloc_internal(size, MCXT_ALLOC_ZERO);
}

void *
palloc(size_t size)
{
  return malloc_internal(size, 0);
}

void
pfree(void *ptr)
{
  if (ptr != NULL)
    free(ptr);
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
  void	   *ret;

  if (!AllocSizeIsValid(size))
    elog(ERROR, "invalid memory alloc request size %zu", size);

  context->isReset = false;

  ret = (*context->methods->alloc) (context, size);
  if (ret == NULL)
  {
    eerror("Failed on request of size %zu.", size);
  }

  MemSetAligned(ret, 0, size);

  return ret;
}

/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length;
 * the string need not be null_terminated.
 */
text *
cstring_to_text_with_len(const char *s, int len)
{
  text	   *result = (text *) malloc(static_cast<size_t>(len + VARHDRSZ));

  SET_VARSIZE(result, len + VARHDRSZ);
  memcpy(VARDATA(result), s, static_cast<size_t>(len));

  return result;
}
text *
cstring_to_text(const char *s)
{
  return cstring_to_text_with_len(s, static_cast<int>(strlen(s)));
}

/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
char *
text_to_cstring(const text *t)
{
  /* must cast away the const, unfortunately */
  text *tunpacked = (text *) t;
  int			len = (int) VARSIZE_ANY_EXHDR(tunpacked);
  char	   *result;

  result = (char *) malloc((size_t)len + 1);
  memcpy(result, VARDATA_ANY(tunpacked), (size_t)len);
  result[len] = '\0';


  return result;
}

void
get_typlenbyvalalign(Oid typid, int16 *typlen, bool *typbyval,
                     char *typalign)
{
  switch (typid) {
    case INT2OID:
      *typbyval = true;
      *typlen = 2;
      *typalign = 's';
      break;
    case INT4OID:
      *typbyval = true;
      *typlen = 4;
      *typalign = 'i';
      break;
    case INT8OID:
      *typbyval = false;
      *typlen = 8;
      *typalign = 'd';
      break;
    case FLOAT4OID:
      *typbyval = false;
      *typlen = 4;
      *typalign = 'i';
      break;
    case FLOAT8OID:
      *typbyval = false;
      *typlen = 8;
      *typalign = 'd';
      break;
    default:
      eerror("unsupported type id: %d", typid);
      break;
  }
}
Datum
OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2)
{
  Datum		result = 0;
  return result;
}

Datum setseed(Datum inSeed)
{
  float8 seed = DatumGetFloat8(inSeed);
  int iseed;
  if (seed < -1 || seed > 1)
  {
    eerror("setseed parameter %f out of range [-1,1]", seed);

  }
  iseed = (int) (seed * MAX_RANDOM_VALUE);
  srandom((unsigned int) iseed);
  return (Datum) 0;
}

Datum drandom()
{
  float8 result;
  result = (double) random() / ((double) MAX_RANDOM_VALUE + 1);
  return Float8GetDatum(result);
}

Datum
Float4GetDatum(float4 X)
{
#ifdef USE_FLOAT4_BYVAL
  union
  {
    float4          value;
    int32           retval;
  }                       myunion;

  myunion.value = X;
  return SET_4_BYTES(myunion.retval);
#else
  float4     *retval = (float4 *) malloc(sizeof(float4));

  *retval = X;
  return PointerGetDatum(retval);
#endif
}

#ifdef USE_FLOAT4_BYVAL

float4
DatumGetFloat4(Datum X)
{
  union
  {
    int32           value;
    float4          retval;
  }                       myunion;

  myunion.value = GET_4_BYTES(X);
  return myunion.retval;
}
#endif

Datum
Float8GetDatum(float8 X)
{
#ifdef USE_FLOAT8_BYVAL
  union
  {
    float8          value;
    int64           retval;
  }                       myunion;

  myunion.value = X;
  return SET_8_BYTES(myunion.retval);
#else
  float8     *retval = (float8 *) malloc(sizeof(float8));

  *retval = X;
  return PointerGetDatum(retval);
#endif
}

#ifdef USE_FLOAT8_BYVAL
float8
DatumGetFloat8(Datum X)
{
  union
  {
    int64           value;
    float8          retval;
  }                       myunion;

  myunion.value = GET_8_BYTES(X);
  return myunion.retval;
}
#endif   /* USE_FLOAT8_BYVAL */

struct varlena *
pg_detoast_datum(struct varlena * datum)
{

  return datum;
}

/* -----------------------------------------------------------------------
 * Type cast functions
 * -----------------------------------------------------------------------
*/

Datum
dtoi8(Datum res)
{
  float8		arg = DatumGetFloat8(res);
  int64		result;

  /* Round arg to nearest integer (but it's still in float form) */
  arg = rint(arg);

  /*
   * Does it fit in an int64?  Avoid assuming that we have handy constants
   * defined for the range boundaries, instead test for overflow by
   * reverse-conversion.
   */
  result = (int64) arg;

  if ((float8) result != arg)
  eerror("bigint out of range");

  return Int64GetDatum(result);
}

Datum
dtof(Datum res)
{
  float8		num = DatumGetFloat8(res);
  CHECKFLOATVAL((float4) num, isinf(num), num == 0);

  return Float4GetDatum((float4) num);
}

Datum
dtoi4(Datum res)
{
  float8		num = DatumGetFloat8(res);
  int32		result;

  /* 'Inf' is handled by INT_MAX */
  if (num < INT_MIN || num > INT_MAX || isnan(num))
  eerror("integer out of range");

  result = (int32) rint(num);
  return Int32GetDatum(result);
}

Datum
dtoi2(Datum res)
{
  float8		num = DatumGetFloat8(res);

  if (num < SHRT_MIN || num > SHRT_MAX || isnan(num))
  eerror("smallint out of range");

  return Int16GetDatum((int16) rint(num));
}

Datum
i8tod(Datum res)
{
  int64		arg = DatumGetInt64(res);
  float8		result;

  result = arg;

  return Float8GetDatum(result);
}

Datum
ftod(Datum res)
{
  float4		num = DatumGetFloat4(res);

  return Float8GetDatum((float8) num);
}

Datum
i4tod(Datum res)
{
  int32		num = DatumGetInt32(res);

  return Float8GetDatum((float8) num);
}

Datum
i2tod(Datum res)
{
  int16		num = DatumGetInt16(res);

  return Float8GetDatum((float8) num);
}

Datum
ftoi8(Datum res)
{
  float4		arg = DatumGetFloat4(res);
  int64		result;
  float8		darg;

  /* Round arg to nearest integer (but it's still in float form) */
  darg = rint(arg);

  /*
   * Does it fit in an int64?  Avoid assuming that we have handy constants
   * defined for the range boundaries, instead test for overflow by
   * reverse-conversion.
   */
  result = (int64) darg;

  if ((float8) result != darg)
  eerror("bigint out of range");

  return Int64GetDatum(result);
}

/*
 *		ftoi4			- converts a float4 number to an int4 number
 */
Datum
ftoi4(Datum res)
{
  float4		num = DatumGetFloat4(res);

  if (num < INT_MIN || num > INT_MAX || isnan(num))
  eerror("integer out of range");

  return Int32GetDatum((int32) rint(num));
}


/*
 *		ftoi2			- converts a float4 number to an int2 number
 */
Datum
ftoi2(Datum res)
{
  float4		num = DatumGetFloat4(res);

  if (num < SHRT_MIN || num > SHRT_MAX || isnan(num))
  eerror("smallint out of range");

  return Int16GetDatum((int16) rint(num));
}

Datum
i8tof(Datum res)
{
  int64		arg = DatumGetInt64(res);
  float4		result;

  result = arg;

  return Float4GetDatum(result);
}


/*
 *		i4tof			- converts an int4 number to a float4 number
 */
Datum
i4tof(Datum res)
{
  int32		num = DatumGetInt32(res);

  return Float4GetDatum((float4) num);
}


/*
 *		i2tof			- converts an int2 number to a float4 number
 */
Datum
i2tof(Datum res)
{
  int16		num = DatumGetInt16(res);

  return Float4GetDatum((float4) num);
}

float8
datum_float8_cast(Datum elt, Oid element_type) {
  switch(element_type){
    case INT2OID:
      return (float8) DatumGetInt16(elt); break;
    case INT4OID:
      return (float8) DatumGetInt32(elt); break;
    case INT8OID:
      return (float8) DatumGetInt64(elt); break;
    case FLOAT4OID:
      return (float8) DatumGetFloat4(elt); break;
    case FLOAT8OID:
      return (float8) DatumGetFloat8(elt); break;
    default:
    eerror("Arrays with element type %s are not supported.",
            format_type_be(element_type));
      break;
  }
  return 0.0;
}

int64
datum_int64_cast(Datum elt, Oid element_type) {
  switch(element_type){
    case INT2OID:
      return (int64) DatumGetInt16(elt); break;
    case INT4OID:
      return (int64) DatumGetInt32(elt); break;
    case INT8OID:
      return elt; break;
    default:
    eerror("Arrays with element type %s are not supported.",
            format_type_be(element_type));
      break;
  }
  return 0;
}

Datum
int64_datum_cast(int64 res, Oid result_type) {
  switch(result_type){
    case INT2OID: {
      int16 result16 = (int16) res;
      if ((int64) result16 != res) {
        eerror("smallint out of range");
      }
      return Int16GetDatum(result16);
      break;
    }
    case INT4OID: {
      int32 result32 = (int32) res;
      if ((int64) result32 != res) {
        eerror("smallint out of range");
      }
      return Int32GetDatum(result32);
      break;
    }
    case INT8OID: {
      return Int64GetDatum(res);
      break;
    }
    default: {
      eerror("Arrays with element type %s are not supported.",
              format_type_be(result_type));
      break;
    }
  }
  return Int64GetDatum(res);
}

Datum
float8_datum_cast(float8 res, Oid result_type) {
  Datum result = Float8GetDatum(res);
  switch(result_type){
    case INT2OID:
      return dtoi2(result); break;
    case INT4OID:
      return dtoi4(result); break;
    case INT8OID:
      return dtoi8(result); break;
    case FLOAT4OID:
      return dtof(result); break;
    case FLOAT8OID:
      return result; break;
    default:
    eerror("Arrays with element type %s are not supported.",
            format_type_be(result_type));
      break;
  }
  return result;
}

char* format_type_be(Oid type) {
  switch(type){
    case INT2OID:
      return const_cast<char *>("short");
      break;
    case INT4OID:
      return const_cast<char *>("int");
      break;
    case INT8OID:
      return const_cast<char *>("long");
      break;
    case FLOAT4OID:
      return const_cast<char *>("float");
      break;
    case FLOAT8OID:
      return const_cast<char *>("double");
      break;
    case TEXTOID:
      return const_cast<char *>("string");
      break;
    default:
      return const_cast<char *>("unknown");
      break;

  }
}

/* -----------------------------------------------------------------------
 * Type cast functions
 * -----------------------------------------------------------------------
*/

