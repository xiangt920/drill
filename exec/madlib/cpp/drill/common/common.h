//
// Created by Xiang on 2018/2/12.
//

#ifndef MADLIB_DRILL_COMMON_H
#define MADLIB_DRILL_COMMON_H

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <cmath>

#define ERROR 20
#define FLEXIBLE_ARRAY_MEMBER
typedef char *Pointer;
#define INT2OID 1
#define INT4OID 2
#define INT8OID 3
#define FLOAT4OID 4
#define FLOAT8OID 5
#define BOOLOID			16
#define REGPROCOID		24
#define TEXTOID			25

#define MAXDIM 6
#define NAMEDATALEN 64

#define MCXT_ALLOC_NO_OOM		0x02
#define MCXT_ALLOC_ZERO			0x04


#define MaxAllocSize	((Size) 0x3fffffff)		/* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)
typedef uintptr_t Datum;
typedef unsigned int Oid;

#ifndef USE_ASSERT_CHECKING

#define Assert(condition)	((void)true)
#define AssertMacro(condition)	((void)true)
#define AssertArg(condition)	((void)true)
#define AssertState(condition)	((void)true)
#define AssertPointerAlignment(ptr, bndr)	((void)true)
#define Trap(condition, errorType)	((void)true)
#define TrapMacro(condition, errorType) (true)

#endif

#ifdef __cplusplus
#define InvalidOid		(Oid(0))
#else
#define InvalidOid		((Oid) 0)
#endif
#define HAVE_LONG_INT_64 1
/*
 * intN
 *		Signed integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_UINT8
typedef unsigned char uint8;	/* == 8 bits */
typedef unsigned short uint16;	/* == 16 bits */
typedef unsigned int uint32;	/* == 32 bits */
#endif   /* not HAVE_UINT8 */
typedef uint8 bits8;			/* >= 8 bits */
typedef uint16 bits16;			/* >= 16 bits */
typedef uint32 bits32;			/* >= 32 bits */
#ifndef HAVE_INT8
typedef signed char int8;		/* == 8 bits */
typedef signed short int16;		/* == 16 bits */
typedef signed int int32;		/* == 32 bits */
#endif   /* not HAVE_INT8 */
/*
 * uintN
 *		Unsigned integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_UINT8
typedef unsigned char uint8;	/* == 8 bits */
typedef unsigned short uint16;	/* == 16 bits */
typedef unsigned int uint32;	/* == 32 bits */
#endif   /* not HAVE_UINT8 */

#if !defined(_POSIX_C_SOURCE) || defined(_DARWIN_C_SOURCE)
typedef	unsigned short		ushort;
typedef	unsigned int		uint;
#endif

typedef float float4;
typedef double float8;
typedef unsigned int Index;
#define VARHDRSZ		((int32) sizeof(int32))
/*
 * 64-bit integers
 */
#ifdef HAVE_LONG_INT_64
/* Plain "long int" fits, use it */

#ifndef HAVE_INT64
typedef long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long int uint64;
#endif
#define INT64CONST(x)  (x##L)
#define UINT64CONST(x) (x##UL)
#elif defined(HAVE_LONG_LONG_INT_64)
/* We have working support for "long long int", use that */

#ifndef HAVE_INT64
typedef long long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long long int uint64;
#endif
#define INT64CONST(x)  (x##LL)
#define UINT64CONST(x) (x##ULL)
#else
/* neither HAVE_LONG_INT_64 nor HAVE_LONG_LONG_INT_64 */
#error must have a working 64-bit integer datatype
#endif

#define unreachable(msg) throw std::runtime_error(msg)

#define NOTICE 1
#define eerror(format, ...) \
  do { \
		char msg[1024]; \
    sprintf(msg, format, ##__VA_ARGS__); \
    unreachable(msg); \
  } while(0)
  

#define elog(level, format, ...) \
  fprintf(stderr, format, ##__VA_ARGS__);
typedef size_t Size;

#define CHECKFLOATVAL(val, inf_is_valid, zero_is_valid)			\
do {															\
	if (isinf(val) && !(inf_is_valid))							\
		eerror("value out of range: overflow");				\
																\
	if ((val) == 0.0 && !(zero_is_valid))						\
		eerror("value out of range: underflow");				\
} while(0)

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union
{
  struct						/* Normal varlena (4-byte length) */
  {
    uint32		va_header;
    char		va_data[FLEXIBLE_ARRAY_MEMBER];
  }			va_4byte;
  struct						/* Compressed-in-line format */
  {
    uint32		va_header;
    uint32		va_rawsize; /* Original data size (excludes header) */
    char		va_data[FLEXIBLE_ARRAY_MEMBER];		/* Compressed data */
  }			va_compressed;
} varattrib_4b;

struct varlena
{
  char		vl_len_[4];		/* Do not touch this field directly! */
  char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE(ptr) - VARHDRSZ.
 */
typedef struct varlena bytea;
typedef struct varlena text;
typedef struct varlena BpChar;	/* blank-padded char, ie SQL char(n) */
typedef struct varlena VarChar; /* var-length char, ie SQL varchar(n) */

#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetPointer(X) ((Pointer) (X))

/* Define as the maximum alignment requirement of any C data type. */
#define MAXIMUM_ALIGNOF 8

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/*
 * The total array header size (in bytes) for an array with the specified
 * number of dimensions and total number of items.
 */
#define ARR_OVERHEAD_NONULLS(ndims) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + \
				 ((nitems) + 7) / 8)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)

/* Get a bit mask of the bits set in non-long aligned addresses */
#define LONG_ALIGN_MASK (sizeof(long) - 1)
/* Define bytes to use libc memset(). */
#define MEMSET_LOOP_LIMIT 1024

typedef enum NodeTag {
  T_Simple,
  T_Agg
} NodeTag;

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.  Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData *MemoryContext;

/*
 * A memory context can have callback functions registered on it.  Any such
 * function will be called once just before the context is next reset or
 * deleted.  The MemoryContextCallback struct describing such a callback
 * typically would be allocated within the context itself, thereby avoiding
 * any need to manage it explicitly (the reset/delete action will free it).
 */
typedef void (*MemoryContextCallbackFunction) (void *arg);

typedef struct MemoryContextCallback
{
  MemoryContextCallbackFunction func; /* function to call */
  void	   *arg;			/* argument to pass it */
  struct MemoryContextCallback *next; /* next in list of callbacks */
} MemoryContextCallback;

typedef struct MemoryContextCounters
{
  Size		nblocks;		/* Total number of malloc blocks */
  Size		freechunks;		/* Total number of free chunks */
  Size		totalspace;		/* Total bytes requested from malloc */
  Size		freespace;		/* The unused portion of totalspace */
} MemoryContextCounters;

typedef struct MemoryContextMethods
{
  void	   *(*alloc) (MemoryContext context, Size size);
  /* call this free_p in case someone #define's free() */
  void		(*free_p) (MemoryContext context, void *pointer);
  void	   *(*realloc) (MemoryContext context, void *pointer, Size size);
  void		(*init) (MemoryContext context);
  void		(*reset) (MemoryContext context);
  void		(*delete_context) (MemoryContext context);
  Size		(*get_chunk_space) (MemoryContext context, void *pointer);
  bool		(*is_empty) (MemoryContext context);
  void		(*stats) (MemoryContext context, int level, bool print,
                    MemoryContextCounters *totals);
#ifdef MEMORY_CONTEXT_CHECKING
  void		(*check) (MemoryContext context);
#endif
} MemoryContextMethods;


typedef struct MemoryContextData
{
  NodeTag		type;			/* identifies exact kind of context */
  /* these two fields are placed here to minimize alignment wastage: */
  bool		isReset;		/* T = no space alloced since last reset */
  bool		allowInCritSection;		/* allow palloc in critical section */
  MemoryContextMethods *methods;		/* virtual function table */
  MemoryContext parent;		/* NULL if no parent (toplevel context) */
  MemoryContext firstchild;	/* head of linked list of children */
  MemoryContext prevchild;	/* previous child of same parent */
  MemoryContext nextchild;	/* next child of same parent */
  char	   *name;			/* context name (just for debugging) */
  MemoryContextCallback *reset_cbs;	/* list of reset/delete callbacks */
} MemoryContextData;

/*
 * MemSet
 *	Exactly the same as standard library function memset(), but considerably
 *	faster for zeroing small word-aligned structures (such as parsetree nodes).
 *	This has to be a macro because the main point is to avoid function-call
 *	overhead.   However, we have also found that the loop is faster than
 *	native libc memset() on some platforms, even those with assembler
 *	memset() functions.  More research needs to be done, perhaps with
 *	MEMSET_LOOP_LIMIT tests in configure.
 */
#define MemSet(start, val, len) \
	do \
	{ \
		/* must be void* because we don't know if it is integer aligned yet */ \
		void   *_vstart = (void *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
			(_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			/* \
			 *	If MEMSET_LOOP_LIMIT == 0, optimizer should find \
			 *	the whole "if" false at compile time. \
			 */ \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_start = (long *) _vstart; \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_vstart, _val, _len); \
	} while (0)

#define MemSetAligned(start, val, len) \
	do \
	{ \
		long   *_start = (long *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_start, _val, _len); \
	} while (0)

static inline void *
malloc_internal(size_t size, int flags);

void *
palloc0(size_t size);

void *
palloc(size_t size);

void
pfree(void *ptr);

void *
MemoryContextAllocZero(MemoryContext context, Size size);

#define SIZEOF_VOID_P 8
#define SIZEOF_DATUM SIZEOF_VOID_P
#define GET_1_BYTE(datum)	(((Datum) (datum)) & 0x000000ff)
#define GET_2_BYTES(datum)	(((Datum) (datum)) & 0x0000ffff)
#define GET_4_BYTES(datum)	(((Datum) (datum)) & 0xffffffff)
#if SIZEOF_DATUM == 8
#define GET_8_BYTES(datum)	((Datum) (datum))
#endif
#define SET_1_BYTE(value)	(((Datum) (value)) & 0x000000ff)
#define SET_2_BYTES(value)	(((Datum) (value)) & 0x0000ffff)
#define SET_4_BYTES(value)	(((Datum) (value)) & 0xffffffff)
#if SIZEOF_DATUM == 8
#define SET_8_BYTES(value)	((Datum) (value))
#endif
#define DatumGetBool(X) ((bool) (GET_1_BYTE(X) != 0))
#define DatumGetChar(X) ((char) GET_1_BYTE(X))
#define DatumGetInt16(X) ((int16) GET_2_BYTES(X))
#define DatumGetInt32(X) ((int32) GET_4_BYTES(X))
#define DatumGetInt64(X) ((int64) GET_8_BYTES(X))
#define DatumGetObjectId(X) ((Oid) GET_4_BYTES(X))

#define ObjectIdGetDatum(X) ((Datum) SET_4_BYTES(X))
#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))
#define CharGetDatum(X) ((Datum) SET_1_BYTE(X))
#define Int16GetDatum(X) ((Datum) SET_2_BYTES(X))
#define Int32GetDatum(X) ((Datum) SET_4_BYTES(X))
#define Int64GetDatum(X) ((Datum) SET_8_BYTES(X))
//#define USE_FLOAT4_BYVAL 1
//#define USE_FLOAT8_BYVAL 1
Datum
Float4GetDatum(float4 X);

#ifdef USE_FLOAT4_BYVAL

float4
DatumGetFloat4(Datum X);
#else
#define DatumGetFloat4(X) (* ((float4 *) DatumGetPointer(X)))
#endif

Datum
Float8GetDatum(float8 X);

#ifdef USE_FLOAT8_BYVAL

float8
DatumGetFloat8(Datum X);
#else
#define DatumGetFloat8(X) (* ((float8 *) DatumGetPointer(X)))
#endif   /* USE_FLOAT8_BYVAL */
/* The normal alignment of `double', in bytes. */
#define ALIGNOF_DOUBLE 8

/* The normal alignment of `int', in bytes. */
#define ALIGNOF_INT 4

/* The normal alignment of `long', in bytes. */
#define ALIGNOF_LONG 8
#define ALIGNOF_SHORT 2
#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))

typedef struct
{
  uint8		va_header;
  char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
  uint8		va_header;		/* Always 0x80 or 0x01 */
  uint8		va_tag;			/* Type of datum */
  char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;
typedef enum vartag_external
{
  VARTAG_INDIRECT = 1,
  VARTAG_EXPANDED_RO = 2,
  VARTAG_EXPANDED_RW = 3,
  VARTAG_ONDISK = 18
} vartag_external;
typedef struct varatt_indirect
{
  struct varlena *pointer;	/* Pointer to in-memory varlena */
}	varatt_indirect;
typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	int32		va_extsize;		/* External saved size (doesn't) */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}	varatt_external;

#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE(PTR)						VARSIZE_4B(PTR)
#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)
#define VARTAG_IS_EXPANDED(tag) \
	(((tag) & ~1) == VARTAG_EXPANDED_RO)
#define VARHDRSZ_SHORT			offsetof(varattrib_1b, va_data)
#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 (true))
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)
#define VARTAG_EXTERNAL(PTR)				VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR)				(VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))
#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == 'i') ? INTALIGN(cur_offset) : \
	 (((attalign) == 'c') ? (uintptr_t) (cur_offset) : \
	  (((attalign) == 'd') ? DOUBLEALIGN(cur_offset) : \
	   ( \
      ((void) true), \
			SHORTALIGN(cur_offset) \
	   ))) \
)
#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: \
	( \
    ((void)true), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	) \
)

#define att_addlength_datum(cur_offset, attlen, attdatum) \
	att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum))

#define store_att_byval(T,newdatum,attlen) \
	do { \
		switch (attlen) \
		{ \
			case sizeof(char): \
				*(char *) (T) = DatumGetChar(newdatum); \
				break; \
			case sizeof(int16): \
				*(int16 *) (T) = DatumGetInt16(newdatum); \
				break; \
			case sizeof(int32): \
				*(int32 *) (T) = DatumGetInt32(newdatum); \
				break; \
			case sizeof(Datum): \
				*(Datum *) (T) = (newdatum); \
				break; \
			default: \
				elog(0, "unsupported byval length: %d", \
					 (int) (attlen)); \
				break; \
		} \
	} while (0)

#define fetch_att(T,attbyval,attlen) \
( \
	(attbyval) ? \
	( \
		(attlen) == (int) sizeof(Datum) ? \
			*((Datum *)(T)) \
		: \
	  ( \
		(attlen) == (int) sizeof(int32) ? \
			Int32GetDatum(*((int32 *)(T))) \
		: \
		( \
			(attlen) == (int) sizeof(int16) ? \
				Int16GetDatum(*((int16 *)(T))) \
			: \
			( \
				((void) true), \
				CharGetDatum(*((char *)(T))) \
			) \
		) \
	  ) \
	) \
	: \
	PointerGetDatum((char *) (T)) \
)

#define VARATT_IS_EXTENDED(PTR)				(!VARATT_IS_4B_U(PTR))
struct varlena *
pg_detoast_datum(struct varlena * datum);

typedef struct {
	int32 vl_len_;   /**< This is unused at the moment */
	int32 dimension; /**< Number of elements in this vector, special case is -1 indicates a scalar */
	char data[1];   /**< The serialized SparseData representing the vector here */
} SvecType;

#define MAX_RANDOM_VALUE (0x7FFFFFFF)

Datum setseed(Datum inSeed);

Datum drandom();
#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA(PTR)						VARDATA_4B(PTR)
//	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL :
#define VARSIZE_ANY_EXHDR(PTR) \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ)

#define DRILL_TRY \
  try {

#define DRILL_CATCH_BEGIN \
  } catch (std::exception &ex) { \
    jclass _exp_class = env->FindClass("org/apache/drill/madlib/jni/DataProcessError"); \
    env->ThrowNew(_exp_class, ex.what());

#define DRILL_CATCH_END_NULL \
    return NULL; \
  }

#define DRILL_CATCH_END_0 \
    return 0; \
  }

#define DEFINE_MCV(_type, _name) \
  TransparentHandle<_type> han_ ## _name = TransparentHandle<_type>(in_ ## _name); \
  MappedColumnVector vec_ ## _name; \
  vec_ ## _name.rebind(han_ ## _name, num_ ## _name)

#define DEFINE_MBS(_name) \
  bytea *_name = reinterpret_cast<bytea *>(in_ ## _name); \
  MutableByteString _name ## _str = MutableByteString(_name)

#define DRILL_BYTE_ARR_RESULT(_result_name, _out_name, env) \
  jbyteArray _out_name = NULL; \
  do { \
    ByteString bs_ ## _result_name = _result_name.getAs<ByteString>(); \
    const bytea *ptr = bs_ ## _result_name.byteString(); \
    jsize _out_name ## _len = static_cast<jsize>(bs_ ## _result_name.size() + 4 + bs_ ## _result_name.kEffectiveHeaderSize); \
    jbyte *_out_name ## _array = static_cast<jbyte *>(palloc(sizeof(jbyte) * _out_name ## _len)); \
    memcpy(_out_name ## _array, ptr,static_cast<size_t>(_out_name ## _len)); \
    _out_name = env->NewByteArray(_out_name ## _len); \
    env->SetByteArrayRegion(_out_name,0, _out_name ## _len, _out_name ## _array); \
    JNI_RELEASE_ARR_ELEMENTS(_out_name ## _array, Byte, env, _out_name,0); \
  } while(false)

/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length;
 * the string need not be null_terminated.
 */
text *
cstring_to_text_with_len(const char *s, int len);
text *
cstring_to_text(const char *s);

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
text_to_cstring(const text *t);

void
get_typlenbyvalalign(Oid typid, int16 *typlen, bool *typbyval,
                     char *typalign);
Datum
OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2);
#define OidFunctionCall2(functionId, arg1, arg2) \
	OidFunctionCall2Coll(functionId, InvalidOid, arg1, arg2)

Datum dtoi8(Datum res);
Datum dtof(Datum res);
Datum dtoi4(Datum res);
Datum dtoi2(Datum res);
Datum i8tod(Datum res);
Datum ftod(Datum res);
Datum i4tod(Datum res);
Datum i2tod(Datum res);
Datum ftoi8(Datum res);
Datum ftoi4(Datum res);
Datum ftoi2(Datum res);
Datum i8tof(Datum res);
Datum i4tof(Datum res);
Datum i2tof(Datum res);

float8 datum_float8_cast(Datum elt, Oid element_type);
int64 datum_int64_cast(Datum elt, Oid element_type);
Datum int64_datum_cast(int64 res, Oid result_type);
Datum float8_datum_cast(float8 res, Oid result_type);
char* format_type_be(Oid type);

#endif //MADLIB_DRILL_COMMON_H
