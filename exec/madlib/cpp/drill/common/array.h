//
// Created by Xiang on 2018/2/12.
//

#ifndef MADLIB_DRILL_ARRAY_H
#define MADLIB_DRILL_ARRAY_H

typedef struct {

  int ndim;      /* # of dimensions */
  Datum dataoffset;    /* offset to data, or 0 if no data */
  Datum data;
  Oid elemtype;    /* element type OID */
  int *dims;
  int *bounds;
} ArrayType;

#define PRINT_NATIVE_ARR(_n, _dims, _bounds, _array) \
  do{ \
    cout<< "array: "; \
    int num = 1; \
    for (int i = 0;i<(_n);++i) { \
      num *= (_dims)[i]; \
    } \
    for (int j = 0;j<num;++j) { \
      cout << (_array)[j] << ", "; \
    } \
    cout<<endl; \
  } while(false)

#define ARR_FROM_PTR(_name, len, oid, _dims, _bounds, _data) \
  ArrayType* _name = (ArrayType *)palloc(sizeof(ArrayType)); \
  (_name)->dataoffset = 0; \
  (_name)->elemtype = oid; \
  (_name)->ndim = len; \
  (_name)->dims = _dims; \
  (_name)->bounds = _bounds; \
  (_name)->data = PointerGetDatum(_data)


#define PFREE_ARR(arr) \
  if (arr) { \
  pfree((arr)->bounds); \
  pfree((arr)->dims); \
  pfree(DatumGetPointer((arr)->dataoffset)); \
  pfree(DatumGetPointer((arr)->data)); \
  pfree((arr)); \
  }

#define PFREE_POINTER_ARR(_pa, _num) \
  do { \
    for (int i = 0; i < _num; ++i) { \
    pfree(_pa[i]); \
    } \
    pfree(_pa); \
  } while(0)

#define INIT_ARR(_name, _n) int _name[_n]

#define INIT_ARR_DIMS(_name, _n) INIT_ARR(dims_ ## _name, _n)

#define INIT_ARR_LBS(_name, _n) INIT_ARR(lbs_ ## _name, _n)

#define ARR_NDIM(a)        ((a)->ndim)

#define ARR_DIMS(a) \
    ((a)->dims)
#define ARR_LBOUND(a) \
    ((a)->bounds)

ArrayType *
construct_empty_array(Oid elmtype);

int
ArrayGetNItems(int ndim, const int *dims);

#define ARR_HASNULL(a)      ((a)->dataoffset != 0)
#define ARR_NULLBITMAP(a) \
    (ARR_HASNULL(a) ? \
     (bits8 *) ((a)->dataoffset) \
     : (bits8 *) NULL)
#define ARR_DATA_PTR(a) \
    (DatumGetPointer((a)->data))

#define ARR_ELEMTYPE(a)      ((a)->elemtype)


#define EXTRACT_DOUBLE_ARRAY(_name, _arr) \
  double *_name = NULL; \
  int32 _name ## Num = extractDoubleArray(_arr, _name)

#define EXTRACT_DOUBLE_ARRAY_H(_name, _arr) \
  double *_name = NULL; \
  int32 _name ## Num = extractDoubleArrayH(_arr, _name)

#define EXTRACT_2D_DOUBLE_ARRAY(_name, _arr) \
  double **_name = NULL; \
  int32* _name ## Num = extract2DDoubleArray(_arr, _name)

#define EXTRACT_2D_DOUBLE_ARRAY_H(_name, _arr) \
  double **_name = NULL; \
  int32* _name ## Num = extract2DDoubleArrayH(_arr, _name); \
  int _name ## EleNum = _name ## Num[0] * _name ##Num[1]

void
deconstruct_array(ArrayType *array,
                  Oid elmtype,
                  int elmlen, bool elmbyval, char elmalign,
                  Datum **elemsp, bool **nullsp, int *nelemsp);

Oid
get_element_type(Oid typid);

#define type_is_array(typid)  (get_element_type(typid) != InvalidOid)

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
                char *dest);

void
CopyArrayEls(ArrayType *array,
             Datum *values,
             bool *nulls,
             int nitems,
             int typlen,
             bool typbyval,
             char typalign,
             bool freedata);

ArrayType *
construct_md_array(Datum *elems,
                   bool *nulls,
                   int ndims,
                   int *dims,
                   int *lbs,
                   Oid elmtype, int elmlen, bool elmbyval, char elmalign);

ArrayType *
construct_array(Datum *elems, int nelems,
                Oid elmtype,
                int elmlen, bool elmbyval, char elmalign);


/*
 * array_ops
 */
static inline float8 float8_dot(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_div(float8 op1, float8 op2, float8 opt_op);
static inline float8 float8_mult(float8 op1, float8 op2, float8 opt_op);
static inline int64 int64_div(int64 num, int64 denom);
/*
 * Add elt1*elt2 to result.
 */
static inline Datum element_dot(Datum element, Oid elt_type, Datum result,
            Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_mult(Datum element, Oid elt_type, Datum result,
             Oid result_type, Datum opt_elt, Oid opt_type);
static inline Datum element_op(Datum element, Oid elt_type, Datum result,
           Oid result_type, Datum opt_elt, Oid opt_type,
           float8 (*op)(float8, float8, float8));

ArrayType *General_Array_to_Array(ArrayType *v1, Datum value,
                                         Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid));
Datum General_2Array_to_Element(ArrayType *v1, ArrayType *v2,
                                       Datum(*element_function)(Datum,Oid,Datum,Oid,Datum,Oid),
                                       Datum(*finalize_function)(Datum,int,Oid));

/*
 * array_ops
 */

// ------------------------------------------------------------------------
// finalize functions
// ------------------------------------------------------------------------
static inline Datum noop_finalize(Datum elt,int size,Oid element_type);

static inline Datum average_finalize(Datum elt,int size,Oid element_type);

static inline Datum average_root_finalize(Datum elt,int size,Oid element_type);

#endif //MADLIB_DRILL_ARRAY_H
