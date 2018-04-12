/* ----------------------------------------------------------------------- *//**
 *
 * @file Backend.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_BACKEND_HPP
#define MADLIB_POSTGRES_BACKEND_HPP

namespace madlib {

namespace dbconnector {

namespace postgres {

namespace {
// No need to make these function accessible outside of the postgres namespace.
MADLIB_WRAP_PG_FUNC(
    bool, type_is_array, (Oid typid), (typid))

MADLIB_WRAP_PG_FUNC(
    struct varlena*, pg_detoast_datum, (struct varlena* datum), (datum))

MADLIB_WRAP_VOID_PG_FUNC(
  get_typlenbyvalalign,
  (Oid typid, int16 *typlen, bool *typbyval, char *typalign),
  (typid, typlen, typbyval, typalign)
)
template <typename T>
inline
T*
madlib_detoast_verlena_datum_if_necessary(Datum inDatum) {
    varlena* ptr = reinterpret_cast<varlena*>(DatumGetPointer(inDatum));

    if (!VARATT_IS_EXTENDED(ptr))
        return reinterpret_cast<T*>(ptr);
    else
        return reinterpret_cast<T*>(madlib_pg_detoast_datum(ptr));
}

/**
 * @brief Convert a Datum into a bytea
 *
 * For performance reasons, we look into the varlena struct in order to check
 * if we can avoid a PG_TRY block.
 */
inline
bytea*
madlib_DatumGetByteaP(Datum inDatum) {
    return madlib_detoast_verlena_datum_if_necessary<bytea>(inDatum);
}

/**
 * @brief Convert a Datum into an ArrayType
 *
 * For performance reasons, we look into the varlena struct in order to check
 * if we can avoid a PG_TRY block.
 */
inline
ArrayType*
madlib_DatumGetArrayTypeP(Datum inDatum) {
    ArrayType* x = madlib_detoast_verlena_datum_if_necessary<ArrayType>(inDatum);
    if (ARR_HASNULL(x)) {
        // an empty array has dimensionality 0
        size_t array_size = ARR_NDIM(x) ? 1 : 0;
        for (int i = 0; i < ARR_NDIM(x); i ++) {
            array_size *= ARR_DIMS(x)[i];
        }

        throw ArrayWithNullException(array_size);
    }

    return x;
}

} // namespace

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_BACKEND_ABSTRACTION_HPP)
