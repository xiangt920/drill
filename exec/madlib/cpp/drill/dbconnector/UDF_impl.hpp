/* ----------------------------------------------------------------------- *//**
 *
 * @file UDF_impl.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_UDF_IMPL_HPP
#define MADLIB_POSTGRES_UDF_IMPL_HPP

namespace madlib {

namespace dbconnector {

namespace postgres {

#define MADLIB_HANDLE_STANDARD_EXCEPTION(err) \
    sqlerrcode = err; \
    strncpy(msg, exc.what(), sizeof(msg));

// FIXME add some information to explain why we do not need a PG_TRY
// for SRF_FIRSTCALL_INIT(), SRF_RETURN_NEXT() and SRF_RETURN_DONE()
#define MADLIB_SRF_IS_FIRSTCALL() SRF_is_firstcall<Function>(fcinfo)
#define MADLIB_SRF_FIRSTCALL_INIT() SRF_FIRSTCALL_INIT()
#define MADLIB_SRF_PERCALL_SETUP() SRF_percall_setup<Function>(fcinfo)
#define MADLIB_SRF_RETURN_NEXT(_funcctx, _result) SRF_RETURN_NEXT(_funcctx, _result)
#define MADLIB_SRF_RETURN_DONE(_funcctx) SRF_RETURN_DONE(_funcctx)

/**
 * @brief A wrapper for SRF_IS_FIRSTCALL.
 *
 * We wrap SRF_IS_FIRSTCALL inside the PG_TRY block to handle the errors
 *
 * @param fcinfo The PostgreSQL FunctionCallInfoData structure
 */
template <class Function>
inline bool
UDF::SRF_is_firstcall(){


    return true;
}


/**
 * @brief A wrapper for SRF_PERCALL_SETUP.
 *
 * We wrap SRF_PERCALL_SETUP inside the PG_TRY block to handle the errors
 *
 * @param fcinfo The PostgreSQL FunctionCallInfoData structure
 */
template <class Function>
inline void
UDF::SRF_percall_setup(){

    return;
}


/**
 * @brief Internal interface for calling a UDF
 *
 * We need the FunctionCallInfo in case some arguments or return values are
 * of polymorphic types.
 *
 * @param args Arguments to the function. While for calls from the backend
 *     all arguments are specified by \c fcinfo, for calls "within" the C++ AL
 *     it is more efficient to pass arguments as "native" C++ object references.
 */
template <class Function>
inline
AnyType
UDF::invoke(AnyType& args) {
    return Function().run(args);
}


/**
 * @brief Internal interface for calling a set return UDF
 *
 * We need the FunctionCallInfo in case some arguments or return values are
 * of polymorphic types.
 *
 * @param fcinfo The PostgreSQL FunctionCallInfoData structure
 *
 */
template <class Function>
inline
Datum
UDF::SRF_invoke() {
    return 0;
}

/**
 * @brief Each exported C function calls this method (and nothing else)
 */
template <class Function>
inline
Datum
UDF::call() {
    return 0;
}

#undef MADLIB_HANDLE_STANDARD_EXCEPTION

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_UDF_IMPL_HPP)
