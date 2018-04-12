/* ----------------------------------------------------------------------- *//**
 *
 * @file SystemInformation_impl.hpp
 *
 *//* ----------------------------------------------------------------------- */


#ifndef MADLIB_POSTGRES_SYSTEMINFORMATION_IMPL_HPP
#define MADLIB_POSTGRES_SYSTEMINFORMATION_IMPL_HPP

namespace madlib {

namespace dbconnector {

namespace postgres {

namespace {


} // namespace

/**
 * @brief Get (and cache) information from the PostgreSQL catalog
 *
 * @param inFmgrInfo System-catalog information about the function. If no
 *     SystemInformation is currently available in struct FmgrInfo, this
 *     function will store it (and thus change the FmgrInfo struct).
 */
inline
SystemInformation*
SystemInformation::get(jobject fcinfo) {

    return NULL;
}

/**
 * @brief Get (and cache) information about a PostgreSQL type
 *
 * @param inPGInfo An PGInformation structure containing all cached information
 * @param inTypeID The OID of the type of interest
 */
inline
TypeInformation*
SystemInformation::typeInformation(Oid inTypeID) {
    TypeInformation* cachedTypeInfo = NULL;

    return cachedTypeInfo;
}

/**
 * @brief Get (and cache) information about a PostgreSQL function
 *
 * @param inFuncID The OID of the function of interest
 * @return
 */
inline
FunctionInformation*
SystemInformation::functionInformation(Oid inFuncID) {
    FunctionInformation* cachedFuncInfo = NULL;

    return cachedFuncInfo;
}

/**
 * @brief Retrieve the name of the specified type
 */
inline
const char*
TypeInformation::getName() {
    return name;
}

inline
bool
TypeInformation::isByValue() {
    return byval;
}

inline
int16_t
TypeInformation::getLen() {
    return len;
}

inline
char
TypeInformation::getType() {
    return type;
}

/**
 * @brief Retrieve the full function name (including arguments)
 *
 * We currently do not cache this information because we expect this function
 * to be primarily called by error handlers.
 */
inline
const char*
FunctionInformation::getFullName() {
    return "function";
}

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_SYSTEMINFORMATION_IMPL_HPP)
