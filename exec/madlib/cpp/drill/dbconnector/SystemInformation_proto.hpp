/* ----------------------------------------------------------------------- *//**
 *
 * @file SystemInformation.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_SYSTEMINFORMATION_PROTO_HPP
#define MADLIB_POSTGRES_SYSTEMINFORMATION_PROTO_HPP

#include "UDF_proto.hpp" // We need UDF::Pointer

namespace madlib {

namespace dbconnector {

namespace postgres {

/**
 * @brief Cached information about PostgreSQL types
 *
 * For explanations, see struct FormData_pg_type defined in pg_type.h
 * and struct TypeCacheEntry defined in typcache.h.
 */
struct TypeInformation {
    /**
     * OID and hash key. Must be the first element.
     */
    Oid oid;

    /**
     * Type name
     */
    char name[NAMEDATALEN];

	/**
	 * For a fixed-size type, typlen is the number of bytes PostgreSQL uses to
	 * represent a value of this type, e.g., 4 for an int4.	But for a
	 * variable-length type, typlen is negative.  -1 indicates a "varlena" type
     * (one that has a length word), -2 indicates a null-terminated C string.
	 */
	int16_t len;

    /**
     * typbyval determines whether internal Postgres routines pass a value of
	 * this type by value or by reference.
     */
	bool byval;

    /**
     * pg_type.h defines the following types:
     * <tt>TYPTYPE_{BASE|COMPOSITE|DOMAIN|ENUM|PSEUDO}</tt>
     */
    char type;

    bool isCompositeType();
    const char* getName();
    bool isByValue();
    int16_t getLen();
    char getType();
};

/**
 * @brief Cached information about PostgreSQL functions
 *
 * For explanations, see struct FmgrInfo in fmgr.h,
 * struct FuncCallContext in funcapi.h, enum TypeFuncClass in funcapi.h,
 * and struct FormData_pg_proc in pg_proc.h.
 */
struct FunctionInformation {
    /**
     * OID and hash key. Must be the first element.
     */
    Oid oid;

    /**
     * Function pointer to C++ function. If this is non-null, the function
     * is known to be implemented on top of the C++ abstraction layer. NULL
     * if not or unknown. If non-NULL, we this function pointer will be
     * called directly, thus circumventing any detour through the backend.
     */
    UDF::Pointer cxx_func;



    /**
     * Number of input arguments. Note that short is the data type used in
     * <tt>struct FunctionCallInfoData</tt>.
     */
    uint16_t nargs;

    /**
     * Array (of length nargs) containing the argument type IDs (excludes
     * OUT parameters).
     */
    Oid *argtypes;

    /**
     * True if we the function may have different return types.
     * False if we the function will always have the same return type.
     */
    bool polymorphic;

    /**
     * True if the function always returns null whenever any of its
     * arguments are null. If this parameter is specified, the function is
     * not executed when there are null arguments.
     */
    bool isstrict;

    /**
     * True if function is to be executed with the privileges of the user
     * that created it. The attribute is called SECURITY DEFINER in
     * PostgreSQL.
     */
    bool secdef;

    /**
     * OID of result type
     */
    Oid rettype;


    /**
     * Backpointer to SystemInformation
     */
    SystemInformation* mSysInfo;

    const char* getFullName();
};

/**
 * @brief Cached information about the PostgreSQL system catalog
 *
 * In order to guarantee type-safety through reflection/type-introspection, the
 * C++ AL has to call many PostgreSQL functions that are tagged as expensive due
 * to lookups in the type cache or even in the system catalog. We therefore wrap
 * all catalog lookups in this class and store the results in our own cache.
 * There is one cache per entry point into the C++ AL, i.e., one per function
 * that is called from the backend. If a UDF based on this AL calls another such
 * UDF, the same cache is reused.
 * Each cache is stored in the \c fn_extra field of struct \c FmgrInfo (or in
 * the \c user_fctx field of struct \c FuncCallContext). As such, the cache only
 * lives till the end of the current query (see
 * <http://www.postgresql.org/docs/current/static/plhandler.html>).
 *
 * @note
 *     In order to not leave defined C++ behavior, this must be a plain-old data
 *     (POD) for the following reason: We store a pointer to our cached
 *     information in the fn_extra field of struct FmgrInfo (or in
 *     the user_fctx field of struct FuncCallContext). We cannot do any
 *     cleanup but instead rely on the PostgreSQL garbage collector.
 */
struct SystemInformation {
    /**
     * OID of first C++ AL function in the current execution stack.
     */
    Oid entryFuncOID;

    /**
     * The memory context used for the hash tables.
     */
    MemoryContext cacheContext;

    /**
     * Collation for function(s) to use. This will be set to fncollation
     * of struct FunctionCallInfoData if the entry-function call (i.e., the
     * first C++ AL function call in the current execution stack).
     * Note: Collation support has been added to PostgreSQL with commit
     * d64713df by Tom Lane <tgl@sss.pgh.pa.us>
     * on Tue Apr 12 2011 23:19:24 UTC. First release: PG9.1. For version, prior
     * to this, collationOID will be InvalidOid.
     */
    Oid collationOID;

    /**
     * As we will put the SystemInformation instance to the pointer
     * static_cast<FuncCallContext*>(inFmgrInfo->fn_extra)->user_fctx
     * (retset == 1) or inFmgrInfo->fn_extra (retset == -1), we provide this
     * pointer for the users to put their own context to this pointer.
     */
    void *user_fctx;

    static SystemInformation* get(jobject fcinfo);
    TypeInformation* typeInformation(Oid inTypeID);
    FunctionInformation* functionInformation(Oid inFuncID);
};

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_SYSTEMINFORMATION_PROTO_HPP)
