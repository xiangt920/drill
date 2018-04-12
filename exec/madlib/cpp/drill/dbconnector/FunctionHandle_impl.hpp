/* ----------------------------------------------------------------------- *//**
 *
 * @file FunctionHandle_impl.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_FUNCTIONHANDLE_IMPL_HPP
#define MADLIB_POSTGRES_FUNCTIONHANDLE_IMPL_HPP

namespace madlib {

namespace dbconnector {

namespace postgres {

inline
FunctionHandle::Argument::Argument(AnyType inValue)
  : AnyType(inValue) { }

template <typename T>
inline
FunctionHandle::Argument::Argument(const T& inValue)
  : AnyType(inValue, true /* forceLazyConversionToDatum */) { }

inline
FunctionHandle::FunctionHandle(SystemInformation* inSysInfo, Oid inFuncID)
  : mSysInfo(inSysInfo),
    mFuncInfo(inSysInfo->functionInformation(inFuncID)),
    mFuncCallOptions(GarbageCollectionAfterCall) { }

inline
UDF::Pointer
FunctionHandle::funcPtr() {
    return mFuncInfo->cxx_func;
}

/**
 * @brief Return the OID of this function
 */
inline
Oid
FunctionHandle::funcID() const {
    return mFuncInfo->oid;
}

inline
FunctionHandle&
FunctionHandle::setFunctionCallOptions(uint32_t inFlags) {
    mFuncCallOptions = inFlags;
    return *this;
}

inline
FunctionHandle&
FunctionHandle::unsetFunctionCallOptions(uint32_t inFlags) {
    mFuncCallOptions &= ~inFlags;
    return *this;
}

inline
uint32_t
FunctionHandle::getFunctionCallOptions() const {
    return mFuncCallOptions;
}

/**
 * @brief Wrapper around FunctionCallInvoke
 *
 * We factor FunctionCallInvoke out in a separate function because local
 * variables could potentially be clobbered during the 'longjmp' otherwise.
 */
inline
Datum
FunctionHandle::internalInvoke(jobject inFCInfo) {
    Datum result = 0;
    return result;
}

inline
AnyType
FunctionHandle::invoke(AnyType &args) {
    madlib_assert(args.isComposite(), std::logic_error(
        "FunctionHandle::invoke() called with simple type."));

    if (args.numFields() > mFuncInfo->nargs)
        throw std::invalid_argument(std::string("More arguments given than "
            "expected by '")
            + mSysInfo->functionInformation(mFuncInfo->oid)->getFullName()
            + "'.");

    bool hasNulls = false;
    for (uint16_t i = 0; i < args.numFields(); ++i)
        hasNulls |= args[i].isNull();

    // If function is strict, we must not call the function at all
    if (mFuncInfo->isstrict && hasNulls)
        return AnyType();

    if (mFuncInfo->cxx_func &&
        !(mFuncCallOptions & GarbageCollectionAfterCall)) {

        // We have called this function before, so we can now do a shortcut:
        // Directly call the C++ function without any detour through the backend
        AnyType::LazyConversionToDatumOverride raii(true);
        return mFuncInfo->cxx_func(args);
    }


    Datum result = 0;


    return AnyType(mSysInfo, result, mFuncInfo->rettype, /* isMutable */ true);
}

inline
AnyType
FunctionHandle::operator()() {
    AnyType nil;
    return invoke(nil);
}

// In the following, we define
// AnyType operator()(FunctionHandle::Argument& inArgs1, ...,
//     FunctionHandle::Argument& inArgsN)
// using Boost.Preprocessor.
#define MADLIB_APPEND_ARG(z, n, data) \
    << data ## n
#define MADLIB_OPERATOR_DEF(z, n, _ignored) \
    inline \
    AnyType \
    FunctionHandle::operator()( \
        BOOST_PP_ENUM_PARAMS_Z(z, BOOST_PP_INC(n), \
            FunctionHandle::Argument inArg) \
    ) { \
        AnyType args; \
        args BOOST_PP_REPEAT(BOOST_PP_INC(n), MADLIB_APPEND_ARG, inArg); \
        return invoke(args); \
    }
BOOST_PP_REPEAT(MADLIB_FUNC_MAX_ARGS, MADLIB_OPERATOR_DEF, 0 /* ignored */)
#undef MADLIB_OPERATOR_DEF
#undef MADLIB_APPEND_ARG

inline
SystemInformation*
FunctionHandle::getSysInfo() const {
    return mSysInfo;
}

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_FUNCTIONHANDLE_IMPL_HPP)
