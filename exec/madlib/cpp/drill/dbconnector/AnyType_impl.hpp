/* ----------------------------------------------------------------------- *//**
 *
 * @file AnyType_impl.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_ANYTYPE_IMPL_HPP
#define MADLIB_POSTGRES_ANYTYPE_IMPL_HPP

#include <common/array.h>

namespace madlib {

namespace dbconnector {

namespace postgres {

inline void * AnyType::getUserFuncContext(){
        return this->mSysInfo->user_fctx;
}
inline void AnyType::setUserFuncContext(void * user_fctx){
        this->mSysInfo->user_fctx = user_fctx;
}
inline MemoryContext AnyType::getCacheMemoryContext(){
    return this->mSysInfo->cacheContext;
}
inline std::function<Datum()> AnyType::getMToDatumFn() {
  return this->mToDatumFn;
}

inline
AnyType::AnyType(jobject inFnCallInfo)
  : mContentType(FunctionComposite),
    mContent(),
    mToDatumFn(),
    mDatum(0),
    fcinfo(inFnCallInfo),
    mSysInfo(SystemInformation::get(inFnCallInfo)),
    mTypeID(InvalidOid),
    mTypeName(NULL),
    mIsMutable(false)
    { }

inline
AnyType::AnyType(SystemInformation* inSysInfo, Datum inDatum,
    Oid inTypeID, bool inIsMutable)
  : mContentType(Scalar),
    mContent(),
    mToDatumFn(),
    mDatum(inDatum),
    fcinfo(NULL),
    mSysInfo(inSysInfo),
    mTypeID(inTypeID),
    mTypeName(inSysInfo->typeInformation(inTypeID)->getName()),
    mIsMutable(inIsMutable)
    { }

/**
 * @brief Template constructor (will \b not be used as copy constructor)
 *
 * @param inValue The value to initialize this AnyType object with
 * @param inForceLazyConversionToDatum If true, initialize this AnyType object
 *     as if <tt>lazyConversionToDatum() == true</tt>
 *
 * This constructor will be invoked when initializing an AnyType object with
 * any scalar value (including arrays, but excluding composite types). This will
 * typically only happen for preparing the return value of a user-defined
 * function.
 * This constructor immediately converts the object into a PostgreSQL Datum
 * using the TypeTraits class. If memory has to be retained, it has to be done
 * there.
 */
template <typename T>
inline
AnyType::AnyType(const T& inValue, bool inForceLazyConversionToDatum)
  : mContentType(Scalar),
    mContent(),
    mToDatumFn(),
    mDatum(0),
    fcinfo(NULL),
    mSysInfo(TypeTraits<T>::toSysInfo(inValue)),
    mTypeID(TypeTraits<T>::oid),
    mTypeName(TypeTraits<T>::typeName()),
    mIsMutable(TypeTraits<T>::isMutable) {

    if (inForceLazyConversionToDatum || lazyConversionToDatum()) {
        mContent = inValue;
        mToDatumFn = std::bind(TypeTraits<T>::toDatum, inValue);
    } else {
        mDatum = TypeTraits<T>::toDatum(inValue);
    }
}

/**
 * @brief Default constructor, initializes AnyType object as Null
 *
 * This constructor initializes the object as Null. It must also be used for
 * building a composite type. After construction, use operator<<() to append
 * values to the composite object.
 */
inline
AnyType::AnyType()
  : mContentType(Null),
    mContent(),
    mToDatumFn(),
    mDatum(0),
    fcinfo(NULL),
    mSysInfo(NULL),
    mTypeID(InvalidOid),
    mTypeName(NULL),
    mIsMutable(false)
    { }

/**
 * @brief Verify consistency of AnyType object. Throw exception if not.
 */
inline
void
AnyType::consistencyCheck() const {
    const char *kMsg("Inconsistency detected while converting between "
        "PostgreSQL and C++ types.");

    madlib_assert(mContentType != Null || (mDatum == 0 && mContent.empty() &&
        fcinfo == NULL && mSysInfo == NULL &&
        mTypeID == InvalidOid && mTypeName == NULL && mChildren.empty()),
        std::logic_error(kMsg));
    madlib_assert(mContentType == Scalar || mContent.empty(),
        std::logic_error(kMsg));
    madlib_assert(mContentType != FunctionComposite || fcinfo != NULL,
        std::logic_error(kMsg));
    madlib_assert(mContentType != NativeComposite,
        std::logic_error(kMsg));
    madlib_assert(mContentType != ReturnComposite || (!mChildren.empty() &&
        mTypeID == InvalidOid),
        std::logic_error(kMsg));
    madlib_assert(mContentType == ReturnComposite || mChildren.empty(),
        std::logic_error(kMsg));
    madlib_assert(
        (mContentType != FunctionComposite && mContentType != NativeComposite)
        || mSysInfo != NULL,
        std::logic_error(kMsg));
    madlib_assert(mChildren.size() <= std::numeric_limits<uint16_t>::max(),
        std::runtime_error("Too many fields in composite type."));
}

/**
 * @brief Convert object to the type specified as template argument
 *
 * @tparam T Type to convert object to
 */
template <typename T>
inline
T
AnyType::getAs() const {
    consistencyCheck();

    if (isNull())
        throw std::invalid_argument("Invalid type conversion. "
            "Null where not expected.");

    if (isComposite())
        throw std::invalid_argument("Invalid type conversion. "
            "Composite type where not expected.");

    // Verify type OID
    if (TypeTraits<T>::oid != InvalidOid && mTypeID != TypeTraits<T>::oid) {
        std::stringstream errorMsg;
        errorMsg << "Invalid type conversion. Expected type ID "
            << TypeTraits<T>::oid;
        if (mSysInfo)
            errorMsg << " ('"
                << mSysInfo->typeInformation(TypeTraits<T>::oid)->getName()
                << "')";
        errorMsg << " but got " << mTypeID;
        if (mSysInfo)
            errorMsg << " ('"
                << mSysInfo->typeInformation(mTypeID)->getName() << "')";
        errorMsg << '.';
        throw std::invalid_argument(errorMsg.str());
    }

    // Verify type name
    if (TypeTraits<T>::typeName() &&
        std::strncmp(mTypeName, TypeTraits<T>::typeName(), NAMEDATALEN)) {

        std::stringstream errorMsg;
        errorMsg << "Invalid type conversion. Expected type '"
            << TypeTraits<T>::typeName() << "' but backend type name is '"
            << mTypeName << "' (ID " << mTypeID << ").";
        throw std::invalid_argument(errorMsg.str());
    }

    if (mContent.empty()) {
        bool needMutableClone = (TypeTraits<T>::isMutable && !mIsMutable);
        return TypeTraits<T>::toCXXType(mDatum, needMutableClone, mSysInfo);
    } else {
        // any_cast<T*> will not throw but return a NULL pointer if of
        // incorrect type
        const T* value = boost::any_cast<T>(&mContent);
        if (value == NULL) {
            std::stringstream errorMsg;
            errorMsg << "Invalid type conversion. Expected type '"
                << typeid(T).name() << "' but stored type is '"
                << mContent.type().name() << "'.";
            throw std::runtime_error(errorMsg.str());
        }
        return *value;
    }
}

/**
 * @brief Return if object is Null
 */
inline
bool
AnyType::isNull() const {
    return mContentType == Null;
}

/**
 * @brief Return if object is of composite type (also called row type or
 *     user-defined type)
 */
inline
bool
AnyType::isComposite() const {
    return mContentType == FunctionComposite ||
        mContentType == NativeComposite || mContentType == ReturnComposite;
}

/**
 * @brief Return the number of fields in a composite value.
 *
 * @returns The number of fields in a composite value. In the case of a scalar
 *     value, return 1. If the content is NULL, return 0.
 */
inline
uint16_t
AnyType::numFields() const {
    consistencyCheck();

    switch (mContentType) {
        case Null: return 0;
        case Scalar: return 1;
        case ReturnComposite: return static_cast<uint16_t>(mChildren.size());
        default:
            // This should never happen
            throw std::logic_error("Unhandled case in AnyType::numFields().");
    }
}

/**
 * @brief Return the n-th element from a composite value
 *
 * To the user, AnyType is a fully recursive type: Each AnyType object can be a
 * composite object and be composed of a number of other AnyType objects.
 * Function written using the C++ abstraction layer have a single logical
 * argument of type AnyType.
 */
inline
AnyType
AnyType::operator[](uint16_t inID) const {
    consistencyCheck();

    if (isNull()) {
        // Handle case mContentType == NULL
        throw std::invalid_argument("Invalid type conversion. "
            "Null where not expected.");
    }
    if (!isComposite()) {
        // Handle case mContentType == Scalar
        throw std::invalid_argument("Invalid type conversion. "
            "Composite type where not expected.");
    }

    if (mContentType == ReturnComposite)
        return mChildren[inID];

    return NULL;
}

/**
 * @brief Add an element to a composite value, for returning to the backend
 */
inline
AnyType&
AnyType::operator<<(const AnyType &inValue) {
    consistencyCheck();

    madlib_assert(mContentType == Null || mContentType == ReturnComposite,
        std::logic_error("Internal inconsistency while creating composite "
            "return value."));

    mContentType = ReturnComposite;
    mChildren.push_back(inValue);
    return *this;
}

/**
 * @brief Return a PostgreSQL Datum representing the current object
 *
 * If the current object is Null, we still return <tt>Datum(0)</tt>, i.e., we
 * return a valid Datum. It is the responsibilty of the caller to separately
 * call isNull().
 *
 * The only *conversion* taking place in this function is *combining* Datums
 * into a tuple. At this place, we do not have to worry any more about retaining
 * memory.
 *
 * @param inFnCallInfo The PostgreSQL FunctionCallInfo that was passed to the
 *     UDF. For polymorphic functions or functions that return RECORD, the
 *     function-call information (specifically, the expression parse tree)
 *     is necessary to dynamically resolve type information.
 * @param inTargetTypeID PostgreSQL OID of the target type to convert to. If
 *     omitted the target type is the return type of the function specified by
 *     \c inFnCallInfo.
 *
 * @see getAsDatum(const FunctionCallInfo)
 */
inline
Datum
AnyType::getAsDatum(jobject inFnCallInfo,
    Oid inTargetTypeID) const {

    consistencyCheck();

    // The default value to return in case of Null is 0. Note, however, that
    // 0 can also be a perfectly valid (non-null) Datum. It is the caller's
    // responsibility to call isNull() separately.
    if (isNull())
        return 0;

    // Note: mSysInfo is NULL if this object was not an argument from the
    // backend.
    SystemInformation* sysInfo = SystemInformation::get(inFnCallInfo);
    FunctionInformation* funcInfo = sysInfo
        ->functionInformation(0);
    Datum returnValue;

    if (mTypeID != InvalidOid && inTargetTypeID != mTypeID) {
        std::stringstream errorMsg;
        errorMsg << "Invalid type conversion. "
            "Backend expects type ID " << inTargetTypeID << " ('"
            << sysInfo->typeInformation(inTargetTypeID)->getName() << "') "
            "but supplied type ID is " << mTypeID << + " ('"
            << sysInfo->typeInformation(mTypeID)->getName() << "').";
        throw std::invalid_argument(errorMsg.str());
    }

    if (mTypeName && std::strncmp(mTypeName,
        sysInfo->typeInformation(inTargetTypeID)->getName(),
        NAMEDATALEN)) {

        std::stringstream errorMsg;
        errorMsg << "Invalid type conversion. Backend expects type '"
            << sysInfo->typeInformation(inTargetTypeID)->getName() <<
            "' (ID " << inTargetTypeID << ") but internal type name is '"
            << mTypeName << "'.";
        throw std::invalid_argument(errorMsg.str());
    }
    returnValue = mContent.empty() ? mDatum : mToDatumFn();
    return returnValue;
}

/**
 * @brief Convert values to PostgreSQL Datum in AnyType constructor (or only
 *     when needed)?
 *
 * Usually, AnyType objects are only used to retrieve or return data from/to the
 * backend. However, there are exceptions. For instance, when calling a
 * FunctionHandle, data might be passed directly from one C++ function to
 * another. In this case, it would be wasteful to convert arguments to
 * PostgreSQL Datum type, and it is better to only lazily convert to Datum
 * (i.e., only when needed by getAsDatum()).
 *
 * Since PostgreSQL is single-threaded, it is sufficient to maintain a global
 * variable that contains whether lazy conversion is requested.
 */
inline
bool
AnyType::lazyConversionToDatum() {
    return sLazyConversionToDatum;
}

inline
AnyType::LazyConversionToDatumOverride::LazyConversionToDatumOverride(
    bool inLazyConversionToDatum) {

    mOriginalValue = AnyType::sLazyConversionToDatum;
    AnyType::sLazyConversionToDatum = inLazyConversionToDatum;
}

inline
AnyType::LazyConversionToDatumOverride::~LazyConversionToDatumOverride() {
    AnyType::sLazyConversionToDatum = mOriginalValue;
}

/**
 * @brief Return an AnyType object representing Null.
 *
 * @internal
 *     An object representing Null is not guaranteed to be unique. In fact, here
 *     we simply return an AnyType object initialized by the default
 *     constructor.
 *
 * @see AbstractionLayer::AnyType::AnyType()
 */
inline
AnyType
Null() {
    return AnyType();
}

  inline int32* extract2DDoubleArray(AnyType array, double ** &result) {

    if (array.isNull()) {
      return 0;
    }
    using namespace madlib::dbal::eigen_integration;
    using namespace madlib::dbconnector::postgres;
    MutableNativeColumnVector vec = array.getAs<MutableNativeColumnVector>();
    ArrayHandle<double> handle = vec.memoryHandle();
    size_t dims = handle.dims();
    madlib_assert(dims == 2, std::runtime_error(
      "The number of dimensions must be 2."));
    const double* ptr = handle.ptr();
    const ArrayType* arr = handle.array();

    result = static_cast<double **>(palloc(sizeof(double *) * arr->dims[0]));
    size_t size = sizeof(double);
    for (int j = 0; j < arr->dims[0]; ++j) {
      result[j] = static_cast<double *>(palloc(size * arr->dims[1]));
      for (int i = 0; i < arr->dims[1]; ++i) {
        result[j][i] = ptr[j*arr->dims[0]+i];
      }
    }

    return arr->dims;
  }


  inline int32 extractDoubleArray(AnyType array, double *&result) {
    if (array.isNull()) {
      return 0;
    }
    using namespace madlib::dbal::eigen_integration;
    using namespace madlib::dbconnector::postgres;

    MutableNativeColumnVector vec = array.getAs<MutableNativeColumnVector>();
    ArrayHandle<double> handle = vec.memoryHandle();
    madlib_assert(handle.dims() > 0, std::runtime_error(
      "The number of dimensions must be greater than 0."));
    const double *ptr = handle.ptr();
    Size num = handle.size();
    Size size = num * sizeof(double);
    result = static_cast<double *>(palloc(size));
    memcpy(result, ptr, size);
    return static_cast<int32>(num);
  }

  inline int32 extractDoubleArrayH(AnyType array, double *&result) {
    if (array.isNull()) {
      return 0;
    }
    using namespace madlib::dbconnector::postgres;

    ArrayHandle<double> handle = array.getAs<ArrayHandle<double> >();
    madlib_assert(handle.dims() > 0, std::runtime_error(
      "The number of dimensions must be greater than 0."));
    const double *ptr = handle.ptr();
    Size num = handle.size();
    Size size = num * sizeof(double);
    result = static_cast<double *>(palloc(size));
    memcpy(result, ptr, size);
    return static_cast<int32>(num);
}

inline int32* extract2DDoubleArrayH(AnyType array, double **&result) {
  if (array.isNull()) {
    return 0;
  }
  using namespace madlib::dbconnector::postgres;

  ArrayHandle<double> handle = array.getAs<ArrayHandle<double> >();
  size_t dims = handle.dims();
  madlib_assert(dims == 2, std::runtime_error(
    "The number of dimensions must be 2."));
  const double* ptr = handle.ptr();
  const ArrayType* arr = handle.array();

  result = static_cast<double **>(palloc(sizeof(double *) * arr->dims[0]));
  size_t size = sizeof(double);
  for (int j = 0; j < arr->dims[0]; ++j) {
    result[j] = static_cast<double *>(palloc(size * arr->dims[1]));
    for (int i = 0; i < arr->dims[1]; ++i) {
      result[j][i] = ptr[j*arr->dims[0]+i];
    }
  }

  return arr->dims;
}

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_ANYTYPE_IMPL_HPP)
