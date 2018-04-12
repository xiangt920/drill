/* ----------------------------------------------------------------------- *//**
 *
 * @file DrillJni.cpp
 *
 * @brief Main file containing the entrance points into the C++ modules
 *
 *//* ----------------------------------------------------------------------- */

#include <jni.h>
#include "DrillJni.h"
#include "jni_ops.hpp"
#include <dbconnector/dbconnector.hpp>
#include <utils/MallocAllocator.hpp>


namespace madlib {

  namespace dbconnector {

    namespace postgres {

      bool AnyType::sLazyConversionToDatum = false;

      namespace {
#define INFO 17
#define WARNING 19
// No need to export these names to other translation units.
#ifndef NDEBUG
        OutputStreamBuffer<INFO, utils::MallocAllocator> gOutStreamBuffer;
        OutputStreamBuffer<WARNING, utils::MallocAllocator> gErrStreamBuffer;
#endif

      }

#ifndef NDEBUG
/**
 * @brief Informational output stream
 */
      std::ostream dbout(&gOutStreamBuffer);

/**
 * @brief Warning and non-fatal error output stream
 */
      std::ostream dberr(&gErrStreamBuffer);
#endif
    } // namespace postgres

  } // namespace dbconnector

} // namespace madlib

// Include declarations declarations
#include <modules/declarations.hpp>

// Now export the symbols
#undef DECLARE_UDF
#undef DECLARE_SR_UDF
#define DECLARE_UDF DECLARE_UDF_EXTERNAL
#define DECLARE_SR_UDF DECLARE_UDF_EXTERNAL

#include "modules/modules.hpp"

