# ------------------------------------------------------------------------------
# Drill Port
# ------------------------------------------------------------------------------

set(PORT "Drill")
string(TOUPPER ${PORT} PORT_UC)
string(TOLOWER ${PORT} PORT_LC)
set(PORT_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
get_filename_component(PORT_DIR_NAME "${PORT_SOURCE_DIR}" NAME)
set(PORT_DEPLOY_SCRIPT "${CMAKE_BINARY_DIR}/deploy/Component_${PORT}.cmake")

list(APPEND CMAKE_MODULE_PATH
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(DrillUtils)


# -- 1. Specify files that will be compiled into the shared library, for *all*
#       versions of this port --------------------------------------------------

include_directories("${CMAKE_CURRENT_SOURCE_DIR}")

set(MAD_DBAL_SOURCES
  ${MAD_SOURCES})
list(APPEND MAD_DBAL_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/Allocator_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/Allocator_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/AnyType_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/AnyType_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/ArrayHandle_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/ArrayHandle_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/ArrayWithNullException_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/Backend.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/ByteString_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/ByteString_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/Compatibility.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/dbconnector.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/EigenIntegration_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/EigenIntegration_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/FunctionHandle_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/FunctionHandle_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/NewDelete.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/NativeRandomNumberGenerator_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/NativeRandomNumberGenerator_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/OutputStreamBuffer_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/OutputStreamBuffer_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/PGException_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/SystemInformation_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/SystemInformation_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/TransparentHandle_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/TransparentHandle_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/TypeTraits_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/TypeTraits_proto.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/UDF_impl.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/UDF_proto.hpp"
  )
file(GLOB_RECURSE COMMON_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/common/*.cpp")
list(APPEND EXECUTABLE_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/dbconnector/main.cpp"
  ${MAD_DBAL_SOURCES}
  ${COMMON_SOURCES})
list(APPEND MAD_DBAL_SOURCES ${COMMON_SOURCES})

# Modules code

file(GLOB_RECURSE MODULES_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/modules/*/*.hpp"
  )
list(APPEND MAD_DBAL_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/modules/modules.hpp"
  ${MODULES_SOURCES}
  )
# FIXME: Convert legacy source code written in C
# BEGIN JNI Code


# END JNI Code


# -- 2. Copy all SQL files. Since SQL files contain file names, they are only
#       preprocessed at installation time, i.e., by madpack. -------------------

add_sql_files(
  SQL_TARGET_FILES
  "modules"
  "${CMAKE_CURRENT_BINARY_DIR}/modules"
)
# FIXME: Check legacy code for compliance with new architecture
# BEGIN Legacy Code

file(GLOB_RECURSE LEGACY_SQL_FILES RELATIVE
  "${CMAKE_SOURCE_DIR}/methods"
  "${CMAKE_SOURCE_DIR}/methods/*.sql_in")
foreach (CURRENT_FILE ${LEGACY_SQL_FILES})
  get_filename_component(CURRENT_PATH "${CMAKE_SOURCE_DIR}/methods/${CURRENT_FILE}" ABSOLUTE)
  set(OUTFILE "${CMAKE_CURRENT_BINARY_DIR}/modules/${CURRENT_FILE}")
  string(REPLACE "/src/pg_gp" "" OUTFILE ${OUTFILE})
  string(REPLACE "/sql/" "/test/" OUTFILE ${OUTFILE})
  add_custom_command(OUTPUT "${OUTFILE}"
    COMMAND "${CMAKE_SOURCE_DIR}/cmake/TestIfNoUTF8BOM.py" "${CURRENT_PATH}"
    COMMAND ${CMAKE_COMMAND} -E copy "${CURRENT_PATH}" "${OUTFILE}"
    DEPENDS "${CURRENT_PATH}"
    COMMENT "Validating and copying ${CURRENT_FILE}"
    )
  list(APPEND SQL_TARGET_FILES ${OUTFILE})
endforeach (CURRENT_FILE ${LEGACY_SQL_FILES})

# END Legacy Code
add_custom_target(sqlFiles_${PORT_LC} ALL DEPENDS ${SQL_TARGET_FILES})


# -- 3. Install all SQL files --------------------------------------------------

cpack_add_port_group_and_component_for_all_versions()
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/modules
  DESTINATION ports/${PORT_DIR_NAME}
  COMPONENT ${PORT_LC}_any
  REGEX "^(.*/)?\\.DS_Store\$" EXCLUDE
  )

# Add a jni library drill port
macro(add_jni_library)

  find_package(JNI REQUIRED)
  # add jni
  include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
  include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})

  include_directories(jni)
  file(GLOB_RECURSE JNI_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/jni/*.cpp
    )
  list(APPEND JNI_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/jni/jni_ops.hpp"
    )

endmacro(add_jni_library)

# -- 4. Building the shared library is specific for each version of
#       this port --------------------------------------------------------------

function(add_${PORT_LC}_library IN_PORT_VERSION)
  string(REPLACE "." "_" PORT_VERSION_UNDERSCORE "${IN_PORT_VERSION}")
  set(DBMS "${PORT_LC}_library")
  set(DBMS_UC "${PORT_UC}_${PORT_VERSION_UNDERSCORE}")

  # -- 4.1. Compile C/C++ files---------------------------------------------------

  message(">> Adding ${PORT} "
    "${IN_PORT_VERSION} "
    "to target list...")

  list(APPEND MAD_DBAL_SOURCES ${JNI_SOURCES})
  if (NOT DRILL_MADLIB_LIB_PATH)
    set(DRILL_MADLIB_LIB_PATH lib)
  endif ()
  add_madlib_connector_library(madlib_${DBMS}
    ${DRILL_MADLIB_LIB_PATH}
    "${${DBMS_UC}_EXECUTABLE}"
    ${MAD_DBAL_SOURCES}
    )
  add_executable(test_main ${EXECUTABLE_SOURCES})

  # FIXME: Convert legacy source code written in C
  # BEGIN Legacy Code

  # From the CMake documentation:
  # "Source file properties are visible only to targets added in the same
  # directory (CMakeLists.txt)." We therefore have to set them for each
  # version of this port!
  if (CMAKE_COMPILER_IS_GNUCC)
    # FIXME: In the C code, we have several places where strict aliasing
    # rules are violated. See this web page for some background:
    # http://dbp-consulting.com/tutorials/StrictAliasing.html
    # For now, we tell GCC that it cannot rely on strict aliasing rules.
    # Downside: We forgo some potential optimization.
    # The warning GCC would output without -fno-strict-aliasing is:
    # dereferencing type-punned pointer will break strict-aliasing rules
    set_source_files_properties(${LEGACY_C_FILES} PROPERTIES
      COMPILE_FLAGS "-fno-strict-aliasing")
  endif (CMAKE_COMPILER_IS_GNUCC)

  # errOmitLocation is used in legacy C code, but it is Greenplum only
  # we define it so that it has no effect for PostgreSQL.
  set_source_files_properties(${LEGACY_C_FILES} PROPERTIES
    COMPILE_DEFINITIONS "NO_PG_MODULE_MAGIC;errOmitLocation=(int)"
    )

  # END Legacy Code


  # -- 4.2. Preprocess all Python files ------------------------------------------

  # -- 4.3. Install shared library, Python files, and M4 header ------------------

  cpack_add_version_component()
  install(TARGETS madlib_${DBMS}
    LIBRARY DESTINATION ports/${PORT_DIR_NAME}/${IN_PORT_VERSION}/lib
    COMPONENT ${DBMS}
    )
  install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/modules
    DESTINATION ports/${PORT_DIR_NAME}/${IN_PORT_VERSION}
    COMPONENT ${DBMS}
    REGEX "^(.*/)?\\.DS_Store\$" EXCLUDE
    )
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/madpack/SQLCommon.m4"
    DESTINATION ports/${PORT_DIR_NAME}/${IN_PORT_VERSION}/madpack
    COMPONENT ${DBMS}
    )


endfunction(add_${PORT_LC}_library)


# -- 5. Provide a macro to be called from CMakeLists.txt in the version
#       directory. We want these files to be one-liners. -----------------------

# Add the current version as a target. This is a macro because it calls
# find_package, and we want the side effects (the defined variables) to be
# visible after the invocation
#
macro(add_current_${PORT_LC}_version)
  get_filename_component(_VERSION "${CMAKE_CURRENT_SOURCE_DIR}" NAME)
  string(REPLACE "." "_" _VERSION_UNDERSCORES ${_VERSION})

endmacro(add_current_${PORT_LC}_version)

# -- Extension support
# If the DBMS can support CREATE EXTENSION, call this function
# after add_current_PORT_version()
function(add_extension_support)
  get_filename_component(_VERSION "${CMAKE_CURRENT_SOURCE_DIR}" NAME)
  string(REPLACE "." "_" PORT_VERSION_UNDERSCORE "${_VERSION}")
  set(DBMS "${PORT_LC}_${PORT_VERSION_UNDERSCORE}")
  set(DBMS_UC "${PORT_UC}_${PORT_VERSION_UNDERSCORE}")


  configure_file("${PORT_SOURCE_DIR}/extension/madlib.control_in"
    "${CMAKE_CURRENT_BINARY_DIR}/extension/madlib.control"
    @ONLY
    )
  # base file contains miscellaneous operations such like creating history table
#  set(EXTENSION_SQL_BASE "${CMAKE_CURRENT_BINARY_DIR}/extension/madlib.sql.base")
#  set(extschema "@extschema@")
#  configure_file("${PORT_SOURCE_DIR}/extension/madlib.sql.base_in"
#    "${EXTENSION_SQL_BASE}"
#    @ONLY
#    )
#  unset(extschema)


  # install target for extension
  # Unfortunately, cmake install command cannot be used here, as
  # DIRECTORY install doesn't have custom target option.
  add_custom_target(extension-install-${DBMS}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${${DBMS_UC}_SHARE_DIR}/madlib
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_BINARY_DIR}/modules ${${DBMS_UC}_SHARE_DIR}/madlib/modules
    COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/lib/libmadlib.so ${${DBMS_UC}_PKGLIB_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy
    ${EXTENSION_SQL} ${${DBMS_UC}_SHARE_DIR}/extension/
    COMMAND ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/extension/madlib.control
    ${${DBMS_UC}_SHARE_DIR}/extension/
    DEPENDS ${EXTENSION_SQL}
    )

  # add dependency to the root target.  This is necessary because
  # we need unique target name, and have "make install" work correctly
  # in the case of PGXN with META.json on top directory.
  add_dependencies(extension-install extension-install-${DBMS})
endfunction(add_extension_support)

add_custom_target(extension)
add_custom_target(extension-clean)
add_custom_target(extension-install)

# -- 6. Build shared library and copy version-specific file for all
#       ${PORT_UC}_X_Y_PG_CONFIG macros defined by the user. If none has been
#       defined, try to find any version this port. ----------------------------

determine_target_versions(VERSIONS)
foreach (VERSION ${VERSIONS})
  add_subdirectory(${VERSION})
endforeach (VERSION)

add_jni_library()
add_subdirectory(1.10)
