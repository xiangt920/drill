set(CMAKE_GENERATOR_CC  suncc gcc)
set(CMAKE_GENERATOR_CXX sunCC g++)

# -- CMake setup ---------------------------------------------------------------

project(drill-madlib)

cmake_minimum_required(VERSION 2.8.4 FATAL_ERROR)


find_package(Git REQUIRED)

set(MADLIB_GIT_DIR "${CMAKE_SOURCE_DIR}/madlib_src/madlib")

if (NOT EXISTS ${MADLIB_GIT_DIR})
  execute_process(
    COMMAND git clone http://gitlab.alipay-inc.com/olap/madlib.git #https://github.com/apache/madlib.git
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/madlib_src
  )

  execute_process(
    COMMAND git am ${CMAKE_SOURCE_DIR}/patch/AddDrill.patch.in
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/madlib_src/madlib
  )

  execute_process(
    COMMAND git am ${CMAKE_SOURCE_DIR}/patch/Copy_eigen_navtree_hacks.js.patch.in
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/madlib_src/madlib
  )

  execute_process(
    COMMAND git am ${CMAKE_SOURCE_DIR}/patch/ChangeArrayType.patch.in
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/madlib_src/madlib
  )

  execute_process(
    COMMAND git am ${CMAKE_SOURCE_DIR}/patch/CPackLicense.patch.in
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/madlib_src/madlib
  )

endif ()

set(USE_DRILL true)

add_subdirectory(madlib_src/madlib)


