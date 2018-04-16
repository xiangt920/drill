/* ----------------------------------------------------------------------- *//**
 *
 * @file OutputStreamBuffer_impl.hpp
 *
 *//* ----------------------------------------------------------------------- */

#ifndef MADLIB_POSTGRES_OUTPUTSTREAMBUFFER_IMPL_HPP
#define MADLIB_POSTGRES_OUTPUTSTREAMBUFFER_IMPL_HPP

namespace madlib {

namespace dbconnector {

namespace postgres {

/**
 * @brief Output a null-terminated C string.
 *
 * @param inMsg Null-terminated C string
 * @param inLength length of inMsg
 */
template <int ErrorLevel, template <class> class Allocator>
inline
void
OutputStreamBuffer<ErrorLevel, Allocator>::output(char *inMsg,
    std::size_t /* inLength */) const {

    volatile bool errorOccurred = false;
      try {
          elog(0, "%s", inMsg);
      } catch(std::exception e) {
          errorOccurred = true;
      }


    if (errorOccurred)
        throw std::runtime_error("An exception occured during message output.");
}

} // namespace postgres

} // namespace dbconnector

} // namespace madlib

#endif // defined(MADLIB_POSTGRES_OUTPUTSTREAMBUFFER_IMPL_HPP)
