/* ----------------------------------------------------------------------- *//**
 *
 * @file main.cpp
 *
 * @brief Main file containing the entrance points into the C++ modules
 *
 *//* ----------------------------------------------------------------------- */

// We do not write #include "dbconnector.hpp" here because we want to rely on
// the search paths, which might point to a port-specific dbconnector.hpp
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
#include <modules/regress/LinearRegression_proto.hpp>

// Now export the symbols
#undef DECLARE_UDF
#undef DECLARE_SR_UDF
#define DECLARE_UDF DECLARE_UDF_EXTERNAL
#define DECLARE_SR_UDF DECLARE_UDF_EXTERNAL

using namespace madlib::dbconnector::postgres;
using namespace madlib::modules::regress;
using namespace madlib::modules::stats;
using namespace madlib::dbal::eigen_integration;
typedef LinearRegressionAccumulator<madlib::RootContainer> LinRegrState;

int linregr_test() {
  linregr_transition lt;
  AnyType args = AnyType();

//  bytea *state = static_cast<bytea *>(malloc(4));
//  state->vl_len_[0] = 16;
//  state->vl_len_[1] = 0;
//  state->vl_len_[2] = 0;
//  state->vl_len_[3] = 0;
//
//  MutableByteString str = MutableByteString(state);
//  args << AnyType(str);
//
//  args << AnyType(50000.0);
//
//
//  double x[4] = {1, 590, 1, 770};
//  TransparentHandle<double> h = TransparentHandle<double >(x);
//  MappedColumnVector v;
//  v.rebind(h, 4);
//  args << AnyType(v, true);
//  AnyType r1 = lt.run(args);
  char r1_arr[] = {32, 3, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 106, -24, 64,
                   0, 0, 0, 32, 95, -96, -30, 65, 0, 0, 0, 0, 0, 106, -24, 64, 0, 0, 0, 0, 38, 34, 124, 65, 0, 0, 0, 0,
                   0, 106, -24, 64, 0, 0, 0, 0, -75, 91, -126, 65, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 112, -126,
                   64, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 16, -120, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16,
                   63, 21, 65, 0, 0, 0, 0, 0, 112, -126, 64, 0, 0, 0, 0, 112, -70, 27, 65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 16, -120, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 24, 34, 65};
  bytea *r1_bytea = reinterpret_cast<bytea *>(r1_arr);
  MutableByteString r1 = MutableByteString(r1_bytea);
  args = AnyType();
  args << r1;
  args << AnyType(85000.0);
  double x1[4] = {1, 1050, 2, 1410};
  TransparentHandle<double> h1 = TransparentHandle<double>(x1);
  MappedColumnVector v1;
  v1.rebind(h1, 4);
  args << AnyType(v1, true);
  AnyType r2 = lt.run(args);
  args = AnyType();
  args << r2;
  linregr_final lf;
  AnyType r = lf.run(args);

  return 1;
}

int linregr_result_test() {
  char state_arr[] = {32, 3, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16,
                      63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0,
                      0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0,
                      0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0,
                      0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 0, 0, 0, 16, 64, 0, 0, 0, 0, 0, 0, 24, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 34, 64, 0, 0, 0, 0, 0, 0};
  bytea *state_bytea = reinterpret_cast<bytea *>(state_arr);
  ByteString state_str = ByteString(state_bytea);
  LinRegrState s = state_str;
  AnyType tuple;
  LinearRegression result(s);
  tuple << result.coef
        << result.r2
        << result.stdErr
        << result.tStats
        << (s.numRows > s.widthOfX ? result.pValues : Null())
        << sqrt(result.conditionNo)
        << static_cast<uint64_t>(s.numRows)
        << result.vcov;
  double *coef = NULL;
  int32 coefNum = extractDoubleArray(tuple[7], coef);
  std::cout << "coef: ";
  for (int i = 0; i < coefNum; ++i) {
    std::cout << coef[i] << ",";
  }
  std::cout << std::endl;
  std::cout << std::endl;
  return 1;
}

int linregr_het_test() {
  double in_state[] = {16, 0, 0, 0, 0, 0, 0, 0};
  double in_y = 50000;
  double in_x[] = {1, 590, 1, 770};
  double in_coef[] = {-12849.416895987, 28.961392265176528, 10181.62907126476, 50.51689491535399};

  int num_x = 4;
  int num_coef = 4;
  AnyType result = NULL;

  DEFINE_MBS(state);

  TransparentHandle<double> han_x = TransparentHandle<double>(in_x);
  MappedColumnVector vec_x;
  vec_x.rebind(han_x, num_x);
  DEFINE_MCV(double, coef);

  hetero_linregr_transition hlt;
  AnyType args = AnyType();
  args << AnyType(state_str)
       << AnyType(in_y)
       << AnyType(vec_x)
       << AnyType(vec_coef);
  result = hlt.run(args);

  return 1;
}

int logregr_step_irls_transition_test() {
  double state[4] = {0, 0, 0, 0};
  bool in_y = true;
  double in_x[6] = {1,1,70,1,1,1};
  int num_x = 6;
  double pre_state[0];

  ArrayType *arr_state = madlib_construct_array_direct(state, 4, FLOAT8OID, 8, true, 'd');

  MutableArrayHandle<double> han_state = MutableArrayHandle<double>(arr_state);
  DEFINE_MCV(double, x);
  AnyType args = AnyType();
  args << AnyType(han_state)
       << AnyType(in_y)
       << AnyType(vec_x)
       << AnyType();
  logregr_irls_step_transition udf;
  AnyType result = udf.run(args);

  EXTRACT_DOUBLE_ARRAY_H(out, result);

  std::cout << "out: ";
  for (int i = 0; i < outNum; ++i) {
    std:: cout << out[i] << ", ";
  }
  std::cout<< std::endl;

  std::cout << outNum;
  return 0;
}

int logregr_step_cg_transition_test() {
  double state[6] = {0, 0, 0, 0,0,0};
  bool in_y = true;
  double in_x[4] = {1,1,70,1};
  int num_x = 4;
  double pre_state[0];

  ArrayType *arr_state = madlib_construct_array_direct(state, 6, FLOAT8OID, 8, true, 'd');

  MutableArrayHandle<double> han_state = MutableArrayHandle<double>(arr_state);
  DEFINE_MCV(double, x);
  AnyType args = AnyType();
  args << AnyType(han_state)
       << AnyType(in_y)
       << AnyType(vec_x)
       << AnyType();
  logregr_cg_step_transition udf;
  AnyType result = udf.run(args);

  EXTRACT_DOUBLE_ARRAY_H(out, result);

  std::cout << "out: ";
  for (int i = 0; i < outNum; ++i) {
    std:: cout << out[i] << ", ";
  }
  std::cout<< std::endl;

  std::cout << outNum;
  return 0;
}

int logregr_step_igd_transition_test() {
  double state[5] = {0, 0, 0, 0,0};
  bool in_y = true;
  double in_x[3] = {1,1,70};
  int num_x = 3;
  double pre_state[0];

  ArrayType *arr_state = madlib_construct_array_direct(state, 5, FLOAT8OID, 8, true, 'd');

  MutableArrayHandle<double> han_state = MutableArrayHandle<double>(arr_state);
  DEFINE_MCV(double, x);
  AnyType args = AnyType();
  args << AnyType(han_state)
       << AnyType(in_y)
       << AnyType(vec_x)
       << AnyType();
  logregr_igd_step_transition udf;
  AnyType result = udf.run(args);

  EXTRACT_DOUBLE_ARRAY_H(out, result);

  std::cout << "out: ";
  for (int i = 0; i < outNum; ++i) {
    std:: cout << out[i] << ", ";
  }
  std::cout<< std::endl;

  std::cout << outNum;
  return 0;
}

int correlation_transition_test_no_state() {
  double in_x[3] = {1.0,2.0,3.0};
  double in_mean[3] = {3.0,2.0,1.0};
  int num_x = 3;
  int num_mean = 3;

  AnyType result = NULL;
  ArrayType* arr_state = NULL;

  AnyType args = AnyType();
  args << AnyType();
  DEFINE_MCV(double, x);
  DEFINE_MCV(double, mean);
  args << AnyType(vec_x)
       << AnyType(vec_mean);

  correlation_transition udf;
  result = udf.run(args);
  EXTRACT_2D_DOUBLE_ARRAY_H(out, result);
  for (int l = 0; l < outNum[0]; ++l) {
    for (int i = 0; i < outNum[1]; ++i) {
      std::cout<< out[l][i] << " ";
    }
  }
  PFREE_ARR(arr_state);
  return 0;
}

int correlation_transition_test() {
  double state[3][3] = {{0,0,0},{0,0,0},{0,0,0}};
  double in_x[3] = {1,2,3};
  double in_mean[3] = {3,2,1};
  int num_x = 3;
  int num_mean = 3;
  int num_state = 0;

  AnyType result = NULL;
  ArrayType* arr_state = NULL;

  Datum * elems = (Datum *)(malloc(sizeof(Datum) * 9));
  do {
    int i = 0;
    for (int j = 0; j < 3; ++j) {
      for (int k = 0; k < 3; ++k) {
        elems[i] = PointerGetDatum(state[j]+k);
        i++;
      }
    }
  } while (0);
  int dims[2] = {3,3};
  int lbs[2] = {0,0};

  arr_state = madlib_construct_md_array(elems, NULL, 2, dims, lbs, FLOAT8OID, 8, false, 'd');
  pfree(elems);
  MutableArrayHandle<double > han_state = MutableArrayHandle<double>(arr_state);

  AnyType args = AnyType();
  args << AnyType(han_state);
  DEFINE_MCV(double, x);
  DEFINE_MCV(double, mean);
  args << AnyType(vec_x)
       << AnyType(vec_mean);

  correlation_transition udf;
  result = udf.run(args);
  EXTRACT_2D_DOUBLE_ARRAY_H(out, result);
  for (int l = 0; l < outNum[0]; ++l) {
    for (int i = 0; i < outNum[1]; ++i) {
      std::cout<< out[l][i] << " ";
    }
  }
  PFREE_ARR(arr_state);
  return 0;
}

int correlation_final_test() {
  double state[3][3] = {{4,0,-4},{0,0,0},{-4,0,4}};


  AnyType result = NULL;
  ArrayType* arr_state = NULL;

  Datum * elems = (Datum *)(malloc(sizeof(Datum) * 9));
  do {
    int i = 0;
    for (int j = 0; j < 3; ++j) {
      for (int k = 0; k < 3; ++k) {
        elems[i] = PointerGetDatum(state[j]+k);
        i++;
      }
    }
  } while (0);
  int dims[2] = {3,3};
  int lbs[2] = {0,0};

  arr_state = madlib_construct_md_array(elems, NULL, 2, dims, lbs, FLOAT8OID, 8, false, 'd');
  pfree(elems);
  MutableArrayHandle<double > han_state = MutableArrayHandle<double>(arr_state);

  AnyType args = AnyType();
  args << AnyType(han_state);


  correlation_final udf;
  result = udf.run(args);
  EXTRACT_2D_DOUBLE_ARRAY_H(out, result);
  for (int l = 0; l < outNum[0]; ++l) {
    for (int i = 0; i < outNum[1]; ++i) {
      std::cout<< out[l][i] << " ";
    }
  }
  PFREE_ARR(arr_state);
  return 0;
}

int main() {
  return linregr_result_test();
}
