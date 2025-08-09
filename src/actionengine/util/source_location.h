#ifndef ACTIONENGINE_UTIL_SOURCE_LOCATION_H_
#define ACTIONENGINE_UTIL_SOURCE_LOCATION_H_

#include <cstdint>

namespace act::util {

// Class representing a specific location in the source code of a program.
// source_location is copyable.
class source_location {
 public:
  // Avoid this constructor; it populates the object with dummy values.
  constexpr source_location() : line_(0), file_name_(nullptr) {}

  // Wrapper to invoke the private constructor below. This should only be
  // used by the ACTIONENGINE_LOC macro, hence the name.
  static constexpr source_location DoNotInvokeDirectly(std::uint_least32_t line,
                                                       const char* file_name) {
    return source_location(line, file_name);
  }

  // The line number of the captured source location.
  constexpr std::uint_least32_t line() const { return line_; }

  // The file name of the captured source location.
  constexpr const char* file_name() const { return file_name_; }

  // column() and function_name() are omitted because we don't have a
  // way to support them.

 private:
  // Do not invoke this constructor directly. Instead, use the
  // ACTIONENGINE_LOC macro below.
  //
  // file_name must outlive all copies of the source_location
  // object, so in practice it should be a string literal.
  constexpr source_location(std::uint_least32_t line, const char* file_name)
      : line_(line), file_name_(file_name) {}

  std::uint_least32_t line_;
  const char* file_name_;
};

}  // namespace act::util

// If a function takes a source_location parameter, pass this as the argument.
#define ACTIONENGINE_LOC \
  act::util::source_location::DoNotInvokeDirectly(__LINE__, __FILE__)

#endif  // ACTIONENGINE_UTIL_SOURCE_LOCATION_H_