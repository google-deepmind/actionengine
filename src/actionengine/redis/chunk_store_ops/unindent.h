#ifndef ACTIONENGINE_REDIS_CHUNK_STORE_OPS_UNINDENT_H_
#define ACTIONENGINE_REDIS_CHUNK_STORE_OPS_UNINDENT_H_

// Taken from: https://stackoverflow.com/a/75105367

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

namespace multiline_raw_string {
template <class char_type>
using string_view = std::basic_string_view<char_type>;

// characters that are considered space
// we need this because std::isspace is not constexpr
template <class char_type>
constexpr string_view<char_type> space_chars =
    std::declval<string_view<char_type>>();
template <>
constexpr inline string_view<char> space_chars<char> = " \f\n\r\t\v";
template <>
constexpr inline string_view<wchar_t> space_chars<wchar_t> = L" \f\n\r\t\v";
template <>
constexpr inline string_view<char8_t> space_chars<char8_t> = u8" \f\n\r\t\v";
template <>
constexpr inline string_view<char16_t> space_chars<char16_t> = u" \f\n\r\t\v";
template <>
constexpr inline string_view<char32_t> space_chars<char32_t> = U" \f\n\r\t\v";

// list of all potential line endings that could be encountered
template <class char_type>
constexpr string_view<char_type> potential_line_endings[] =
    std::declval<string_view<char_type>[]>();

template <>
constexpr inline string_view<char> potential_line_endings<char>[] = {
    "\r\n", "\r", "\n"};
template <>
constexpr inline string_view<wchar_t> potential_line_endings<wchar_t>[] = {
    L"\r\n", L"\r", L"\n"};
template <>
constexpr inline string_view<char8_t> potential_line_endings<char8_t>[] = {
    u8"\r\n", u8"\r", u8"\n"};
template <>
constexpr inline string_view<char16_t> potential_line_endings<char16_t>[] = {
    u"\r\n", u"\r", u"\n"};
template <>
constexpr inline string_view<char32_t> potential_line_endings<char32_t>[] = {
    U"\r\n", U"\r", U"\n"};

// null-terminator for the different character types
template <class char_type>
constexpr char_type null_char = std::declval<char_type>();
template <>
constexpr inline char null_char<char> = '\0';
template <>
constexpr inline wchar_t null_char<wchar_t> = L'\0';
template <>
constexpr inline char8_t null_char<char8_t> = static_cast<char8_t>('\0');
template <>
constexpr inline char16_t null_char<char16_t> = u'\0';
template <>
constexpr inline char32_t null_char<char32_t> = U'\0';

// detects the line ending used within a string.
// e.g. detect_line_ending("foo\nbar\nbaz") -> "\n"
template <class char_type>
consteval string_view<char_type> detect_line_ending(
    string_view<char_type> str) {
  return *std::ranges::max_element(potential_line_endings<char_type>, {},
                                   [str](string_view<char_type> line_ending) {
                                     // count the number of lines we would get with line_ending
                                     auto view =
                                         std::views::split(str, line_ending);
                                     return std::ranges::distance(view);
                                   });
}

// returns a view to the leading sequence of space characters within a string
// e.g. get_leading_space_sequence(" \t  foo") -> " \t  "
template <class char_type>
consteval string_view<char_type> get_leading_space_sequence(
    string_view<char_type> line) {
  return line.substr(0, line.find_first_not_of(space_chars<char_type>));
}

// checks if a line consists purely out of space characters
// e.g. is_line_empty("    \t") -> true
//      is_line_empty("   foo") -> false
template <class char_type>
consteval bool is_line_empty(string_view<char_type> line) {
  return get_leading_space_sequence(line).size() == line.size();
}

// splits a string into individual lines
// and removes the first & last line if they are empty
// e.g. split_lines("\na\nb\nc\n", "\n") -> {"a", "b", "c"}
template <class char_type>
consteval std::vector<string_view<char_type>> split_lines(
    string_view<char_type> str, string_view<char_type> line_ending) {
  std::vector<string_view<char_type>> lines;

  for (auto line : std::views::split(str, line_ending)) {
    lines.emplace_back(line.begin(), line.end());
  }

  // remove first/last lines in case they are completely empty
  if (lines.size() > 1 && is_line_empty(lines[0])) {
    lines.erase(lines.begin());
  }
  if (lines.size() > 1 && is_line_empty(lines[lines.size() - 1])) {
    lines.erase(lines.end() - 1);
  }

  return lines;
}

// determines the longest possible sequence of space characters
// that we can remove from each line.
// e.g. determine_common_space_prefix_sequence({" \ta", " foo", " \t\tbar"}) -> " "
template <class char_type>
consteval string_view<char_type> determine_common_space_prefix_sequence(
    std::vector<string_view<char_type>> const& lines) {
  std::vector<string_view<char_type>> space_sequences = {
      string_view<char_type>{}  // empty string
  };

  for (string_view<char_type> line : lines) {
    string_view<char_type> spaces = get_leading_space_sequence(line);
    for (std::size_t len = 1; len <= spaces.size(); len++) {
      space_sequences.emplace_back(spaces.substr(0, len));
    }

    // remove duplicates
    std::ranges::sort(space_sequences);
    auto [first, last] = std::ranges::unique(space_sequences);
    space_sequences.erase(first, last);
  }

  // only consider space prefix sequences that apply to all lines
  // (ignoring completely blank lines)
  auto shared_prefixes = std::views::filter(
      space_sequences, [&lines](string_view<char_type> prefix) {
        return std::ranges::all_of(
            lines, [&prefix](string_view<char_type> line) {
              return line.empty() || line.starts_with(prefix);
            });
      });

  // select the longest possible space prefix sequence
  return *std::ranges::max_element(shared_prefixes, {},
                                   &string_view<char_type>::size);
}

// unindents the individual lines of a raw string literal
// e.g. unindent_string("  \n  a\n  b\n  c\n") -> "a\nb\nc"
template <class char_type>
consteval std::vector<char_type> unindent_string(string_view<char_type> str) {
  string_view<char_type> line_ending = detect_line_ending(str);
  std::vector<string_view<char_type>> lines = split_lines(str, line_ending);
  string_view<char_type> common_space_sequence =
      determine_common_space_prefix_sequence(lines);

  std::vector<char_type> new_string;
  bool is_first = true;
  for (auto line : lines) {
    // append newline
    if (is_first) {
      is_first = false;
    } else {
      new_string.insert(new_string.end(), line_ending.begin(),
                        line_ending.end());
    }

    // append unindented line
    if (line.empty()) {
      continue;
    }
    auto unindented = line.substr(common_space_sequence.size());
    new_string.insert(new_string.end(), unindented.begin(), unindented.end());
  }

  // add null terminator
  new_string.push_back(null_char<char_type>);

  return new_string;
}

// returns the size required for the unindented string
template <class char_type>
consteval std::size_t unindent_string_size(string_view<char_type> str) {
  return unindent_string(str).size();
}

// simple type that stores a raw string
// we need this to get around the limitation that string literals
// are not considered valid non-type template arguments.
template <class _char_type, std::size_t size>
struct string_wrapper {
  using char_type = _char_type;

  consteval string_wrapper(const char_type (&arr)[size]) {
    std::ranges::copy(arr, str);
  }

  char_type str[size];
};

// used for sneakily creating and storing
// the unindented string in a template parameter.
template <string_wrapper sw>
struct unindented_string_wrapper {
  using char_type = typename decltype(sw)::char_type;
  static constexpr std::size_t buffer_size =
      unindent_string_size<char_type>(sw.str);
  using array_ref = const char_type (&)[buffer_size];

  consteval unindented_string_wrapper(int) {
    auto newstr = unindent_string<char_type>(sw.str);
    std::ranges::copy(newstr, buffer);
  }

  consteval array_ref get() const { return buffer; }

  char_type buffer[buffer_size];
};

// uses a defaulted template argument that depends on the str
// to initialize the unindented string within a template parameter.
// this enables us to return a reference to the unindented string.
template <string_wrapper str, unindented_string_wrapper<str> unindented = 0>
consteval decltype(auto) do_unindent() {
  return unindented.get();
}

// the actual user-defined string literal operator
template <string_wrapper str>
consteval decltype(auto) operator"" _unindent() {
  return do_unindent<str>();
}
}  // namespace multiline_raw_string

using multiline_raw_string::operator"" _unindent;

#endif  // ACTIONENGINE_REDIS_CHUNK_STORE_OPS_UNINDENT_H_