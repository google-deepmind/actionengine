#include "eg_structs.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include <eglt/absl_headers.h>

namespace eglt::base {

std::vector<std::string> Indent(std::vector<std::string> fields,
                                int indentation, bool indent_first_line) {
  if (fields.empty()) {
    return fields;
  }

  std::vector<std::string> result = std::move(fields);
  size_t start_index = indent_first_line ? 0 : 1;

  for (size_t index = start_index; index < result.size(); ++index) {
    result[index] = absl::StrCat(std::string(indentation, ' '), result[index]);
  }

  return result;
}

std::string Indent(std::string field, int indentation, bool indent_first_line) {
  std::vector<std::string> lines = Indent(
      absl::StrSplit(std::move(field), '\n'), indentation, indent_first_line);

  return absl::StrJoin(lines,
                       "\n",
                       [](std::string* out, std::string_view line) {
                         absl::StrAppend(out, line);
                       });
}

bool IsNullChunk(const Chunk& chunk) {
  return chunk.metadata.mimetype == "application/octet-stream" &&
      chunk.data.empty();
}

} // namespace eglt::base

