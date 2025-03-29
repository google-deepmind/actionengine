#ifndef EGLT_UTIL_RANDOM_H_
#define EGLT_UTIL_RANDOM_H_

#include <string>

#include "eglt/absl_headers.h"

namespace eglt {

// TODO: Check this for correctness
inline std::string GenerateUUID4() {
  absl::BitGen gen;  // Abseil random number generator

  // Generate 16 random bytes
  std::array<uint8_t, 16> bytes{};
  for (int i = 0; i < 16; ++i) {
    bytes[i] = absl::Uniform<int>(gen, 0, 256);  // Generate random byte (0-255)
  }

  // Set the version (UUID version 4) - The 7th nibble (most significant nibble of 13th byte) must be 4
  bytes[6] = (bytes[6] & 0x0f) | 0x40;

  // Set the variant (the 9th nibble must be 8, 9, A, or B)
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  // Convert to string (UUID format: 8-4-4-4-12)
  std::ostringstream oss;
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      oss << "-";
    }
    oss << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<int>(bytes[i]);
  }

  return oss.str();
}

} // namespace eglt

#endif // EGLT_UTIL_RANDOM_H_