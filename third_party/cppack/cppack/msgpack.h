// MIT License
//
// Copyright (c) 2019 Mike Loomis, 2025 Google LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef CPPACK_MSGPACK_H_
#define CPPACK_MSGPACK_H_

#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <cmath>

namespace cppack {
enum class UnpackerError { OutOfRange = 1 };

struct UnpackerErrCategory final : public std::error_category {
  [[nodiscard]] const char* name() const noexcept override {
    return "unpacker";
  };

  [[nodiscard]] std::string message(int ev) const override;
};

const UnpackerErrCategory theUnpackerErrCategory{};

std::error_code make_error_code(cppack::UnpackerError e);
}  // namespace cppack

template <>
struct std::is_error_code_enum<cppack::UnpackerError> : true_type {
};  // namespace std

namespace cppack {

class Packer;
template <typename PackableType>
void PackStandalone(PackableType&& obj, Packer* packer);

class Unpacker;
template <typename PackableType>
absl::Status UnpackStandalone(PackableType&& obj, Unpacker* unpacker);

template <typename T, typename Enable = void>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

// template <typename T>
// constexpr bool is_optional(T const &) {
//   return false;
// }
// template <typename T>
// constexpr bool is_optional(std::optional<T> const &) {
//   return true;
// }

enum FormatConstants : uint8_t {
  // positive fixint = 0x00 - 0x7f
  // fixmap = 0x80 - 0x8f
  // fixarray = 0x90 - 0x9a
  // fixstr = 0xa0 - 0xbf
  // negative fixint = 0xe0 - 0xff

  nil = 0xc0,
  false_bool = 0xc2,
  true_bool = 0xc3,
  bin8 = 0xc4,
  bin16 = 0xc5,
  bin32 = 0xc6,
  ext8 = 0xc7,
  ext16 = 0xc8,
  ext32 = 0xc9,
  float32 = 0xca,
  float64 = 0xcb,
  uint8 = 0xcc,
  uint16 = 0xcd,
  uint32 = 0xce,
  uint64 = 0xcf,
  int8 = 0xd0,
  int16 = 0xd1,
  int32 = 0xd2,
  int64 = 0xd3,
  fixext1 = 0xd4,
  fixext2 = 0xd5,
  fixext4 = 0xd6,
  fixext8 = 0xd7,
  fixext16 = 0xd8,
  str8 = 0xd9,
  str16 = 0xda,
  str32 = 0xdb,
  array16 = 0xdc,
  array32 = 0xdd,
  map16 = 0xde,
  map32 = 0xdf
};

template <class T>
struct is_container {
  static constexpr bool value = false;
};

template <class T, class Alloc>
struct is_container<std::vector<T, Alloc>> {
  static constexpr bool value = true;
};

template <class T, class Alloc>
struct is_container<std::list<T, Alloc>> {
  static constexpr bool value = true;
};

template <class T, class Alloc>
struct is_container<std::map<T, Alloc>> {
  static constexpr bool value = true;
};

template <class T, class Alloc>
struct is_container<std::unordered_map<T, Alloc>> {
  static constexpr bool value = true;
};

template <class T, class Alloc>
struct is_container<std::set<T, Alloc>> {
  static constexpr bool value = true;
};

template <class T>
struct is_stdarray {
  static constexpr bool value = false;
};

template <class T, std::size_t N>
struct is_stdarray<std::array<T, N>> {
  static constexpr bool value = true;
};

template <class T>
struct is_map {
  static constexpr bool value = false;
};

template <class T, class Alloc>
struct is_map<std::map<T, Alloc>> {
  static constexpr bool value = true;
};

template <class T, class Alloc>
struct is_map<std::unordered_map<T, Alloc>> {
  static constexpr bool value = true;
};

class Packer {
 public:
  template <class... Types>
  void operator()(const Types&... args) {
    (pack_type(std::forward<const Types&>(args)), ...);
  }

  template <class... Types>
  void process(const Types&... args) {
    (pack_type(std::forward<const Types&>(args)), ...);
  }

  [[nodiscard]] const std::vector<uint8_t>& vector() const {
    return serialized_object;
  }

  void clear() { serialized_object.clear(); }

  template <class T>
  void pack_type(const T& value) {
    if constexpr (is_optional<T>::value) {
      if (value.has_value()) {
        pack_type(value.value());
      } else {
        serialized_object.emplace_back(nil);
      }
    } else if constexpr (is_map<T>::value) {
      pack_map(value);
    } else if constexpr (is_container<T>::value || is_stdarray<T>::value) {
      pack_array(value);
    } else {
      auto recursive_packer = Packer{};
      PackStandalone(value, &recursive_packer);
      pack_type(recursive_packer.vector());
    }
  }

  // template <class T>
  // void pack_type(const std::chrono::time_point<T> &value) {
  //   pack_type(value.time_since_epoch().count());
  // }

 private:
  std::vector<uint8_t> serialized_object;

  template <class T>
  void pack_array(const T& array) {
    if (array.size() < 16) {
      auto size_mask = static_cast<uint8_t>(0b10010000);
      serialized_object.emplace_back(
          static_cast<uint8_t>(array.size() | size_mask));
    } else if (array.size() < std::numeric_limits<uint16_t>::max()) {
      serialized_object.emplace_back(array16);
      for (auto i = sizeof(uint16_t); i > 0; --i) {
        serialized_object.emplace_back(
            static_cast<uint8_t>(array.size() >> (8U * (i - 1)) & 0xff));
      }
    } else if (array.size() < std::numeric_limits<uint32_t>::max()) {
      serialized_object.emplace_back(array32);
      for (auto i = sizeof(uint32_t); i > 0; --i) {
        serialized_object.emplace_back(
            static_cast<uint8_t>(array.size() >> (8U * (i - 1)) & 0xff));
      }
    } else {
      return;  // Give up if string is too long
    }
    for (const auto& elem : array) {
      pack_type(elem);
    }
  }

  template <class T>
  void pack_map(const T& map) {
    if (map.size() < 16) {
      auto size_mask = static_cast<uint8_t>(0b10000000);
      serialized_object.emplace_back(
          static_cast<uint8_t>(map.size() | size_mask));
    } else if (map.size() < std::numeric_limits<uint16_t>::max()) {
      serialized_object.emplace_back(map16);
      for (auto i = sizeof(uint16_t); i > 0; --i) {
        serialized_object.emplace_back(
            static_cast<uint8_t>(map.size() >> (8U * (i - 1)) & 0xff));
      }
    } else if (map.size() < std::numeric_limits<uint32_t>::max()) {
      serialized_object.emplace_back(map32);
      for (auto i = sizeof(uint32_t); i > 0; --i) {
        serialized_object.emplace_back(
            static_cast<uint8_t>(map.size() >> (8U * (i - 1)) & 0xff));
      }
    }
    for (const auto& elem : map) {
      pack_type(std::get<0>(elem));
      pack_type(std::get<1>(elem));
    }
  }

  static std::bitset<64> twos_complement(int64_t value);

  static std::bitset<32> twos_complement(int32_t value);

  static std::bitset<16> twos_complement(int16_t value);

  static std::bitset<8> twos_complement(int8_t value);
};

template <>
void Packer::pack_type(const int8_t& value);

template <>
inline void Packer::pack_type(const int16_t& value) {
  if (abs(value) < abs(std::numeric_limits<int8_t>::min())) {
    pack_type(static_cast<int8_t>(value));
  } else {
    serialized_object.emplace_back(int16);
    auto serialize_value =
        static_cast<uint16_t>(twos_complement(value).to_ulong());
    for (auto i = sizeof(value); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(serialize_value >> (8U * (i - 1)) & 0xff));
    }
  }
}

template <>
inline void Packer::pack_type(const int32_t& value) {
  if (abs(value) < abs(std::numeric_limits<int16_t>::min())) {
    pack_type(static_cast<int16_t>(value));
  } else {
    serialized_object.emplace_back(int32);
    auto serialize_value =
        static_cast<uint32_t>(twos_complement(value).to_ulong());
    for (auto i = sizeof(value); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(serialize_value >> (8U * (i - 1)) & 0xff));
    }
  }
}

template <>
inline void Packer::pack_type(const int64_t& value) {
  if (llabs(value) < llabs(std::numeric_limits<int32_t>::min()) &&
      value != std::numeric_limits<int64_t>::min()) {
    pack_type(static_cast<int32_t>(value));
  } else {
    serialized_object.emplace_back(int64);
    auto serialize_value = twos_complement(value).to_ullong();
    for (auto i = sizeof(value); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(serialize_value >> (8U * (i - 1)) & 0xff));
    }
  }
}

template <>
inline void Packer::pack_type(const uint8_t& value) {
  if (value <= 0x7f) {
    serialized_object.emplace_back(value);
  } else {
    serialized_object.emplace_back(uint8);
    serialized_object.emplace_back(value);
  }
}

template <>
inline void Packer::pack_type(const uint16_t& value) {
  if (value > std::numeric_limits<uint8_t>::max()) {
    serialized_object.emplace_back(uint16);
    for (auto i = sizeof(value); i > 0U; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value >> (8U * (i - 1)) & 0xff));
    }
  } else {
    pack_type(static_cast<uint8_t>(value));
  }
}

template <>
inline void Packer::pack_type(const uint32_t& value) {
  if (value > std::numeric_limits<uint16_t>::max()) {
    serialized_object.emplace_back(uint32);
    for (auto i = sizeof(value); i > 0U; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value >> (8U * (i - 1)) & 0xff));
    }
  } else {
    pack_type(static_cast<uint16_t>(value));
  }
}

template <>
inline void Packer::pack_type(const uint64_t& value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    serialized_object.emplace_back(uint64);
    for (auto i = sizeof(value); i > 0U; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value >> (8U * (i - 1)) & 0xff));
    }
  } else {
    pack_type(static_cast<uint32_t>(value));
  }
}

template <>
inline void Packer::pack_type(const std::nullptr_t& /*value*/) {
  serialized_object.emplace_back(nil);
}

template <>
inline void Packer::pack_type(const bool& value) {
  if (value) {
    serialized_object.emplace_back(true_bool);
  } else {
    serialized_object.emplace_back(false_bool);
  }
}

template <typename T>
absl::InlinedVector<uint8_t, 9> ToBigEndianBytes(const T& value,
                                                 uint8_t pad = 0) {
  auto value_ptr = reinterpret_cast<const uint8_t*>(&value);
  absl::InlinedVector<uint8_t, 9> bytes(sizeof(T) + pad);
  if constexpr (std::endian::native == std::endian::little) {
    for (size_t i = 0; i < sizeof(T); ++i) {
      bytes[i + pad] = value_ptr[sizeof(T) - 1 - i];
    }
  } else {
    for (size_t i = 0; i < sizeof(T); ++i) {
      bytes[i + pad] = value_ptr[i];
    }
  }
  return bytes;
}

template <typename T>
T FromBigEndianBytes(const uint8_t* absl_nonnull bytes) {
  if constexpr (std::endian::native == std::endian::big) {
    return *reinterpret_cast<T*>(bytes);
  }
  absl::InlinedVector<uint8_t, sizeof(T)> reversed_bytes(sizeof(T));
  for (size_t i = 0; i < sizeof(T); ++i) {
    reversed_bytes[i] = bytes[sizeof(T) - 1 - i];
  }
  return *reinterpret_cast<T*>(reversed_bytes.data());
}

template <>
inline void Packer::pack_type(const float& value) {
  if constexpr (!std::numeric_limits<float>::is_iec559) {
    // Crash OK: Non-IEC 559 floats are not supported and it should be treated
    // as a platform restriction, not a runtime error.
    LOG(FATAL) << "Non-IEC 559 floats are not supported";
    ABSL_ASSUME(false);
  }
  auto result = ToBigEndianBytes<float>(value, /*pad=*/1);
  result[0] = float32;
  serialized_object.insert(serialized_object.end(), result.begin(),
                           result.end());
}

template <>
inline void Packer::pack_type(const double& value) {
  if constexpr (!std::numeric_limits<double>::is_iec559) {
    // Crash OK: Non-IEC 559 doubles are not supported and it should be treated
    // as a platform restriction, not a runtime error.
    LOG(FATAL) << "Non-IEC 559 doubles are not supported";
    ABSL_ASSUME(false);
  }
  auto result = ToBigEndianBytes<double>(value, /*pad=*/1);
  result[0] = float64;
  serialized_object.insert(serialized_object.end(), result.begin(),
                           result.end());
}

template <>
inline void Packer::pack_type(const std::string& value) {
  if (value.size() < 32) {
    serialized_object.emplace_back(static_cast<uint8_t>(value.size()) |
                                   0b10100000);
  } else if (value.size() < std::numeric_limits<uint8_t>::max()) {
    serialized_object.emplace_back(str8);
    serialized_object.emplace_back(static_cast<uint8_t>(value.size()));
  } else if (value.size() < std::numeric_limits<uint16_t>::max()) {
    serialized_object.emplace_back(str16);
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value.size() >> (8U * (i - 1)) & 0xff));
    }
  } else if (value.size() < std::numeric_limits<uint32_t>::max()) {
    serialized_object.emplace_back(str32);
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value.size() >> (8U * (i - 1)) & 0xff));
    }
  } else {
    return;  // Give up if string is too long
  }
  for (char i : value) {
    serialized_object.emplace_back(static_cast<uint8_t>(i));
  }
}

template <>
inline void Packer::pack_type(const std::vector<uint8_t>& value) {
  if (value.size() < std::numeric_limits<uint8_t>::max()) {
    serialized_object.emplace_back(bin8);
    serialized_object.emplace_back(static_cast<uint8_t>(value.size()));
  } else if (value.size() < std::numeric_limits<uint16_t>::max()) {
    serialized_object.emplace_back(bin16);
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value.size() >> (8U * (i - 1)) & 0xff));
    }
  } else if (value.size() < std::numeric_limits<uint32_t>::max()) {
    serialized_object.emplace_back(bin32);
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      serialized_object.emplace_back(
          static_cast<uint8_t>(value.size() >> (8U * (i - 1)) & 0xff));
    }
  } else {
    return;  // Give up if vector is too large
  }
  for (const auto& elem : value) {
    serialized_object.emplace_back(elem);
  }
}

class Unpacker {
 public:
  Unpacker() : data_pointer_(nullptr), data_end(nullptr) {};

  Unpacker(const uint8_t* data_start, std::size_t bytes);
  ;

  template <class... Types>
  void operator()(Types&... args) {
    (unpack_type(std::forward<Types&>(args)), ...);
  }

  template <class... Types>
  void process(Types&... args) {
    (unpack_type(std::forward<Types&>(args)), ...);
  }

  void set_data(const uint8_t* pointer, std::size_t size);

  std::error_code GetErrorCode() const;

  const uint8_t* GetDataPtr() const;

 private:
  const uint8_t* data_pointer_;
  const uint8_t* data_end;
  mutable std::error_code error_code_{};

  uint8_t safe_data() const;

  void safe_increment(int64_t bytes = 1);

  template <class T>
  void unpack_type(T& value) {
    if constexpr (is_optional<T>::value) {
      if (safe_data() == nil) {
        value = std::nullopt;
        safe_increment();
      } else {
        value.emplace();
        unpack_type(*value);
      }
    } else if constexpr (is_map<T>::value) {
      unpack_map(value);
    } else if constexpr (is_container<T>::value) {
      unpack_array(value);
    } else if constexpr (is_stdarray<T>::value) {
      unpack_stdarray(value);
    } else {
      auto recursive_data = std::vector<uint8_t>{};
      unpack_type(recursive_data);
      auto recursive_unpacker =
          Unpacker{recursive_data.data(), recursive_data.size()};
      absl::Status status = UnpackStandalone(value, &recursive_unpacker);
      error_code_ = recursive_unpacker.GetErrorCode();
      if (!status.ok()) {
        error_code_ = std::make_error_code(std::errc::invalid_argument);
        error_code_.message() = std::string(status.message());
      }
    }
  }

  // template <class Clock, class Duration>
  // void unpack_type(std::chrono::time_point<Clock, Duration> &value) {
  //   using RepType = typename std::chrono::time_point<Clock, Duration>::rep;
  //   using DurationType = Duration;
  //   using TimepointType = typename std::chrono::time_point<Clock, Duration>;
  //   auto placeholder = RepType{};
  //   unpack_type(placeholder);
  //   value = TimepointType(DurationType(placeholder));
  // }

  template <class T>
  void unpack_array(T& array) {
    using ValueType = typename T::value_type;
    if (safe_data() == array32) {
      safe_increment();
      std::size_t array_size = 0;
      for (auto i = sizeof(uint32_t); i > 0; --i) {
        array_size += static_cast<uint32_t>(safe_data()) << 8 * (i - 1);
        safe_increment();
      }
      std::vector<uint32_t> x{};
      for (auto i = 0U; i < array_size; ++i) {
        ValueType val{};
        unpack_type(val);
        array.emplace_back(val);
      }
    } else if (safe_data() == array16) {
      safe_increment();
      std::size_t array_size = 0;
      for (auto i = sizeof(uint16_t); i > 0; --i) {
        array_size += static_cast<uint16_t>(safe_data()) << 8 * (i - 1);
        safe_increment();
      }
      for (auto i = 0U; i < array_size; ++i) {
        ValueType val{};
        unpack_type(val);
        array.emplace_back(val);
      }
    } else {
      std::size_t array_size = safe_data() & 0b00001111;
      safe_increment();
      for (auto i = 0U; i < array_size; ++i) {
        ValueType val{};
        unpack_type(val);
        array.emplace_back(val);
      }
    }
  }

  template <class T>
  void unpack_stdarray(T& array) {
    using ValueType = typename T::value_type;
    auto vec = std::vector<ValueType>{};
    unpack_array(vec);
    std::copy(vec.begin(), vec.end(), array.begin());
  }

  template <class T>
  void unpack_map(T& map) {
    using KeyType = typename T::key_type;
    using MappedType = typename T::mapped_type;
    if (safe_data() == map32) {
      safe_increment();
      std::size_t map_size = 0;
      for (auto i = sizeof(uint32_t); i > 0; --i) {
        map_size += static_cast<uint32_t>(safe_data()) << 8 * (i - 1);
        safe_increment();
      }
      std::vector<uint32_t> x{};
      for (auto i = 0U; i < map_size; ++i) {
        KeyType key{};
        MappedType value{};
        unpack_type(key);
        unpack_type(value);
        map.insert_or_assign(key, value);
      }
    } else if (safe_data() == map16) {
      safe_increment();
      std::size_t map_size = 0;
      for (auto i = sizeof(uint16_t); i > 0; --i) {
        map_size += static_cast<uint16_t>(safe_data()) << 8 * (i - 1);
        safe_increment();
      }
      for (auto i = 0U; i < map_size; ++i) {
        KeyType key{};
        MappedType value{};
        unpack_type(key);
        unpack_type(value);
        map.insert_or_assign(key, value);
      }
    } else {
      std::size_t map_size = safe_data() & 0b00001111;
      safe_increment();
      for (auto i = 0U; i < map_size; ++i) {
        KeyType key{};
        MappedType value{};
        unpack_type(key);
        unpack_type(value);
        map.insert_or_assign(key, value);
      }
    }
  }
};

template <>
inline void Unpacker::unpack_type(int8_t& value) {
  if (safe_data() == int8) {
    safe_increment();
    value = safe_data();
    safe_increment();
  } else {
    value = safe_data();
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(int16_t& value) {
  if (safe_data() == int16) {
    safe_increment();
    std::bitset<16> bits;
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      bits |= static_cast<uint16_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
    if (bits[15]) {
      value = -1 * (static_cast<uint16_t>((~bits).to_ulong()) + 1);
    } else {
      value = static_cast<uint16_t>(bits.to_ulong());
    }
  } else if (safe_data() == int8) {
    int8_t val;
    unpack_type(val);
    value = val;
  } else {
    value = static_cast<int8_t>(safe_data());
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(int32_t& value) {
  if (safe_data() == int32) {
    safe_increment();
    std::bitset<32> bits;
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      bits |= static_cast<uint32_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
    if (bits[31]) {
      value = -1 * ((~bits).to_ulong() + 1);
    } else {
      value = bits.to_ulong();
    }
  } else if (safe_data() == int16) {
    int16_t val;
    unpack_type(val);
    value = val;
  } else if (safe_data() == int8) {
    int8_t val;
    unpack_type(val);
    value = val;
  } else {
    value = static_cast<int8_t>(safe_data());
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(int64_t& value) {
  if (safe_data() == int64) {
    safe_increment();
    std::bitset<64> bits;
    for (auto i = sizeof(value); i > 0; --i) {
      bits |= std::bitset<8>(safe_data()).to_ullong() << 8 * (i - 1);
      safe_increment();
    }
    if (bits[63]) {
      value = -1 * ((~bits).to_ullong() + 1);
    } else {
      value = bits.to_ullong();
    }
  } else if (safe_data() == int32) {
    int32_t val;
    unpack_type(val);
    value = val;
  } else if (safe_data() == int16) {
    int16_t val;
    unpack_type(val);
    value = val;
  } else if (safe_data() == int8) {
    int8_t val;
    unpack_type(val);
    value = val;
  } else {
    value = safe_data();
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(uint8_t& value) {
  if (safe_data() == uint8) {
    safe_increment();
    value = safe_data();
    safe_increment();
  } else {
    value = safe_data();
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(uint16_t& value) {
  if (safe_data() == uint16) {
    safe_increment();
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      value += safe_data() << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == uint8) {
    safe_increment();
    value = safe_data();
    safe_increment();
  } else {
    value = safe_data();
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(uint32_t& value) {
  if (safe_data() == uint32) {
    safe_increment();
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      value += safe_data() << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == uint16) {
    safe_increment();
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      value += safe_data() << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == uint8) {
    safe_increment();
    value = safe_data();
    safe_increment();
  } else {
    value = safe_data();
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(uint64_t& value) {
  if (safe_data() == uint64) {
    safe_increment();
    for (auto i = sizeof(uint64_t); i > 0; --i) {
      value += static_cast<uint64_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == uint32) {
    safe_increment();
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      value += static_cast<uint64_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
    data_pointer_++;
  } else if (safe_data() == uint16) {
    safe_increment();
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      value += static_cast<uint64_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == uint8) {
    safe_increment();
    value = safe_data();
    safe_increment();
  } else {
    value = safe_data();
    safe_increment();
  }
}

template <>
inline void Unpacker::unpack_type(std::nullptr_t& /*value*/) {
  safe_increment();
}

template <>
inline void Unpacker::unpack_type(bool& value) {
  value = safe_data() != 0xc2;
  safe_increment();
}

template <>
inline void Unpacker::unpack_type(float& value) {
  const auto type = static_cast<FormatConstants>(safe_data());
  safe_increment();

  if (type == float32) {
    if (data_end - data_pointer_ < 4) {
      error_code_ = UnpackerError::OutOfRange;
      return;
    }
    value = FromBigEndianBytes<float>(data_pointer_);
    safe_increment(4);
    return;
  }

  if (type == int8 || type == int16 || type == int32 || type == int64) {
    int64_t val = 0;
    unpack_type(val);
    value = static_cast<float>(val);
    return;
  }

  if (type == uint8 || type == uint16 || type == uint32 || type == uint64) {
    int64_t val = 0;
    unpack_type(val);
    value = static_cast<float>(val);
    return;
  }
  error_code_ = UnpackerError::OutOfRange;
}

template <>
inline void Unpacker::unpack_type(double& value) {
  const auto type = static_cast<FormatConstants>(safe_data());
  safe_increment();

  if (type == float32) {
    if (data_end - data_pointer_ < 4) {
      error_code_ = UnpackerError::OutOfRange;
      return;
    }
    value = FromBigEndianBytes<float>(data_pointer_);
    safe_increment(4);
    return;
  }

  if (type == float64) {
    if (data_end - data_pointer_ < 8) {
      error_code_ = UnpackerError::OutOfRange;
      return;
    }
    value = FromBigEndianBytes<double>(data_pointer_);
    safe_increment(8);
    return;
  }

  if (type == int8 || type == int16 || type == int32 || type == int64) {
    int64_t val = 0;
    unpack_type(val);
    value = static_cast<double>(val);
    return;
  }

  if (type == uint8 || type == uint16 || type == uint32 || type == uint64) {
    int64_t val = 0;
    unpack_type(val);
    value = static_cast<double>(val);
    return;
  }
  error_code_ = UnpackerError::OutOfRange;
}

template <>
inline void Unpacker::unpack_type(std::string& value) {
  std::size_t str_size = 0;
  if (safe_data() == str32) {
    safe_increment();
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      str_size += static_cast<uint32_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == str16) {
    safe_increment();
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      str_size += static_cast<uint16_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == str8) {
    safe_increment();
    for (auto i = sizeof(uint8_t); i > 0; --i) {
      str_size += static_cast<uint8_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else {
    str_size = safe_data() & 0b00011111;
    safe_increment();
  }
  if (data_pointer_ + str_size <= data_end) {
    value = std::string{data_pointer_, data_pointer_ + str_size};
    safe_increment(str_size);
  } else {
    error_code_ = UnpackerError::OutOfRange;
  }
}

template <>
inline void Unpacker::unpack_type(std::vector<uint8_t>& value) {
  std::size_t bin_size = 0;
  if (safe_data() == bin32) {
    safe_increment();
    for (auto i = sizeof(uint32_t); i > 0; --i) {
      bin_size += static_cast<uint32_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else if (safe_data() == bin16) {
    safe_increment();
    for (auto i = sizeof(uint16_t); i > 0; --i) {
      bin_size += static_cast<uint16_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  } else {
    safe_increment();
    for (auto i = sizeof(uint8_t); i > 0; --i) {
      bin_size += static_cast<uint8_t>(safe_data()) << 8 * (i - 1);
      safe_increment();
    }
  }
  if (data_pointer_ + bin_size <= data_end) {
    value = std::vector<uint8_t>{data_pointer_, data_pointer_ + bin_size};
    safe_increment(bin_size);
  } else {
    error_code_ = UnpackerError::OutOfRange;
  }
}

template <class PackableObject>
std::vector<uint8_t> pack(PackableObject& obj) {
  auto packer = Packer{};
  PackStandalone(obj, &packer);
  return packer.vector();
}

template <class PackableObject>
std::vector<uint8_t> pack(PackableObject&& obj) {
  auto packer = Packer{};
  PackStandalone(std::forward<PackableObject>(obj), &packer);
  return packer.vector();
}

template <class UnpackableObject>
UnpackableObject unpack(const uint8_t* data_start, const std::size_t size,
                        std::error_code& ec) {
  auto obj = UnpackableObject{};
  auto unpacker = Unpacker(data_start, size);
  UnpackStandalone(obj, &unpacker);
  ec = unpacker.GetErrorCode();
  return obj;
}

template <class UnpackableObject>
UnpackableObject unpack(const uint8_t* data_start, const std::size_t size) {
  std::error_code error_code{};
  return unpack<UnpackableObject>(data_start, size, error_code);
}

template <class UnpackableObject>
UnpackableObject unpack(const std::vector<uint8_t>& data, std::error_code& ec) {
  return unpack<UnpackableObject>(data.data(), data.size(), ec);
}

template <class UnpackableObject>
UnpackableObject unpack(const std::vector<uint8_t>& data) {
  std::error_code ec;
  return unpack<UnpackableObject>(data.data(), data.size(), ec);
}

template <typename PackableType>
void PackStandalone(PackableType&& obj, Packer* packer) {
  CppackToBytes(std::forward<PackableType>(obj), *packer).IgnoreError();
}

template <typename PackableType>
absl::Status UnpackStandalone(PackableType&& obj, Unpacker* unpacker) {
  return CppackFromBytes(std::forward<PackableType>(obj), *unpacker);
}

template <typename PackableType>
std::vector<uint8_t> Pack(PackableType&& obj) {
  Packer packer;
  packer.process(std::forward<PackableType>(obj));
  return packer.vector();
}

template <typename PackableType>
absl::StatusOr<PackableType> Unpack(const std::vector<uint8_t>& data) {
  PackableType obj;

  Unpacker unpacker;
  unpacker.set_data(data.data(), data.size());
  unpacker.process(obj);

  const std::error_code ec = unpacker.GetErrorCode();
  if (!ec) {
    return obj;
  }

  return absl::InternalError(ec.message());
}

}  // namespace cppack

#endif  // CPPACK_MSGPACK_H_