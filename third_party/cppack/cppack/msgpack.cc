#include "cppack/msgpack.h"

namespace cppack {
std::string UnpackerErrCategory::message(int ev) const {
  switch (static_cast<cppack::UnpackerError>(ev)) {
    case cppack::UnpackerError::OutOfRange:
      return "tried to dereference out of range during deserialization";
    default:
      return "(unrecognized error)";
  }
}

std::error_code make_error_code(cppack::UnpackerError e) {
  return {static_cast<int>(e), theUnpackerErrCategory};
}

std::bitset<64> Packer::twos_complement(int64_t value) {
  if (value < 0) {
    const auto abs_v = llabs(value);
    return ~abs_v + 1;
  }
  return {static_cast<uint64_t>(value)};
}

std::bitset<32> Packer::twos_complement(int32_t value) {
  if (value < 0) {
    const auto abs_v = abs(value);
    return ~abs_v + 1;
  }
  return {static_cast<uint32_t>(value)};
}

std::bitset<16> Packer::twos_complement(int16_t value) {
  if (value < 0) {
    const auto abs_v = abs(value);
    return ~abs_v + 1;
  }
  return {static_cast<uint16_t>(value)};
}

std::bitset<8> Packer::twos_complement(int8_t value) {
  if (value < 0) {
    const auto abs_v = abs(value);
    return ~abs_v + 1;
  }
  return {static_cast<uint8_t>(value)};
}

template <>
void Packer::pack_type(const int8_t& value) {
  if (value > 31 || value < -32) {
    serialized_object.emplace_back(int8);
  }
  serialized_object.emplace_back(
      static_cast<uint8_t>(twos_complement(value).to_ulong()));
}

Unpacker::Unpacker(const uint8_t* data_start, std::size_t bytes)
    : data_pointer(data_start), data_end(data_start + bytes) {}

void Unpacker::set_data(const uint8_t* pointer, std::size_t size) {
  data_pointer = pointer;
  data_end = data_pointer + size;
}

std::error_code Unpacker::GetErrorCode() const {
  return error_code_;
}

const uint8_t* Unpacker::GetDataPtr() const {
  if (data_pointer < data_end) {
    return data_pointer;
  }
  error_code_ = UnpackerError::OutOfRange;
  return nullptr;
}

uint8_t Unpacker::safe_data() const {
  if (data_pointer < data_end)
    return *data_pointer;
  error_code_ = UnpackerError::OutOfRange;
  return 0;
}

void Unpacker::safe_increment(int64_t bytes) {
  if (data_end - data_pointer >= 0) {
    data_pointer += bytes;
  } else {
    error_code_ = UnpackerError::OutOfRange;
  }
}
}  // namespace cppack