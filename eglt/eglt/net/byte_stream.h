#ifndef EGLT_NET_BYTE_STREAM_H_
#define EGLT_NET_BYTE_STREAM_H_

#include <memory>
#include <optional>

#include <eglt/absl_headers.h>
#include <eglt/data/eg_structs.h>
#include <eglt/data/serialization.h>
#include <eglt/net/stream.h>

namespace eglt {

class EvergreenByteStream {
public:
  explicit EvergreenByteStream(
    SendBytesT send_bytes = nullptr, ReceiveBytesT receive_bytes = nullptr,
    const std::shared_ptr<Serializer>& serializer = nullptr);

  absl::Status Send(base::SessionMessage message) const;
  [[nodiscard]] std::optional<base::SessionMessage> Receive() const;

  [[nodiscard]] std::optional<Bytes> ReceiveBytes() const;
  absl::Status SendBytes(Bytes bytes) const;

protected:
  SendBytesT send_bytes_;
  ReceiveBytesT receive_bytes_;
  std::shared_ptr<Serializer> serializer_;
};

} // namespace eglt

#endif  // EGLT_NET_BYTE_STREAM_H_
