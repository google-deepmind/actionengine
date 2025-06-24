#ifndef EGLT_NET_PEERS_H
#define EGLT_NET_PEERS_H

#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <eglt/absl_headers.h>
#include <eglt/data/eg_structs.h>
#include <eglt/net/recoverable_stream.h>
#include <eglt/nodes/async_node.h>
#include <eglt/stores/local_chunk_store.h>

namespace eglt::net {

struct SessionMessageWithAddress {
  std::optional<SessionMessage> message;
  std::string_view sender_id{};
  std::vector<std::string_view> recipient_ids{};
  std::vector<std::string_view> excluded_ids{};
};

class BackgroundReceiver {
 public:
  explicit BackgroundReceiver(
      WireStream* absl_nonnull stream,
      thread::Writer<SessionMessageWithAddress>* absl_nonnull receive_into) {
    receiver_fiber_ = std::make_unique<thread::Fiber>([stream, receive_into]() {
      while (!thread::Cancelled()) {
        std::optional<SessionMessage> message = stream->Receive();
        const bool is_null = !message.has_value();
        receive_into->WriteUnlessCancelled(
            {.message = std::move(message), .sender_id = stream->GetId()});
        if (is_null) {
          break;
        }
      }
    });
  }
  ~BackgroundReceiver() {
    receiver_fiber_->Cancel();
    receiver_fiber_->Join();
  }

 private:
  std::unique_ptr<thread::Fiber> receiver_fiber_;
};

class BufferedSender {
 public:
  explicit BufferedSender(WireStream* absl_nonnull stream,
                          thread::Reader<SessionMessage>* absl_nonnull
                              send_from)
      : writer_fiber_(
            std::make_unique<thread::Fiber>([stream, send_from, this]() {
              while (!thread::Cancelled()) {
                SessionMessage message;
                if (!send_from->Read(&message)) {
                  break;
                }
                concurrency::MutexLock lock(&mu_);
                status_ = stream->Send(std::move(message));
                if (!status_.ok()) {
                  failed_event_.Notify();
                  break;
                }
              }
            })) {}

  // This class is not copyable or movable.
  BufferedSender(const BufferedSender&) = delete;
  BufferedSender& operator=(const BufferedSender&) = delete;

  ~BufferedSender() {
    writer_fiber_->Cancel();
    writer_fiber_->Join();
  }

  thread::Case OnFailed() const { return failed_event_.OnEvent(); }

  absl::Status GetStatus() const {
    concurrency::MutexLock lock(&mu_);
    return status_;
  }

 private:
  std::unique_ptr<thread::Fiber> writer_fiber_;
  absl::Status status_;
  thread::PermanentEvent failed_event_{};

  mutable concurrency::Mutex mu_;
};

class WirePeer {
 public:
  WirePeer(std::unique_ptr<WireStream> stream,
           concurrency::Writer<SessionMessageWithAddress>* receive_into)
      : stream_(std::move(stream)),
        receiver_{stream.get(), receive_into},
        sender_{stream_.get(), send_queue_.reader()} {}

  ~WirePeer() = default;

  std::string_view GetId() const { return stream_->GetId(); }

  absl::Status Send(SessionMessage message) {
    if (!sender_.GetStatus().ok()) {
      return absl::FailedPreconditionError(
          "Cannot send message on a stream whose status is not ok.");
    }
    send_queue_.writer()->Write(std::move(message));
    return absl::OkStatus();
  }

 private:
  std::unique_ptr<WireStream> stream_;
  thread::Channel<SessionMessage> send_queue_{1024};

  BackgroundReceiver receiver_;
  BufferedSender sender_;
};

class OutboundPeerGroup {
 public:
  explicit OutboundPeerGroup(std::vector<std::shared_ptr<WirePeer>> peers) {
    absl::flat_hash_map<std::string_view, std::shared_ptr<WirePeer>> peer_map;
    for (auto& peer : peers) {
      peer_map.emplace(peer->GetId(), std::move(peer));
    }
    peers_ = std::move(peer_map);
  }

  OutboundPeerGroup(const OutboundPeerGroup& other) {
    concurrency::TwoMutexLock lock(&mu_, &other.mu_);
    peers_ = other.peers_;
  }
  OutboundPeerGroup(OutboundPeerGroup&& other) noexcept {
    concurrency::TwoMutexLock lock(&mu_, &other.mu_);
    peers_ = std::move(other.peers_);
  }

  OutboundPeerGroup& operator=(const OutboundPeerGroup& other) {
    if (this == &other) {
      return *this;
    }
    concurrency::TwoMutexLock lock(&mu_, &other.mu_);
    peers_ = other.peers_;
    return *this;
  }

  OutboundPeerGroup& operator=(OutboundPeerGroup&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    concurrency::TwoMutexLock lock(&mu_, &other.mu_);
    peers_ = std::move(other.peers_);
    return *this;
  }

  ~OutboundPeerGroup() = default;

  void AddPeer(const std::shared_ptr<WirePeer>& peer) {
    concurrency::MutexLock lock(&mu_);
    peers_.emplace(peer->GetId(), peer);
  }

  void RemovePeer(const std::shared_ptr<WirePeer>& peer) {
    concurrency::MutexLock lock(&mu_);
    peers_.erase(peer->GetId());
  }

  std::vector<std::pair<std::string_view, absl::Status>> Send(
      const SessionMessage& message) {
    concurrency::MutexLock lock(&mu_);
    std::vector<std::pair<std::string_view, absl::Status>> statuses;
    for (const auto& [peer_id, peer] : peers_) {
      statuses.emplace_back(peer_id, peer->Send(message));
    }
    return statuses;
  }

  size_t Size() const {
    concurrency::MutexLock lock(&mu_);
    return peers_.size();
  }

 private:
  absl::flat_hash_map<std::string_view, std::shared_ptr<WirePeer>> peers_
      ABSL_GUARDED_BY(mu_);
  mutable concurrency::Mutex mu_;
};

}  // namespace eglt::net

#endif  // EGLT_NET_PEERS_H