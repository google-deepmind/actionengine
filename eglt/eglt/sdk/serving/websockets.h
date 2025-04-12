#ifndef EGLT_SDK_SERVING_WEBSOCKETS_H_
#define EGLT_SDK_SERVING_WEBSOCKETS_H_

#include <memory>

#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerRequestImpl.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/WebSocket.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPMessage.h"
#include "Poco/Net/HTTPClientSession.h"

#include "cppack/msgpack.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/stream.h"
#include "eglt/service/service.h"

namespace eglt::sdk {

class WSRequestHandler;

class WSEvergreenStream : public base::EvergreenStream {
public:
  explicit
  WSEvergreenStream(
      WSRequestHandler* absl_nonnull handler) : handler_(handler) {}

private:
  WSRequestHandler* absl_nonnull handler_ ABSL_GUARDED_BY(mutex_);
  eglt::concurrency::Mutex mutex_;
};

class WSRequestHandler final : public Poco::Net::HTTPRequestHandler {
public:
  class Factory final :
      public Poco::Net::HTTPRequestHandlerFactory {
  public:
    Poco::Net::HTTPRequestHandler* createRequestHandler(
        const Poco::Net::HTTPServerRequest& request) override {
      return new WSRequestHandler();
    }
  };

  void handleRequest(Poco::Net::HTTPServerRequest& request,
                     Poco::Net::HTTPServerResponse& response) override {}

private:
};

class WebsocketEvergreenServer {
public:
  WebsocketEvergreenServer(int port = 20000);
};

} // namespace eglt::sdk


#endif   // EGLT_SDK_SERVING_WEBSOCKETS_H_