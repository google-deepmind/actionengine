# Evergreen Light / Action Engine

## Development log / to do list

- [x] Make `thread_on_boost` work (achieve runnable ChunkStoreTest)
- [ ] Achieve multithreaded fibers (worker pool for `boost::fibers::context`s)
- [ ] Set up a Doxygen build action for the C++ code (and delete html folder)
- [ ] Integrate Pythonic bindings into this repo
- [ ] Add a test suite for the Pythonic bindings
- [ ] Add a CI pipeline that ensures the C++ code compiles and tests pass
- [ ] Add examples with a client-server architecture
- [ ] Implement WebSocket client/server support in C++ with msgpack
  serialization
- [ ] Cover important code with tests