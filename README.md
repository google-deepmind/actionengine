# ActionEngine Light / Action Engine

## Development log / to do list

- [ ] (Important) Refine ChunkStoreReader status handling
- [ ] Debug WS streams state/error handling
- [ ] Outline action error / status flow
- [ ] Make final alignment of this repo with ai-action-engine-ts
- [x] Make `g3fiber` work (achieve runnable ChunkStoreTest)
- [x] Achieve multithreaded fibers (worker pool for `boost::fibers::context`s)
- [ ] Set up a Doxygen build action for the C++ code (and delete html folder)
- [x] Integrate Pythonic bindings into this repo
- [ ] Add a test suite for the Pythonic bindings
- [ ] Add a CI pipeline that ensures the C++ code compiles and tests pass
- [x] Add examples with a client-server architecture (__WIP, needs debugging for
  blocking operations__)
- [x] Implement WebSocket client/server support in C++ with msgpack
  serialization
- [ ] Bring global settings and serialiser registries to the C++ code from
  Python bindings

- [ ] Cover important code with tests