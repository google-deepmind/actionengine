// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef EGLT_STORES_SHARED_CHUNK_STORE_H_
#define EGLT_STORES_SHARED_CHUNK_STORE_H_

#include <cstddef>
#include <memory>

#include "eglt/absl_headers.h"
#include "eglt/concurrency/concurrency.h"
#include "eglt/data/eg_structs.h"
#include "eglt/net/recoverable_stream.h"
#include "eglt/nodes/async_node.h"
#include "eglt/stores/chunk_store.h"
#include "eglt/stores/local_chunk_store.h"
#include "eglt/util/map_util.h"

namespace eglt {}  // namespace eglt

#endif  // EGLT_STORES_SHARED_CHUNK_STORE_H_