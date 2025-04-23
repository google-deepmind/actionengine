#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <absl/status/status_matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "eglt/absl_headers.h"
#include "eglt/data/eg_structs.h"
#include "eglt/nodes/chunk_store_io.h"
#include "eglt/nodes/chunk_store_local.h"

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::absl_testing::IsOk())

namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;

using eglt::base::Chunk;

TEST(ChunkStoreTest, CanWriteChunks) {
  {
    eglt::LocalChunkStore chunk_store;
    eglt::ChunkStoreWriter writer(&chunk_store);

    writer << "Hello" << "World" << "!" << eglt::EndOfStream();

    chunk_store.WaitForArrivalOffset(3, /*timeout=*/absl::InfiniteDuration())
        .IgnoreError();
    EXPECT_THAT(chunk_store.Size(), Eq(4));
    EXPECT_THAT(chunk_store.GetFinalSeqId(), Eq(2));
  }

  // if no explicit end of stream is written, the store should contain exactly
  // the same number of chunks as written.
  {
    eglt::LocalChunkStore chunk_store;
    eglt::ChunkStoreWriter writer(&chunk_store);

    writer << "Hello" << "World"
           << std::pair("!", true);  // true is for final chunk

    eglt::concurrency::SleepFor(absl::Seconds(
        0.05));  // TODO(hpnkv): add a method to wait for finalisation

    EXPECT_THAT(chunk_store.Size(), Eq(3));
    EXPECT_THAT(chunk_store.GetFinalSeqId(), Eq(2));
  }
}

TEST(ChunkStoreTest, WrittenChunksAreReadable) {
  eglt::LocalChunkStore chunk_store;
  eglt::ChunkStoreWriter writer(&chunk_store);

  // Write some chunks.
  std::vector<std::string> words = {"Hello", "World", "!"};
  writer << words << eglt::EndOfStream();

  // Wait for all chunks to arrive.
  chunk_store
      .WaitForArrivalOffset(static_cast<int>(words.size()),
                            /*timeout=*/absl::InfiniteDuration())
      .IgnoreError();

  // Read the chunks back in order and check that they are correct.
  eglt::ChunkStoreReader reader(&chunk_store, /*ordered=*/true);

  std::vector<std::string> read_words;
  reader >> read_words;

  EXPECT_THAT(read_words, Eq(words));
}

TEST(ChunkStoreTest, CanReadChunksAsynchronously) {
  // Even though writes happen in the background, in some runs the writer will
  // finish before the reader starts, so we run the test multiple times.
  for (int i = 0; i < 100; ++i) {
    eglt::LocalChunkStore chunk_store;
    eglt::ChunkStoreWriter writer(&chunk_store);
    eglt::ChunkStoreReader reader(&chunk_store, /*ordered=*/true);

    std::vector<std::string> words = {"Hello", "World", "!"};
    writer << words << eglt::EndOfStream();

    // ------- NOT waiting for all chunks to arrive. -------
    // Reader should be able to read the chunks as they arrive.

    std::vector<std::string> read_words;
    reader >> read_words;

    EXPECT_EQ(read_words, words);
  }
}

TEST(ChunkStoreTest, OrderedReaderOrdersChunks) {
  std::vector<std::string> words =
      absl::StrSplit("Hello World! This is a slightly longer sentence.", ' ');

  std::vector<std::pair<int, std::string>> seq_and_words;
  seq_and_words.reserve(words.size());
  for (const auto& word : words) {
    seq_and_words.emplace_back(std::pair(seq_and_words.size(), word));
  }

  absl::c_shuffle(seq_and_words, absl::BitGen());
  // just to make sure that the test is not trivial.
  if (seq_and_words[0].first == 0) {
    std::swap(seq_and_words[0], seq_and_words[1]);
  }

  eglt::LocalChunkStore chunk_store;

  eglt::ChunkStoreWriter writer(&chunk_store);
  for (const auto& [seq, word] : seq_and_words) {
    writer << std::pair(word, seq);
  }
  writer << eglt::EndOfStream();

  eglt::ChunkStoreReader reader(&chunk_store, /*ordered=*/true);

  for (const auto& word : words) {
    EXPECT_THAT(reader.Next<std::string>(), Eq(word));
  }
  EXPECT_THAT(reader.Next<std::string>(), Eq(std::nullopt));
}

TEST(ChunkStoreTest, UnorderedReaderReadsChunksAsTheyArrive) {
  std::vector<std::string> words =
      absl::StrSplit("Hello World! This is a slightly longer sentence.", ' ');

  std::vector<std::pair<int, std::string>> seq_and_words;
  seq_and_words.reserve(words.size());
  for (const auto& word : words) {
    seq_and_words.emplace_back(std::pair(seq_and_words.size(), word));
  }

  absl::c_shuffle(seq_and_words, absl::BitGen());
  // just to make sure that the test is not trivial.
  if (seq_and_words[0].first == 0) {
    std::swap(seq_and_words[0], seq_and_words[1]);
  }

  eglt::LocalChunkStore chunk_store;
  eglt::ChunkStoreWriter writer(&chunk_store);

  for (const auto& [seq, word] : seq_and_words) {
    writer << std::pair(word, seq);
  }

  writer << eglt::EndOfStream();

  eglt::ChunkStoreReader reader(&chunk_store, /*ordered=*/false);

  for (const auto& [seq, word] : seq_and_words) {
    EXPECT_THAT(reader.Next<std::string>(), Eq(word));
  }
  EXPECT_THAT(reader.Next<std::string>(), Eq(std::nullopt));
}

TEST(ChunkStoreTest, ReaderRemovesChunks) {
  {
    eglt::LocalChunkStore chunk_store;
    eglt::ChunkStoreWriter writer(&chunk_store);
    eglt::ChunkStoreReader reader(&chunk_store, /*ordered=*/true,
                                  /*remove_chunks=*/true);

    writer << "Hello" << "World" << "!" << eglt::EndOfStream();
    chunk_store.WaitForArrivalOffset(3, /*timeout=*/absl::InfiniteDuration())
        .IgnoreError();
    EXPECT_THAT(chunk_store.Size(), Eq(4));
    EXPECT_THAT(chunk_store.GetFinalSeqId(), Eq(2));

    std::vector<std::string> read_words;
    reader >> read_words;
    EXPECT_OK(reader.GetStatus());
    EXPECT_THAT(chunk_store.Size(),
                Eq(1));  // Only the null chunk remains, and it is allowed.
  }

  {
    eglt::LocalChunkStore chunk_store;
    eglt::ChunkStoreWriter writer(&chunk_store);

    writer << "Hello" << "World"
           << std::pair("!", true);  // true is for final chunk

    eglt::concurrency::SleepFor(absl::Seconds(0.01));

    EXPECT_THAT(chunk_store.Size(), Eq(3));
    EXPECT_THAT(chunk_store.GetFinalSeqId(), Eq(2));

    eglt::ChunkStoreReader reader(&chunk_store, /*ordered=*/true,
                                  /*remove_chunks=*/true);
    std::vector<std::string> read_words;
    reader >> read_words;
    EXPECT_OK(reader.GetStatus());
    EXPECT_THAT(chunk_store.Size(),
                Eq(0));  // No chunks should remain.
  }
}

TEST(ChunkStoreTest, OrderedReaderBlocksUntilChunksArrive) {
  eglt::LocalChunkStore chunk_store;

  eglt::concurrency::Fiber::Current();

  auto writer = std::make_unique<eglt::ChunkStoreWriter>(&chunk_store);
  auto reader =
      std::make_unique<eglt::ChunkStoreReader>(&chunk_store,
                                               /*ordered=*/true,
                                               /*remove_chunks=*/false,
                                               /*n_chunks_to_buffer=*/-1,
                                               /*timeout=*/absl::Seconds(0.01));

  writer << std::pair("World", 1) << std::pair("!", 2)
         << std::pair(eglt::EndOfStream(), 3);

  // The reader should block until chunk 0 arrives.
  reader->Next<std::string>();
  EXPECT_THAT(reader->GetStatus(),
              StatusIs(absl::StatusCode::kDeadlineExceeded));

  writer << std::pair("Hello", 0);

  // The reader should now be able to read the chunks without blocking.
  reader = std::make_unique<eglt::ChunkStoreReader>(&chunk_store,
                                                    /*ordered=*/true);

  std::vector<std::string> read_words;
  *reader >> read_words;

  EXPECT_OK(reader->GetStatus());
  EXPECT_THAT(read_words, ElementsAre("Hello", "World", "!"));
}

}  // namespace
