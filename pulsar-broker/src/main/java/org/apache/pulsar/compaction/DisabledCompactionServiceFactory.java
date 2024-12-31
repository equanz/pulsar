package org.apache.pulsar.compaction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.jetbrains.annotations.NotNull;

public class DisabledCompactionServiceFactory implements CompactionServiceFactory {
    @Override
    public CompletableFuture<Void> initialize(@NotNull PulsarService pulsarService) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TopicCompactionService> newTopicCompactionService(@NotNull String topic) {
        return CompletableFuture.completedFuture(new DisabledCompactor());
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

    public static class DisabledCompactor implements TopicCompactionService {
        @Override
        public CompletableFuture<Void> compact() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<Entry>> readCompactedEntries(@NotNull Position startPosition,
                                                                   int numberOfEntriesToRead) {
            return CompletableFuture.completedFuture(Collections.EMPTY_LIST);
        }

        @Override
        public CompletableFuture<Entry> readLastCompactedEntry() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Position> getLastCompactedPosition() {
            return CompletableFuture.completedFuture(PositionFactory.EARLIEST);
        }

        @Override
        public CompletableFuture<Entry> findEntryByPublishTime(long publishTime) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Entry> findEntryByEntryIndex(long entryIndex) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws Exception {

        }
    }
}
