package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.AsyncOperationTimeoutSeconds;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

@Slf4j
public class FileManagedLedgerImpl implements ManagedLedger {
    private final String name;

    private volatile ManagedLedgerConfig config;
    private volatile ManagedLedgerImpl.State state = null;

    public FileManagedLedgerImpl(String name, ManagedLedgerConfig config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, 0, data.length);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, numberOfMessages, 0, data.length);
    }

    @Override
    public void asyncAddEntry(byte[] data, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        asyncAddEntry(data, 0, data.length, callback, ctx);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, 1, offset, length);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        // Result list will contain the status exception and the resulting
        // position
        class Result {
            ManagedLedgerException status = null;
            Position position = null;
        }
        final Result result = new Result();

        asyncAddEntry(data, numberOfMessages, offset, length, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                result.status = exception;
                counter.countDown();
            }
        }, null);

        counter.await();

        if (result.status != null) {
            log.error("[{}] Error adding entry", name, result.status);
            throw result.status;
        }

        return result.position;
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data, offset, length);
        asyncAddEntry(buffer, callback, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length,
                              AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data, offset, length);
        asyncAddEntry(buffer, numberOfMessages, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        asyncAddEntry(buffer, 1, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {
        // TODO: impl
    }

    @Override
    public ManagedCursor openCursor(String cursorName) throws InterruptedException, ManagedLedgerException {
        return openCursor(cursorName, InitialPosition.Latest);
    }

    @Override
    public ManagedCursor openCursor(String cursorName, InitialPosition initialPosition)
            throws InterruptedException, ManagedLedgerException {
        return openCursor(cursorName, initialPosition, Collections.emptyMap(), Collections.emptyMap());
    }

    @Override
    public ManagedCursor openCursor(String cursorName, InitialPosition initialPosition, Map<String, Long> properties,
                                    Map<String, String> cursorProperties)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedCursor cursor = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncOpenCursor(cursorName, initialPosition, properties, cursorProperties,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        result.cursor = cursor;
                        counter.countDown();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        result.exception = exception;
                        counter.countDown();
                    }

                }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during open-cursor operation");
        }

        if (result.exception != null) {
            log.error("Error adding entry", result.exception);
            throw result.exception;
        }

        return result.cursor;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        return newNonDurableCursor(
                startCursorPosition,
                "non-durable-cursor-" + UUID.randomUUID());
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName)
            throws ManagedLedgerException {
        return newNonDurableCursor(startPosition, subscriptionName, InitialPosition.Latest, false);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName,
                                             InitialPosition initialPosition, boolean isReadCompacted)
            throws ManagedLedgerException {
        throw ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException());
    }

    @Override
    public void asyncDeleteCursor(String name, AsyncCallbacks.DeleteCursorCallback callback, Object ctx) {
        // TOOD: impl
    }

    @Override
    public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncDeleteCursor(name, new AsyncCallbacks.DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during delete-cursors operation");
        }

        if (result.exception != null) {
            log.error("Deleting cursor", result.exception);
            throw result.exception;
        }
    }

    @Override
    public void removeWaitingCursor(ManagedCursor cursor) {
        // no-op
    }

    @Override
    public void asyncOpenCursor(String cursorName, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        this.asyncOpenCursor(cursorName, InitialPosition.Latest, callback, ctx);
    }

    @Override
    public void asyncOpenCursor(String cursorName, InitialPosition initialPosition,
                                AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        this.asyncOpenCursor(cursorName, initialPosition, Collections.emptyMap(), Collections.emptyMap(),
                callback, ctx);
    }

    @Override
    public void asyncOpenCursor(String cursorName, InitialPosition initialPosition, Map<String, Long> properties,
                                Map<String, String> cursorProperties, AsyncCallbacks.OpenCursorCallback callback,
                                Object ctx) {
        // TODO: impl
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        // TODO: impl
        return null;
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        // TODO: impl
        return null;
    }

    @Override
    public long getNumberOfEntries() {
        // TODO: impl
        return 0;
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        // TODO: impl
        return 0;
    }

    @Override
    public long getNumberOfActiveEntries() {
        // mock
        return 0;
    }

    @Override
    public long getTotalSize() {
        // mock
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        // mock
        return 0;
    }

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        // mock
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public long getOffloadedSize() {
        // mock
        return 0;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        // mock
        return 0;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        // mock
        return 0;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        // mock
        return 0;
    }

    @Override
    public void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {
        // TODO: impl
    }

    @Override
    public CompletableFuture<Position> asyncMigrate() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public Position terminate() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            Position lastPosition = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncTerminate(new AsyncCallbacks.TerminateCallback() {
            @Override
            public void terminateComplete(Position lastPosition, Object ctx) {
                result.lastPosition = lastPosition;
                counter.countDown();
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger terminate");
        }

        if (result.exception != null) {
            log.error("[{}] Error terminating managed ledger", name, result.exception);
            throw result.exception;
        }

        return result.lastPosition;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger close");
        }

        if (result.exception != null) {
            log.error("[{}] Error closing managed ledger", name, result.exception);
            throw result.exception;
        }
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        // TODO: impl
        callback.closeComplete(ctx);
    }

    private static final class MockManagedLedgerMXBean implements ManagedLedgerMXBean {
        @Override
        public String getName() {
            return "";
        }

        @Override
        public long getStoredMessagesSize() {
            return 0;
        }

        @Override
        public long getStoredMessagesLogicalSize() {
            return 0;
        }

        @Override
        public long getNumberOfMessagesInBacklog() {
            return 0;
        }

        @Override
        public double getAddEntryMessagesRate() {
            return 0;
        }

        @Override
        public double getAddEntryBytesRate() {
            return 0;
        }

        @Override
        public long getAddEntryBytesTotal() {
            return 0;
        }

        @Override
        public double getAddEntryWithReplicasBytesRate() {
            return 0;
        }

        @Override
        public long getAddEntryWithReplicasBytesTotal() {
            return 0;
        }

        @Override
        public double getReadEntriesRate() {
            return 0;
        }

        @Override
        public double getReadEntriesBytesRate() {
            return 0;
        }

        @Override
        public long getReadEntriesBytesTotal() {
            return 0;
        }

        @Override
        public double getMarkDeleteRate() {
            return 0;
        }

        @Override
        public long getMarkDeleteTotal() {
            return 0;
        }

        @Override
        public long getAddEntrySucceed() {
            return 0;
        }

        @Override
        public long getAddEntrySucceedTotal() {
            return 0;
        }

        @Override
        public long getAddEntryErrors() {
            return 0;
        }

        @Override
        public long getAddEntryErrorsTotal() {
            return 0;
        }

        @Override
        public long getEntriesReadTotalCount() {
            return 0;
        }

        @Override
        public long getReadEntriesSucceeded() {
            return 0;
        }

        @Override
        public long getReadEntriesSucceededTotal() {
            return 0;
        }

        @Override
        public long getReadEntriesErrors() {
            return 0;
        }

        @Override
        public long getReadEntriesErrorsTotal() {
            return 0;
        }

        @Override
        public double getReadEntriesOpsCacheMissesRate() {
            return 0;
        }

        @Override
        public long getReadEntriesOpsCacheMissesTotal() {
            return 0;
        }

        @Override
        public double getEntrySizeAverage() {
            return 0;
        }

        @Override
        public long[] getEntrySizeBuckets() {
            return new long[0];
        }

        @Override
        public double getAddEntryLatencyAverageUsec() {
            return 0;
        }

        @Override
        public long[] getAddEntryLatencyBuckets() {
            return new long[0];
        }

        @Override
        public long[] getLedgerSwitchLatencyBuckets() {
            return new long[0];
        }

        @Override
        public double getLedgerSwitchLatencyAverageUsec() {
            return 0;
        }

        @Override
        public StatsBuckets getInternalAddEntryLatencyBuckets() {
            return null;
        }

        @Override
        public StatsBuckets getInternalEntrySizeBuckets() {
            return null;
        }

        @Override
        public PendingBookieOpsStats getPendingBookieOpsStats() {
            return null;
        }

        @Override
        public double getLedgerAddEntryLatencyAverageUsec() {
            return 0;
        }

        @Override
        public long[] getLedgerAddEntryLatencyBuckets() {
            return new long[0];
        }

        @Override
        public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() {
            return null;
        }
    }

    ;
    private final ManagedLedgerMXBean mockStats = new MockManagedLedgerMXBean();

    @Override
    public ManagedLedgerMXBean getStats() {
        // mock
        return mockStats;
    }

    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicReference<ManagedLedgerException> exception = new AtomicReference<>();

        asyncDelete(new AsyncCallbacks.DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException e, Object ctx) {
                exception.set(e);
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger delete operation");
        }

        if (exception.get() != null) {
            log.error("[{}] Error deleting managed ledger", name, exception.get());
            throw exception.get();
        }
    }

    @Override
    public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        // TODO: impl
    }

    @Override
    public Position offloadPrefix(Position pos) throws InterruptedException, ManagedLedgerException {
        CompletableFuture<Position> promise = new CompletableFuture<>();

        asyncOffloadPrefix(pos, new AsyncCallbacks.OffloadCallback() {
            @Override
            public void offloadComplete(Position offloadedTo, Object ctx) {
                promise.complete(offloadedTo);
            }

            @Override
            public void offloadFailed(ManagedLedgerException e, Object ctx) {
                promise.completeExceptionally(e);
            }
        }, null);

        try {
            return promise.get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            throw new ManagedLedgerException("Timeout during managed ledger offload operation");
        } catch (ExecutionException e) {
            log.error("[{}] Error offloading. pos = {}", name, pos, e.getCause());
            throw ManagedLedgerException.getManagedLedgerException(e.getCause());
        }
    }

    @Override
    public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {
        callback.offloadFailed(ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()),
                ctx);
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        // TODO: impl
        return null;
    }

    @Override
    public boolean isTerminated() {
        return state == ManagedLedgerImpl.State.Terminated;
    }

    @Override
    public boolean isMigrated() {
        return false;
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(ManagedLedgerConfig config) {
        this.config = config;
    }

    @Override
    public Position getLastConfirmedEntry() {
        // TODO: impl
        return null;
    }

    @Override
    public void readyToCreateNewLedger() {
        // no-op
    }

    @Override
    public Map<String, String> getProperties() {
        // mock
        return Map.of();
    }

    @Override
    public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {
        // no-op
    }

    @Override
    public void asyncSetProperty(String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback,
                                 Object ctx) {
        callback.updatePropertiesFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public void deleteProperty(String key) throws InterruptedException, ManagedLedgerException {
        // no-op
    }

    @Override
    public void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
        callback.updatePropertiesFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {
        // no-op
    }

    @Override
    public void asyncSetProperties(Map<String, String> properties, AsyncCallbacks.UpdatePropertiesCallback callback,
                                   Object ctx) {
        callback.updatePropertiesFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
        // TODO: impl
    }

    @Override
    public void rollCurrentLedgerIfFull() {
        // TODO: impl
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        // mock
        return null;
    }

    @Override
    public CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId) {
        // TODO: impl
        return null;
    }

    @Override
    public Optional<LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
        // TODO: impl
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Void> asyncTruncate() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean checkInactiveLedgerAndRollOver() {
        // mock
        return false;
    }

    @Override
    public void checkCursorsToCacheEntries() {
        // no-op
    }

    @Override
    public void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        // TODO: impl
    }

    @Override
    public NavigableMap<Long, LedgerInfo> getLedgersInfo() {
        // TODO: impl
        return null;
    }

    @Override
    public Position getNextValidPosition(Position position) {
        // TODO: impl
        return null;
    }

    @Override
    public Position getPreviousPosition(Position position) {
        // TODO: impl
        return null;
    }

    @Override
    public long getEstimatedBacklogSize(Position position) {
        // mock
        return 0;
    }

    @Override
    public Position getPositionAfterN(Position startPosition, long n, PositionBound startRange) {
        // TODO: impl
        return null;
    }

    @Override
    public int getPendingAddEntriesCount() {
        // mock
        return 0;
    }

    @Override
    public long getCacheSize() {
        // mock
        return 0;
    }

    @Override
    public Position getFirstPosition() {
        // TODO: impl
        return null;
    }
}
