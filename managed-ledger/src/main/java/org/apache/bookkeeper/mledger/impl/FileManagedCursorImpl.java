package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import com.google.common.collect.Range;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

@Slf4j
public class FileManagedCursorImpl implements ManagedCursor {
    private final FileManagedLedgerImpl ml;
    private final String name;
    private volatile long lastActive;
    private volatile Position readPosition;
    private volatile Position markDeletePosition;
    private volatile Position persistentMarkDeletePosition;
    private volatile ManagedCursorImpl.State state = null;

    private static final long NO_MAX_SIZE_LIMIT = -1L;

    public FileManagedCursorImpl(FileManagedLedgerImpl ml,
                                 String cursorName) {
        this.ml = ml;
        this.name = cursorName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastActive() {
        // mock
        return lastActive;
    }

    @Override
    public void updateLastActive() {
        lastActive = System.currentTimeMillis();
    }

    @Override
    public Map<String, Long> getProperties() {
        // mock
        return Map.of();
    }

    @Override
    public Map<String, String> getCursorProperties() {
        // mock
        return Map.of();
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public boolean putProperty(String key, Long value) {
        // no-op
        return false;
    }

    @Override
    public boolean removeProperty(String key) {
        // no-op
        return false;
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReadEntries(numberOfEntriesToRead, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null, PositionFactory.LATEST);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                 Position maxPosition) {
        asyncReadEntries(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                 AsyncCallbacks.ReadEntriesCallback callback, Object ctx, Position maxPosition) {
        // TODO: impl
        callback.readEntriesFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {
        throw ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException());
    }

    @Override
    public void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        callback.readEntryFailed(ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()),
                ctx);
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead)
            throws InterruptedException, ManagedLedgerException {
        return readEntriesOrWait(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT);
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead, long maxSizeBytes)
            throws InterruptedException, ManagedLedgerException {
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReadEntriesOrWait(numberOfEntriesToRead, maxSizeBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null, PositionFactory.LATEST);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, Position maxPosition) {
        asyncReadEntriesOrWait(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    public static final class OpReadEntries implements AsyncCallbacks.ReadEntriesCallback {
        public final Position readPosition;
        public final int maxEntries;
        public final long maxSizeBytes;
        public final Position maxPosition;
        private final AsyncCallbacks.ReadEntriesCallback callback;
        private final CountDownLatch latch;

        public OpReadEntries(Position readPosition, int maxEntries, long maxSizeBytes, Position maxPosition,
                             AsyncCallbacks.ReadEntriesCallback callback, CountDownLatch latch) {
            this.readPosition = readPosition;
            this.maxEntries = maxEntries;
            this.maxSizeBytes = maxSizeBytes;
            this.maxPosition = maxPosition;
            this.callback = callback;
            this.latch = latch;
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            callback.readEntriesComplete(entries, ctx);
            latch.countDown();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            callback.readEntriesFailed(exception, ctx);
            latch.countDown();
        }
    }

    @Override
    public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, Position maxPosition) {
        // TODO: impl
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // spin if the backlog has no new messages
        while (readPosition.compareTo(ml.getLastConfirmedEntry()) > 0) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                callback.readEntriesFailed(ManagedLedgerException.getManagedLedgerException(e), ctx);
                return;
            }
        }

        final OpReadEntries op =
                new OpReadEntries(readPosition, maxEntries, maxSizeBytes, maxPosition, callback, countDownLatch);

        // simple tail read
        ml.asyncReadEntries(op, ctx);

        try {
            countDownLatch.await();
        } catch (Exception e) {
            callback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(e), ctx);
        }
    }

    @Override
    public boolean cancelPendingReadRequest() {
        // no-op
        return false;
    }

    @Override
    public boolean hasMoreEntries() {
        // no-op
        return false;
        /*
        // TODO: impl, manage readPosition
        final Position writerPosition = ml.getLastPosition();
        if (writerPosition.getEntryId() != -1) {
            return readPosition.compareTo(writerPosition) <= 0;
        } else {
            // Fall back to checking the number of entries to ensure we are at the last entry in ledger and no ledgers
            // are in the middle
            return getNumberOfEntries() > 0;
        }
         */
    }

    public void initializeCursorPosition(Position position) {
        this.readPosition = position.getNext();
    }

    @Override
    public long getNumberOfEntries() {
        // mock
        return 0;
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean isPrecise) {
        // mock
        return 0;
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        markDelete(position, Collections.emptyMap());
    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties)
            throws InterruptedException, ManagedLedgerException {
        requireNonNull(position);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncMarkDelete(position, properties, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during mark-delete operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        asyncMarkDelete(position, Collections.emptyMap(), callback, ctx);
    }

    @Override
    public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        // TODO: impl
        callback.markDeleteFailed(ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()),
                ctx);
    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {
        delete(Collections.singletonList(position));
    }

    @Override
    public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        asyncDelete(Collections.singletonList(position), callback, ctx);
    }

    @Override
    public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {
        requireNonNull(positions);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicBoolean timeout = new AtomicBoolean(false);

        asyncDelete(positions, new AsyncCallbacks.DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                if (timeout.get()) {
                    log.warn("[{}] [{}] Delete operation timeout. Callback deleteComplete at position {}",
                            ml.getName(), name, positions);
                }

                counter.countDown();
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;

                if (timeout.get()) {
                    log.warn("[{}] [{}] Delete operation timeout. Callback deleteFailed at position {}",
                            ml.getName(), name, positions);
                }

                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            timeout.set(true);
            log.warn("[{}] [{}] Delete operation timeout. No callback was triggered at position {}", ml.getName(),
                    name, positions);
            throw new ManagedLedgerException("Timeout during delete operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncDelete(Iterable<Position> position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        // TODO: impl
        callback.deleteFailed(ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()),
                ctx);
    }

    @Override
    public Position getReadPosition() {
        return readPosition;
    }

    @Override
    public Position getMarkDeletedPosition() {
        return markDeletePosition;
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return persistentMarkDeletePosition;
    }

    @Override
    public void rewind() {
        // no-op

        /*
        lock.writeLock().lock();
        try {
            final Position newReadPosition = ml.getNextValidPosition(markDeletePosition);
            final Position oldReadPosition = readPosition;

            log.info("[{}-{}] Rewind from {} to {}", ml.getName(), name, oldReadPosition, newReadPosition);

            readPosition = newReadPosition;
            ml.onCursorReadPositionUpdated(FileManagedCursorImpl.this, newReadPosition);
        } finally {
            lock.writeLock().unlock();
        }
         */
    }

    @Override
    public void seek(Position newReadPosition, boolean force) {
        // no-op
    }

    @Override
    public void clearBacklog() throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncClearBacklog(new AsyncCallbacks.ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during clear backlog operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {
        callback.clearBacklogFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncSkipEntries(numEntriesToSkip, deletedEntries, new AsyncCallbacks.SkipEntriesCallback() {
            @Override
            public void skipEntriesComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during skip messages operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {
        callback.skipEntriesFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        return findNewestMatching(FindPositionConstraint.SearchActiveEntries, condition);
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            Position position = null;
        }

        final Result result = new Result();
        asyncFindNewestMatching(constraint, condition, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                                        Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        counter.await();
        if (result.exception != null) {
            throw result.exception;
        }

        return result.position;
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx) {
        asyncFindNewestMatching(constraint, condition, callback, ctx, false);
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx,
                                        boolean isFindFromLedger) {
        callback.findEntryFailed(ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()),
                Optional.empty(), ctx);
    }

    @Override
    public void resetCursor(Position position) throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncResetCursor(position, false, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();

            }
        });

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            if (result.exception != null) {
                log.warn("[{}] Reset cursor to {} on cursor {} timed out with exception {}", ml.getName(), position,
                        name, result.exception);
            }
            throw new ManagedLedgerException("Timeout during reset cursor");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncResetCursor(Position position, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {
        callback.resetFailed(ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()),
                null);
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReplayEntries(positions, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        return asyncReplayEntries(positions, callback, ctx, false);
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                                      boolean sortEntries) {
        // TODO: impl
        callback.readEntriesFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
        return Set.of();
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Successfully closed ledger for cursor {}", ml.getName(), name);
                }
                latch.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Closing ledger failed for cursor {}", ml.getName(), name, exception);
                result.exception = exception;
                latch.countDown();
            }
        }, null);

        if (!latch.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during close operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        // TODO: impl
        callback.closeComplete(ctx);
    }

    @Override
    public Position getFirstPosition() {
        final Long firstLedgerId = ml.getLedgersInfo().firstKey();
        return firstLedgerId == null ? null : PositionFactory.create(firstLedgerId, 0);
    }

    @Override
    public void setActive() {
        // no-op
    }

    @Override
    public void setInactive() {
        // no-op
    }

    @Override
    public void setAlwaysInactive() {
        // no-op
    }

    @Override
    public boolean isActive() {
        // mock
        return true;
    }

    @Override
    public boolean isDurable() {
        // mock
        return true;
    }

    @Override
    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        // mock
        return 0;
    }

    @Override
    public int getTotalNonContiguousDeletedMessagesRange() {
        // mock
        return 0;
    }

    @Override
    public int getNonContiguousDeletedMessagesRangeSerializedSize() {
        // mock
        return 0;
    }

    @Override
    public long getEstimatedSizeSinceMarkDeletePosition() {
        // mock
        return 0;
    }

    @Override
    public double getThrottleMarkDelete() {
        // mock
        return 0;
    }

    @Override
    public void setThrottleMarkDelete(double throttleMarkDelete) {
        // no-op
    }

    @Override
    public ManagedLedger getManagedLedger() {
        return ml;
    }

    @Override
    public Range<Position> getLastIndividualDeletedRange() {
        // mock
        return null;
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {
        // no-op
    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(Position position) {
        // no-op
        return new long[0];
    }

    private final ManagedCursorMXBean mockManagedCursorStats = new ManagedCursorMXBean() {
        @Override
        public String getName() {
            return "";
        }

        @Override
        public String getLedgerName() {
            return "";
        }

        @Override
        public void persistToLedger(boolean success) {

        }

        @Override
        public void persistToZookeeper(boolean success) {

        }

        @Override
        public long getPersistLedgerSucceed() {
            return 0;
        }

        @Override
        public long getPersistLedgerErrors() {
            return 0;
        }

        @Override
        public long getPersistZookeeperSucceed() {
            return 0;
        }

        @Override
        public long getPersistZookeeperErrors() {
            return 0;
        }

        @Override
        public void addWriteCursorLedgerSize(long size) {

        }

        @Override
        public void addReadCursorLedgerSize(long size) {

        }

        @Override
        public long getWriteCursorLedgerSize() {
            return 0;
        }

        @Override
        public long getWriteCursorLedgerLogicalSize() {
            return 0;
        }

        @Override
        public long getReadCursorLedgerSize() {
            return 0;
        }
    };

    @Override
    public ManagedCursorMXBean getStats() {
        return mockManagedCursorStats;
    }

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        // mock
        return false;
    }

    @Override
    public boolean isClosed() {
        return state == ManagedCursorImpl.State.Closed || state == ManagedCursorImpl.State.Closing;
    }

    private final ManagedLedgerInternalStats.CursorStats mockCursorStats = new ManagedLedgerInternalStats.CursorStats();

    @Override
    public ManagedLedgerInternalStats.CursorStats getCursorStats() {
        return mockCursorStats;
    }

    @Override
    public boolean isMessageDeleted(Position position) {
        // mock
        return false;
    }

    @Override
    public ManagedCursor duplicateNonDurableCursor(String nonDurableCursorName) throws ManagedLedgerException {
        throw ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException());
    }

    @Override
    public long[] getBatchPositionAckSet(Position position) {
        // no-op, mock
        return new long[0];
    }

    @Override
    public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        // mock
        return 0;
    }

    @Override
    public void updateReadStats(int readEntriesCount, long readEntriesSize) {
        // no-op
    }
}
