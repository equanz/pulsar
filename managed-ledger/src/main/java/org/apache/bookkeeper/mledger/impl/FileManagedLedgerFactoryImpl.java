/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.ManagedLedgerException.getManagedLedgerException;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.MetadataCompressionConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class FileManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final ManagedLedgerFactoryConfig config;
    @Getter
    private final OrderedScheduler scheduledExecutor;
    private final MetaStore store;
    private final ConcurrentHashMap<String, CompletableFuture<FileManagedLedgerImpl>> ledgers =
            new ConcurrentHashMap<>();
    private final File nextLedgerIdFile;
    private final AtomicLongWithSave nextLedgerIdCounter;

    public FileManagedLedgerFactoryImpl(ManagedLedgerFactoryConfig config,
                                        MetadataStoreExtended metadataStore) {
        this.config = config;
        MetadataCompressionConfig compressionConfigForManagedLedgerInfo =
                config.getCompressionConfigForManagedLedgerInfo();
        MetadataCompressionConfig compressionConfigForManagedCursorInfo =
                config.getCompressionConfigForManagedCursorInfo();
        this.scheduledExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(config.getNumManagedLedgerSchedulerThreads())
                .traceTaskExecution(config.isTraceTaskExecution())
                .name("file-ml-scheduler")
                .build();
        this.store = new MetaStoreImpl(metadataStore, scheduledExecutor,
                compressionConfigForManagedLedgerInfo,
                compressionConfigForManagedCursorInfo);

        new File("./data/fileml").mkdirs();

        // assume single process
        nextLedgerIdFile = new File("./data/fileml/nextLedgerId");
        if (nextLedgerIdFile.exists()) {
            try (FileInputStream nextLedgerIdInput = new FileInputStream(nextLedgerIdFile);
                 DataInputStream nextLedgerIdMarker = new DataInputStream(nextLedgerIdInput)) {
                nextLedgerIdCounter = new AtomicLongWithSave(nextLedgerIdMarker.readLong(), nextLedgerIdFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                nextLedgerIdCounter = new AtomicLongWithSave(0L, nextLedgerIdFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class AtomicLongWithSave extends AtomicLong implements Closeable {
        private final FileOutputStream nextLedgerIdOutput;
        private final DataOutputStream nextLedgerIdMarker;

        public AtomicLongWithSave(long initialValue, File dstFile) throws IOException {
            super(initialValue);
            nextLedgerIdOutput = new FileOutputStream(dstFile);
            nextLedgerIdMarker = new DataOutputStream(nextLedgerIdOutput);
        }

        public synchronized long getAndIncrementWithSave() throws IOException {
            final long result = getAndIncrement();
            nextLedgerIdMarker.writeLong(result + 1L);
            return result;
        }

        @Override
        public void close() throws IOException {
            nextLedgerIdMarker.writeLong(get());
            nextLedgerIdMarker.close();
            nextLedgerIdOutput.close();
        }
    }

    @Override
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
        return open(name, new ManagedLedgerConfig());
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedger l = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncOpen(name, config, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                r.l = ledger;
                latch.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.l;
    }

    @Override
    public void asyncOpen(String name, AsyncCallbacks.OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, new ManagedLedgerConfig(), callback, null, ctx);
    }

    @Override
    public void asyncOpen(String name, ManagedLedgerConfig config, AsyncCallbacks.OpenLedgerCallback callback,
                          Supplier<CompletableFuture<Boolean>> mlOwnershipChecker, Object ctx) {
        ledgers.computeIfAbsent(name, (mlName) -> {
                    final CompletableFuture<FileManagedLedgerImpl> future = new CompletableFuture<>();
                    try {
                        final FileManagedLedgerImpl ml = new FileManagedLedgerImpl(mlName, config, nextLedgerIdCounter,
                                store);
                        future.complete(ml);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }

                    return future;
                }).thenAccept(ml -> callback.openLedgerComplete(ml, ctx))
                .exceptionally((ex) -> {
                    callback.openLedgerFailed(ManagedLedgerException.getManagedLedgerException(
                            FutureUtil.unwrapCompletionException(ex)), ctx);
                    return null;
                });
    }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition,
                                             ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ReadOnlyCursor c = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncOpenReadOnlyCursor(managedLedgerName, startPosition, config,
                new AsyncCallbacks.OpenReadOnlyCursorCallback() {
                    @Override
                    public void openReadOnlyCursorComplete(ReadOnlyCursor cursor, Object ctx) {
                        r.c = cursor;
                        latch.countDown();
                    }

                    @Override
                    public void openReadOnlyCursorFailed(ManagedLedgerException exception, Object ctx) {
                        r.e = exception;
                        latch.countDown();
                    }
                }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.c;
    }

    @Override
    public void asyncOpenReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config,
                                        AsyncCallbacks.OpenReadOnlyCursorCallback callback, Object ctx) {
        callback.openReadOnlyCursorFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public void asyncOpenReadOnlyManagedLedger(String managedLedgerName,
                                               AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback,
                                               ManagedLedgerConfig config, Object ctx) {
        callback.openReadOnlyManagedLedgerFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerInfo info = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncGetManagedLedgerInfo(name, new AsyncCallbacks.ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
                r.info = info;
                latch.countDown();
            }

            @Override
            public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.info;
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, AsyncCallbacks.ManagedLedgerInfoCallback callback, Object ctx) {
        // TODO: impl
    }

    @Override
    public void delete(String name) throws InterruptedException, ManagedLedgerException {
        delete(name, CompletableFuture.completedFuture(null));
    }

    @Override
    public void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncDelete(name, mlConfigFuture, new AsyncCallbacks.DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                latch.countDown();
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
    }

    @Override
    public void asyncDelete(String name, AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        asyncDelete(name, CompletableFuture.completedFuture(null), callback, ctx);
    }

    @Override
    public void asyncDelete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                            AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        // TODO: impl
        callback.deleteLedgerFailed(
                ManagedLedgerException.getManagedLedgerException(new UnsupportedOperationException()), ctx);
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        try {
            shutdownAsync().get();
        } catch (ExecutionException e) {
            throw getManagedLedgerException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> shutdownAsync() throws ManagedLedgerException, InterruptedException {
        // TODO: impl
        try {
            nextLedgerIdCounter.close();
            return CompletableFuture.completedFuture(null);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> asyncExists(String ledgerName) {
        // TODO: impl
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public EntryCacheManager getEntryCacheManager() {
        // TODO: impl
        return null;
    }

    @Override
    public void updateCacheEvictionTimeThreshold(long cacheEvictionTimeThresholdNanos) {
        // no-op
    }

    @Override
    public long getCacheEvictionTimeThreshold() {
        // mock
        return 0;
    }

    @Override
    public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
        return store.getManagedLedgerPropertiesAsync(name);
    }

    @Override
    public Map<String, ManagedLedger> getManagedLedgers() {
        // TODO: impl
        return Map.of();
    }

    final ManagedLedgerFactoryMXBean mockStats = new ManagedLedgerFactoryMXBean() {
        @Override
        public int getNumberOfManagedLedgers() {
            return 0;
        }

        @Override
        public long getCacheUsedSize() {
            return 0;
        }

        @Override
        public long getCacheMaxSize() {
            return 0;
        }

        @Override
        public double getCacheHitsRate() {
            return 0;
        }

        @Override
        public long getCacheHitsTotal() {
            return 0;
        }

        @Override
        public double getCacheMissesRate() {
            return 0;
        }

        @Override
        public long getCacheMissesTotal() {
            return 0;
        }

        @Override
        public double getCacheHitsThroughput() {
            return 0;
        }

        @Override
        public long getCacheHitsBytesTotal() {
            return 0;
        }

        @Override
        public double getCacheMissesThroughput() {
            return 0;
        }

        @Override
        public long getCacheMissesBytesTotal() {
            return 0;
        }

        @Override
        public long getNumberOfCacheEvictions() {
            return 0;
        }

        @Override
        public long getNumberOfCacheEvictionsTotal() {
            return 0;
        }

        @Override
        public long getCacheInsertedEntriesCount() {
            return 0;
        }

        @Override
        public long getCacheEvictedEntriesCount() {
            return 0;
        }

        @Override
        public long getCacheEntriesCount() {
            return 0;
        }
    };

    @Override
    public ManagedLedgerFactoryMXBean getCacheStats() {
        return mockStats;
    }

    @Override
    public void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats, TopicName topicName,
                                             boolean accurate, Object ctx) throws Exception {
        // no-op
    }

    @Override
    public ManagedLedgerFactoryConfig getConfig() {
        return config;
    }
}
