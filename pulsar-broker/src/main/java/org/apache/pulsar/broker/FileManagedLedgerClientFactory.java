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
package org.apache.pulsar.broker;

import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.FileManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileManagedLedgerClientFactory implements ManagedLedgerStorage {
    private static final Logger log = LoggerFactory.getLogger(FileManagedLedgerClientFactory.class);
    private static final String DEFAULT_STORAGE_CLASS_NAME = "file";
    private ManagedLedgerStorageClass defaultStorageClass;
    private ManagedLedgerFactory managedLedgerFactory;

    @Override
    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider,
                           EventLoopGroup eventLoopGroup,
                           OpenTelemetry openTelemetry) throws Exception {
        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(conf.getManagedLedgerCacheSizeMB() * 1024L * 1024L);
        managedLedgerFactoryConfig.setCacheEvictionWatermark(conf.getManagedLedgerCacheEvictionWatermark());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(conf.getManagedLedgerNumSchedulerThreads());
        managedLedgerFactoryConfig.setCacheEvictionIntervalMs(conf.getManagedLedgerCacheEvictionIntervalMs());
        managedLedgerFactoryConfig.setCacheEvictionTimeThresholdMillis(
                conf.getManagedLedgerCacheEvictionTimeThresholdMillis());
        managedLedgerFactoryConfig.setCopyEntriesInCache(conf.isManagedLedgerCacheCopyEntries());
        managedLedgerFactoryConfig.setManagedLedgerMaxReadsInFlightSize(
                conf.getManagedLedgerMaxReadsInFlightSizeInMB() * 1024L * 1024L);
        managedLedgerFactoryConfig.setPrometheusStatsLatencyRolloverSeconds(
                conf.getManagedLedgerPrometheusStatsLatencyRolloverSeconds());
        managedLedgerFactoryConfig.setTraceTaskExecution(conf.isManagedLedgerTraceTaskExecution());
        managedLedgerFactoryConfig.setCursorPositionFlushSeconds(conf.getManagedLedgerCursorPositionFlushSeconds());
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionType(conf.getManagedLedgerInfoCompressionType());
        managedLedgerFactoryConfig.setManagedLedgerInfoCompressionThresholdInBytes(
                conf.getManagedLedgerInfoCompressionThresholdInBytes());
        managedLedgerFactoryConfig.setStatsPeriodSeconds(conf.getManagedLedgerStatsPeriodSeconds());
        managedLedgerFactoryConfig.setManagedCursorInfoCompressionType(conf.getManagedCursorInfoCompressionType());
        managedLedgerFactoryConfig.setManagedCursorInfoCompressionThresholdInBytes(
                conf.getManagedCursorInfoCompressionThresholdInBytes());

        try {
            this.managedLedgerFactory =
                    new FileManagedLedgerFactoryImpl(managedLedgerFactoryConfig, metadataStore);
        } catch (Exception e) {
            throw e;
        }

        defaultStorageClass = new ManagedLedgerStorageClass() {
            @Override
            public String getName() {
                return DEFAULT_STORAGE_CLASS_NAME;
            }

            @Override
            public ManagedLedgerFactory getManagedLedgerFactory() {
                return managedLedgerFactory;
            }
        };
    }

    @Override
    public Collection<ManagedLedgerStorageClass> getStorageClasses() {
        return List.of(getDefaultStorageClass());
    }

    @Override
    public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
        if (name == null || DEFAULT_STORAGE_CLASS_NAME.equals(name)) {
            return Optional.of(getDefaultStorageClass());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public ManagedLedgerStorageClass getDefaultStorageClass() {
        return defaultStorageClass;
    }

    @Override
    public void close() throws IOException {
        try {
            if (null != managedLedgerFactory) {
                managedLedgerFactory.shutdown();
                log.info("Closed managed ledger factory");
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            throw new IOException(e);
        }
    }
}
