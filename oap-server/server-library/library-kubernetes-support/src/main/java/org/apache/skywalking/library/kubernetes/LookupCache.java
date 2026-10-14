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

package org.apache.skywalking.library.kubernetes;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * Caches what lookups find in the Kubernetes metadata, and reads it in the background, so a lookup does not wait for
 * the API server, or waits only up to the timeout it asks for.
 * <ul>
 *     <li>A key that is not cached is read once in the background; the lookups of the key share that read. A key has
 *     one pending read at most. The reads of every cache are queued to two lanes, each running its reads one at a
 *     time: one for the first reads a waiting lookup queues, and one for the rest. A lookup shares any read already
 *     pending for its key, including one in the other lane.</li>
 *     <li>An entry used after {@code refreshInterval} since it was written is read again in the background, once,
 *     while lookups keep getting it. A read that returns data, the same or not, writes the entry again.</li>
 *     <li>An entry expires {@code 2 * refreshInterval} after it was last written. So an entry that is not used, as
 *     it is not read again, and an entry whose read again found nothing, as it is not written, expire.</li>
 *     <li>A first read that finds nothing is cached, so a key matching nothing, such as an IP outside the cluster,
 *     is not read on every lookup.</li>
 * </ul>
 *
 * @param <K> the key
 * @param <V> the data found for a key
 */
@Slf4j
public final class LookupCache<K, V> {
    /**
     * The lane of the first reads queued by waiting lookups, apart from the reads nobody waits for.
     */
    private static final SerialReader WAITING_READS =
        new SerialReader(BackgroundExecutors.create("K8sLookup-waiting"), "waiting read queue");
    /**
     * The lane of the reads queued by lookups that do not wait, and of every read again.
     */
    private static final SerialReader BACKGROUND_READS =
        new SerialReader(BackgroundExecutors.create("K8sLookup"), "background read queue");

    private final long refreshNanos;
    private final Function<K, V> reader;
    private final IntSupplier maxWaitingReads;
    private final IntSupplier maxBackgroundReads;
    private final SerialReader waitingReads;
    private final SerialReader backgroundReads;
    private final Ticker ticker;
    private final Cache<K, Entry<V>> entries;
    private final ConcurrentMap<K, PendingRead<V>> pendingReads = new ConcurrentHashMap<>();

    /**
     * @param refreshInterval how long an entry is used before it is read again
     * @param reader          finds the data of a key, returns null when nothing is found; it runs in the background
     * @param maxWaitingReads    the most reads queued at once in the lane of the reads lookups wait for, or a negative
     *                           number for no limit; a key not queued as the lane is full is read on a later lookup
     * @param maxBackgroundReads the same for the lane of the other reads
     */
    public LookupCache(final Duration refreshInterval, final Function<K, V> reader,
                       final IntSupplier maxWaitingReads, final IntSupplier maxBackgroundReads) {
        this(refreshInterval, reader, maxWaitingReads, maxBackgroundReads, WAITING_READS, BACKGROUND_READS,
             Ticker.systemTicker());
    }

    LookupCache(final Duration refreshInterval, final Function<K, V> reader,
                final IntSupplier maxWaitingReads, final IntSupplier maxBackgroundReads,
                final SerialReader waitingReads, final SerialReader backgroundReads, final Ticker ticker) {
        this.refreshNanos = refreshInterval.toNanos();
        this.reader = reader;
        this.maxWaitingReads = maxWaitingReads;
        this.maxBackgroundReads = maxBackgroundReads;
        this.waitingReads = waitingReads;
        this.backgroundReads = backgroundReads;
        this.ticker = ticker;
        this.entries = CacheBuilder.newBuilder()
                                   .expireAfterWrite(refreshInterval.multipliedBy(2))
                                   .ticker(ticker)
                                   .build();
    }

    /**
     * Never waits: a key not cached yet is found by a later lookup, once its read has completed.
     */
    public Optional<V> find(final K key) {
        final Entry<V> entry = entries.getIfPresent(key);
        if (entry != null) {
            return use(key, entry);
        }
        final PendingRead<V> read = readInBackground(key, null, backgroundReads, maxBackgroundReads);
        return read.result.isDone() ? Optional.ofNullable(read.result.join()) : Optional.empty();
    }

    /**
     * Waits for a key not cached yet up to {@code timeoutMillis} since its read was queued, so lookups of a key whose
     * read is slow do not each wait the whole timeout.
     */
    public Optional<V> find(final K key, final long timeoutMillis) {
        final Entry<V> entry = entries.getIfPresent(key);
        if (entry != null) {
            return use(key, entry);
        }
        final PendingRead<V> read = readInBackground(key, null, waitingReads, maxWaitingReads);
        final long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis) - (ticker.read() - read.queuedNanos);
        if (remainingNanos <= 0 && !read.result.isDone()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(read.result.get(Math.max(remainingNanos, 0), TimeUnit.NANOSECONDS));
        } catch (final TimeoutException | ExecutionException e) {
            return Optional.empty();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    private Optional<V> use(final K key, final Entry<V> entry) {
        if (ticker.read() - entry.writtenNanos >= refreshNanos && entry.refreshing.compareAndSet(false, true)) {
            readInBackground(key, entry, backgroundReads, maxBackgroundReads);
        }
        return Optional.ofNullable(entry.value);
    }

    /**
     * Queues a read of {@code key}, unless one is pending: a key has one pending read at most, whether it reads the
     * key for the first time or again, so a lookup after the entry expired shares the read its last use queued.
     *
     * @param readAgain the entry the read is for, or null when the key is not cached
     * @param lane      where a read not pending yet is queued
     * @param maxQueued the size of {@code lane}
     * @return the pending read of {@code key}; it completes with nothing at once when the lane is full
     */
    private PendingRead<V> readInBackground(final K key, final Entry<V> readAgain, final SerialReader lane,
                                            final IntSupplier maxQueued) {
        final PendingRead<V> existing = pendingReads.get(key);
        if (existing != null) {
            return existing;
        }
        final PendingRead<V> created = new PendingRead<>(readAgain, ticker.read());
        final PendingRead<V> raced = pendingReads.putIfAbsent(key, created);
        if (raced != null) {
            return raced;
        }
        // A read completing between the lookup's miss and this registration has written the entry.
        final Entry<V> written = readAgain == null ? entries.getIfPresent(key) : null;
        if (written != null) {
            created.result.complete(written.value);
            pendingReads.remove(key, created);
            return created;
        }
        if (!lane.submit(() -> runRead(key, created), maxQueued.getAsInt())) {
            // Lookups sharing this read get nothing now, and a later lookup queues the key again.
            created.result.complete(null);
            pendingReads.remove(key, created);
            if (readAgain != null) {
                readAgain.refreshing.set(false);
            }
        }
        return created;
    }

    private void runRead(final K key, final PendingRead<V> pendingRead) {
        final V value = read(key);
        if (value != null) {
            // The only pending read of the key: no newer data can have been written meanwhile.
            entries.put(key, new Entry<>(value, ticker.read()));
        } else if (pendingRead.readAgain == null || entries.getIfPresent(key) != pendingRead.readAgain) {
            // Finding nothing is cached for a key not cached any more, and never replaces an entry written meanwhile.
            // Read again for an entry still cached, it is not written, so the entry expires.
            entries.asMap().putIfAbsent(key, new Entry<>(null, ticker.read()));
        }
        // Written before the read is released, so a lookup finds either the entry or the read.
        pendingRead.result.complete(value);
        pendingReads.remove(key, pendingRead);
    }

    private V read(final K key) {
        try {
            return reader.apply(key);
        } catch (final RuntimeException | LinkageError e) {
            // The readers report their API failures themselves and return null: this is a fault of the reader.
            log.warn("Failed to read {}.", key, e);
            return null;
        }
    }

    private static final class Entry<V> {
        /**
         * Null when the read found nothing.
         */
        private final V value;
        private final long writtenNanos;
        private final AtomicBoolean refreshing = new AtomicBoolean();

        private Entry(final V value, final long writtenNanos) {
            this.value = value;
            this.writtenNanos = writtenNanos;
        }
    }

    private static final class PendingRead<V> {
        private final CompletableFuture<V> result = new CompletableFuture<>();
        /**
         * The entry this read is for, or null when the key was not cached.
         */
        private final Entry<V> readAgain;
        private final long queuedNanos;

        private PendingRead(final Entry<V> readAgain, final long queuedNanos) {
            this.readAgain = readAgain;
            this.queuedNanos = queuedNanos;
        }
    }
}
