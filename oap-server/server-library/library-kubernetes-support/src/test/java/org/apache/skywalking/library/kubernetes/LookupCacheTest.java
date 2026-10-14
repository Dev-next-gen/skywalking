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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LookupCacheTest {
    private static final Duration REFRESH_INTERVAL = Duration.ofMinutes(3);

    private final AtomicLong nanos = new AtomicLong(TimeUnit.HOURS.toNanos(1));
    private final Ticker ticker = new Ticker() {
        @Override
        public long read() {
            return nanos.get();
        }
    };
    private final Queue<Runnable> readerThread = new ArrayDeque<>();
    private final Queue<Runnable> waitingReaderThread = new ArrayDeque<>();
    private final SerialReader serialReader = new SerialReader(readerThread::add, "background read queue");
    private final SerialReader waitingReader = new SerialReader(waitingReaderThread::add, "waiting read queue");
    private final Map<String, String> apiServer = new HashMap<>();
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger maxQueuedReads = new AtomicInteger(-1);
    private final LookupCache<String, String> cache = new LookupCache<>(
        REFRESH_INTERVAL, this::readFromApiServer, maxQueuedReads::get, maxQueuedReads::get, waitingReader, serialReader,
        ticker);

    @Test
    public void aLookupDoesNotWaitAndTheReadServesTheNextLookups() {
        apiServer.put("10.0.0.1", "web-1");

        assertEquals(Optional.empty(), cache.find("10.0.0.1"));
        assertEquals(Optional.empty(), cache.find("10.0.0.1"));
        assertEquals(0, reads.get());

        runReads();
        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1"));
        assertEquals(1, reads.get());
    }

    @Test
    public void anEntryUsedAfterTheRefreshIntervalIsReadAgainOnceWhileTheLookupsKeepGettingIt() {
        readFirst("10.0.0.1", "web-1");
        advance(REFRESH_INTERVAL);
        apiServer.put("10.0.0.1", "web-2");

        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1"));
        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1"));
        runReads();

        assertEquals(Optional.of("web-2"), cache.find("10.0.0.1"));
        assertEquals(2, reads.get());
    }

    @Test
    public void aReadAgainOfTheSameDataWritesTheEntryAgainSoAnEntryInUseDoesNotExpire() {
        readFirst("10.0.0.1", "web-1");
        advance(REFRESH_INTERVAL);
        cache.find("10.0.0.1");
        runReads();

        // Five minutes after the second write, eight after the first.
        advance(Duration.ofMinutes(5));
        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1"));
    }

    @Test
    public void anEntryNotUsedExpiresTwoRefreshIntervalsAfterItsLastWrite() {
        readFirst("10.0.0.1", "web-1");
        advance(REFRESH_INTERVAL.multipliedBy(2));

        assertEquals(Optional.empty(), cache.find("10.0.0.1"));
    }

    @Test
    public void aReadAgainThatFindsNothingDoesNotWriteSoTheEntryExpires() {
        readFirst("10.0.0.1", "web-1");
        advance(REFRESH_INTERVAL);
        apiServer.remove("10.0.0.1");
        cache.find("10.0.0.1");
        runReads();

        advance(Duration.ofMinutes(2));
        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1"));
        // Read again at most once per write.
        runReads();
        assertEquals(2, reads.get());

        advance(Duration.ofMinutes(1));
        assertEquals(Optional.empty(), cache.find("10.0.0.1"));
    }

    @Test
    public void aKeyHasOnePendingReadSoALookupAfterItsEntryExpiredSharesTheReadAgain() {
        maxQueuedReads.set(1);
        readFirst("10.0.0.1", "web-1");
        advance(REFRESH_INTERVAL);
        cache.find("10.0.0.1");
        advance(REFRESH_INTERVAL);
        apiServer.put("10.0.0.1", "web-2");

        // Expired, while its read again is still queued: the lookup shares it, the queue keeps one slot taken.
        assertEquals(Optional.empty(), cache.find("10.0.0.1"));
        cache.find("10.0.0.2");
        runReads();

        assertEquals(Optional.of("web-2"), cache.find("10.0.0.1"));
        assertEquals(2, reads.get());
    }

    @Test
    public void aTimedLookupWhoseReadTheFullQueueRefusesDoesNotWait() {
        maxQueuedReads.set(0);

        assertTimeoutPreemptively(
            Duration.ofSeconds(5), () -> assertEquals(Optional.empty(), cache.find("10.0.0.1", 10_000)));
        assertTrue(readerThread.isEmpty());
    }

    @Test
    public void aFirstReadThatFindsNothingIsCachedSoTheKeyIsNotReadOnEveryLookup() {
        cache.find("8.8.8.8");
        runReads();

        assertEquals(Optional.empty(), cache.find("8.8.8.8"));
        assertEquals(Optional.empty(), cache.find("8.8.8.8"));
        runReads();
        assertEquals(1, reads.get());
    }

    @Test
    public void aKeyFindingTheQueueFullIsReadOnALaterLookup() {
        maxQueuedReads.set(1);
        apiServer.put("10.0.0.1", "web-1");
        apiServer.put("10.0.0.2", "web-2");

        cache.find("10.0.0.1");
        cache.find("10.0.0.2");
        runReads();
        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1"));
        assertEquals(Optional.empty(), cache.find("10.0.0.2"));

        runReads();
        assertEquals(Optional.of("web-2"), cache.find("10.0.0.2"));
    }

    @Test
    public void aTimedLookupWaitsForTheReadOfAKeyNotReadYet() {
        final ExecutorService readers = Executors.newSingleThreadExecutor();
        try {
            apiServer.put("10.0.0.1", "web-1");
            final LookupCache<String, String> timed = new LookupCache<>(
                REFRESH_INTERVAL, this::readFromApiServer, () -> -1, () -> -1, new SerialReader(readers, "waiting"),
                new SerialReader(readers, "background"), ticker);

            assertEquals(Optional.of("web-1"), timed.find("10.0.0.1", 10_000));
        } finally {
            readers.shutdownNow();
        }
    }

    @Test
    public void aTimedLookupWaitsAtMostTheTimeoutSinceTheReadWasQueued() {
        final ExecutorService readers = Executors.newSingleThreadExecutor();
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final LookupCache<String, String> timed = new LookupCache<>(REFRESH_INTERVAL, key -> {
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "web-1";
            }, () -> -1, () -> -1, new SerialReader(readers, "waiting"), new SerialReader(readers, "background"), ticker);

            assertEquals(Optional.empty(), timed.find("10.0.0.1", 10));
            // Queued more than the timeout ago: a later lookup returns without waiting again.
            advance(Duration.ofSeconds(11));
            assertTimeoutPreemptively(
                Duration.ofSeconds(5), () -> assertEquals(Optional.empty(), timed.find("10.0.0.1", 10_000)));
        } finally {
            release.countDown();
            readers.shutdownNow();
        }
    }

    @Test
    public void aWaitingLookupDoesNotQueueBehindTheReadsNobodyWaitsFor() {
        apiServer.put("10.0.0.9", "mal-pod");
        for (int i = 0; i < 100; i++) {
            cache.find("10.1.0." + i);
        }

        // Its read goes to the waiting lane, which runs it while the background lane still holds 100 reads.
        cache.find("10.0.0.9", 0);
        assertEquals(1, waitingReaderThread.size());
        waitingReaderThread.poll().run();
        assertEquals(Optional.of("mal-pod"), cache.find("10.0.0.9"));
        assertEquals(1, reads.get());
    }

    @Test
    public void aWaitingLookupSharesTheReadAlreadyQueuedForItsKeyInTheOtherLane() {
        apiServer.put("10.0.0.1", "web-1");
        cache.find("10.0.0.1");

        assertEquals(Optional.empty(), cache.find("10.0.0.1", 0));
        assertTrue(waitingReaderThread.isEmpty());
        runReads();
        assertEquals(Optional.of("web-1"), cache.find("10.0.0.1", 0));
        assertEquals(1, reads.get());
    }

    @Test
    public void aWaitingLookupAfterItsEntryExpiredSharesTheQueuedReadAgain() {
        readFirst("10.0.0.1", "web-1");
        advance(REFRESH_INTERVAL);
        cache.find("10.0.0.1");
        advance(REFRESH_INTERVAL);
        apiServer.put("10.0.0.1", "web-2");

        assertEquals(Optional.empty(), cache.find("10.0.0.1", 0));
        assertTrue(waitingReaderThread.isEmpty());
        runReads();
        assertEquals(Optional.of("web-2"), cache.find("10.0.0.1", 0));
        assertEquals(2, reads.get());
    }

    @Test
    public void eachLaneHasItsOwnLimit() {
        final LookupCache<String, String> limited = new LookupCache<>(
            REFRESH_INTERVAL, this::readFromApiServer, () -> 1, () -> 0, waitingReader, serialReader, ticker);

        limited.find("10.0.0.1");
        limited.find("10.0.0.2", 0);
        assertTrue(readerThread.isEmpty());
        assertEquals(1, waitingReaderThread.size());
    }

    @Test
    public void aFaultOfTheReaderCountsAsFindingNothing() {
        final LookupCache<String, String> faulty = new LookupCache<>(REFRESH_INTERVAL, key -> {
            throw new IllegalStateException("bug");
        }, () -> -1, () -> -1, waitingReader, serialReader, ticker);

        faulty.find("10.0.0.1");
        runReads();
        assertEquals(Optional.empty(), faulty.find("10.0.0.1"));
        assertTrue(readerThread.isEmpty());
    }

    private String readFromApiServer(final String key) {
        reads.incrementAndGet();
        return apiServer.get(key);
    }

    private void readFirst(final String key, final String value) {
        apiServer.put(key, value);
        cache.find(key);
        runReads();
    }

    private void runReads() {
        for (Runnable task = readerThread.poll(); task != null; task = readerThread.poll()) {
            task.run();
        }
    }

    private void advance(final Duration duration) {
        nanos.addAndGet(duration.toNanos());
    }
}
