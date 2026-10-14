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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CachedReadTest {
    private final AtomicLong nanos = new AtomicLong(TimeUnit.HOURS.toNanos(1));
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicReference<List<String>> apiServer = new AtomicReference<>(List.of("a"));
    private final CachedRead<List<String>> cached = new CachedRead<>(Duration.ofMinutes(3), () -> {
        reads.incrementAndGet();
        return apiServer.get();
    }, Collections.emptyList(), new Ticker() {
        @Override
        public long read() {
            return nanos.get();
        }
    });

    @Test
    public void readsAgainOnlyOnceTheRefreshIntervalHasPassed() {
        assertEquals(List.of("a"), cached.get());
        apiServer.set(List.of("b"));
        nanos.addAndGet(TimeUnit.SECONDS.toNanos(179));
        assertEquals(List.of("a"), cached.get());
        assertEquals(1, reads.get());

        nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
        assertEquals(List.of("b"), cached.get());
        assertEquals(2, reads.get());
    }

    @Test
    public void aFailedReadIsNotKeptSoTheNextCallerReadsAgain() {
        apiServer.set(null);
        assertEquals(Collections.emptyList(), cached.get());

        apiServer.set(List.of("a"));
        assertEquals(List.of("a"), cached.get());
        assertEquals(2, reads.get());
    }
}
