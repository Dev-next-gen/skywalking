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

import java.time.Duration;
import java.util.function.Supplier;

/**
 * The result of a read the readers of {@link LookupCache} share, such as the list of all endpoints. It is read on the
 * calling thread, a background reader, when it is not read yet or was read {@code refreshInterval} ago or longer, so
 * a reader never builds on stale data; callers arriving meanwhile wait for that read. A read that fails is not kept:
 * the caller gets the empty value, and the next caller reads again.
 *
 * @param <V> the data read
 */
public final class CachedRead<V> {
    private final long refreshNanos;
    private final Supplier<V> reader;
    private final V empty;
    private final Ticker ticker;

    private V value;
    private long readNanos;

    /**
     * @param reader reads the data, returns null when the read failed
     * @param empty  what callers get when the read failed
     */
    public CachedRead(final Duration refreshInterval, final Supplier<V> reader, final V empty) {
        this(refreshInterval, reader, empty, Ticker.systemTicker());
    }

    CachedRead(final Duration refreshInterval, final Supplier<V> reader, final V empty, final Ticker ticker) {
        this.refreshNanos = refreshInterval.toNanos();
        this.reader = reader;
        this.empty = empty;
        this.ticker = ticker;
    }

    public synchronized V get() {
        if (value != null && ticker.read() - readNanos < refreshNanos) {
            return value;
        }
        value = reader.get();
        readNanos = ticker.read();
        return value == null ? empty : value;
    }
}
