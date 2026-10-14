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

package org.apache.skywalking.oap.server.core.config;

/**
 * The core settings of the Kubernetes metadata reads, which the readers, in the receivers and the meter analyzer,
 * get from here: the core module that loads them cannot reach the readers.
 */
public final class K8sMetadataReadSettings {
    private static final int NO_LIMIT = -1;

    private static volatile long MAL_READ_TIMEOUT_MILLIS = 5000;
    private static volatile int ALS_READ_QUEUE_SIZE = NO_LIMIT;
    private static volatile int MAL_READ_QUEUE_SIZE = NO_LIMIT;

    private K8sMetadataReadSettings() {
    }

    /**
     * @return how long a MAL lookup, such as {@code retagByK8sMeta}, waits for metadata not read yet, from
     * {@code core.default.malMetadataReadTimeout}
     */
    public static long getMalReadTimeoutMillis() {
        return MAL_READ_TIMEOUT_MILLIS;
    }

    /**
     * @return the most reads waiting at once for the lookups that do not wait (Envoy ALS, the eBPF access log) and
     * for every read again, or a negative number for no limit: {@code als} of
     * {@code core.default.k8sMetadataReadQueueSize}
     */
    public static int getAlsReadQueueSize() {
        return ALS_READ_QUEUE_SIZE;
    }

    /**
     * @return the most reads waiting at once for the MAL lookups, which wait, or a negative number for no limit:
     * {@code mal} of {@code core.default.k8sMetadataReadQueueSize}
     */
    public static int getMalReadQueueSize() {
        return MAL_READ_QUEUE_SIZE;
    }

    /**
     * @param readQueueSizes {@code als=<size>,mal=<size>}; either may be left out, for no limit
     * @throws IllegalArgumentException when {@code readQueueSizes} is not in that form
     */
    public static void set(final long malReadTimeoutMillis, final String readQueueSizes) {
        final int[] sizes = parseReadQueueSizes(readQueueSizes);
        MAL_READ_TIMEOUT_MILLIS = malReadTimeoutMillis;
        ALS_READ_QUEUE_SIZE = sizes[0];
        MAL_READ_QUEUE_SIZE = sizes[1];
    }

    /**
     * @return the {@code als} and {@code mal} sizes, {@link #NO_LIMIT} for one left out
     */
    static int[] parseReadQueueSizes(final String readQueueSizes) {
        final int[] sizes = {NO_LIMIT, NO_LIMIT};
        if (readQueueSizes == null || readQueueSizes.trim().isEmpty()) {
            return sizes;
        }
        final boolean[] given = new boolean[2];
        for (final String part : readQueueSizes.split(",", -1)) {
            final int equals = part.indexOf('=');
            if (equals < 0) {
                throw invalid(readQueueSizes);
            }
            final int lane;
            switch (part.substring(0, equals).trim()) {
                case "als":
                    lane = 0;
                    break;
                case "mal":
                    lane = 1;
                    break;
                default:
                    throw invalid(readQueueSizes);
            }
            if (given[lane]) {
                throw invalid(readQueueSizes);
            }
            given[lane] = true;
            try {
                sizes[lane] = Integer.parseInt(part.substring(equals + 1).trim());
            } catch (final NumberFormatException e) {
                throw invalid(readQueueSizes);
            }
        }
        return sizes;
    }

    private static IllegalArgumentException invalid(final String readQueueSizes) {
        return new IllegalArgumentException(
            "core.default.k8sMetadataReadQueueSize must be als=<size>,mal=<size>, either may be left out, -1 for no "
                + "limit, but is: " + readQueueSizes);
    }
}
