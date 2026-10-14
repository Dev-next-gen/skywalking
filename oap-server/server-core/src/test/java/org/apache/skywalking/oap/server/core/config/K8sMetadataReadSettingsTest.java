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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class K8sMetadataReadSettingsTest {
    @Test
    public void readsTheSizeOfEachLane() {
        assertArrayEquals(new int[] {1000, 200}, K8sMetadataReadSettings.parseReadQueueSizes("als=1000,mal=200"));
        assertArrayEquals(new int[] {1000, 200}, K8sMetadataReadSettings.parseReadQueueSizes(" mal = 200 , als=1000 "));
    }

    @Test
    public void aLaneLeftOutHasNoLimit() {
        assertArrayEquals(new int[] {1000, -1}, K8sMetadataReadSettings.parseReadQueueSizes("als=1000"));
        assertArrayEquals(new int[] {-1, 200}, K8sMetadataReadSettings.parseReadQueueSizes("mal=200"));
        assertArrayEquals(new int[] {-1, -1}, K8sMetadataReadSettings.parseReadQueueSizes(""));
        assertArrayEquals(new int[] {-1, -1}, K8sMetadataReadSettings.parseReadQueueSizes(null));
    }

    @Test
    public void publishesTheSizeOfEachLane() {
        try {
            K8sMetadataReadSettings.set(1234, "als=0,mal=1");
            assertEquals(1234, K8sMetadataReadSettings.getMalReadTimeoutMillis());
            assertEquals(0, K8sMetadataReadSettings.getAlsReadQueueSize());
            assertEquals(1, K8sMetadataReadSettings.getMalReadQueueSize());
        } finally {
            K8sMetadataReadSettings.set(5000, "");
        }
    }

    @Test
    public void rejectsAnythingElse() {
        for (final String invalid : new String[] {"1000", "als=1000,", "ebpf=10", "als=ten", "als=1,als=2", "als"}) {
            assertThrows(IllegalArgumentException.class, () -> K8sMetadataReadSettings.parseReadQueueSizes(invalid),
                         invalid);
        }
    }
}
