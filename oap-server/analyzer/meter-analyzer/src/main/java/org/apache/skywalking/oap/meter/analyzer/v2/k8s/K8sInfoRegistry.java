/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.meter.analyzer.v2.k8s;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import org.apache.skywalking.library.kubernetes.KubernetesPods;
import org.apache.skywalking.library.kubernetes.KubernetesServices;
import org.apache.skywalking.library.kubernetes.LookupCache;
import org.apache.skywalking.library.kubernetes.ObjectID;
import org.apache.skywalking.oap.server.core.config.K8sMetadataReadSettings;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Resolves pods and services for MAL and the eBPF access log. The lookups MAL makes wait for a key not read yet up to
 * {@link K8sMetadataReadSettings#getMalReadTimeoutMillis()}; the lookups ending in {@code WithoutWait}, for the eBPF access log, never wait and
 * find a key not read yet on a later lookup. Either way the key is read in the background.
 */
public class K8sInfoRegistry {
    private final static K8sInfoRegistry INSTANCE = new K8sInfoRegistry();
    private final LookupCache<ObjectID /* Pod */, ObjectID /* Service */> podServiceMap;
    private final LookupCache<String/* podIP */, ObjectID /* Pod */> ipPodMap;
    private final LookupCache<String/* serviceIP */, ObjectID /* Service */> ipServiceMap;

    private K8sInfoRegistry() {
        ipPodMap = new LookupCache<>(Duration.ofMinutes(3), ip -> KubernetesPods.INSTANCE
            .findByIP(ip)
            .map(it -> ObjectID
                .builder()
                .name(it.getMetadata().getName())
                .namespace(it.getMetadata().getNamespace())
                .build())
            .orElse(null), K8sMetadataReadSettings::getMalReadQueueSize,
            K8sMetadataReadSettings::getAlsReadQueueSize);

        ipServiceMap = new LookupCache<>(Duration.ofMinutes(3), ip -> KubernetesServices.INSTANCE
            .list()
            .stream()
            .filter(it -> it.getSpec() != null)
            .filter(it -> it.getStatus() != null)
            .filter(it -> it.getMetadata() != null)
            .filter(it -> (it.getSpec().getClusterIPs() != null &&
                it.getSpec().getClusterIPs().stream()
                    .anyMatch(clusterIP -> Objects.equals(clusterIP, ip)))
                || (it.getStatus().getLoadBalancer() != null &&
                    it.getStatus().getLoadBalancer().getIngress() != null &&
                    it.getStatus().getLoadBalancer().getIngress().stream()
                        .anyMatch(ingress -> Objects.equals(ingress.getIp(), ip))))
            .map(it -> ObjectID
                .builder()
                .name(it.getMetadata().getName())
                .namespace(it.getMetadata().getNamespace())
                .build())
            .findFirst()
            .orElse(null), K8sMetadataReadSettings::getMalReadQueueSize,
            K8sMetadataReadSettings::getAlsReadQueueSize);

        podServiceMap = new LookupCache<>(Duration.ofMinutes(3), podObjectID -> {
            final Optional<Pod> pod = KubernetesPods.INSTANCE
                .findByObjectID(
                    ObjectID
                        .builder()
                        .name(podObjectID.name())
                        .namespace(podObjectID.namespace())
                        .build());

            if (!pod.isPresent()
                || pod.get().getMetadata() == null
                || pod.get().getMetadata().getLabels() == null) {
                return null;
            }

            final Optional<Service> service = KubernetesServices.INSTANCE
                .list()
                .stream()
                .filter(it -> it.getMetadata() != null)
                .filter(it -> Objects.equals(it.getMetadata().getNamespace(), pod.get().getMetadata().getNamespace()))
                .filter(it -> it.getSpec() != null)
                .filter(it -> requireNonNull(it.getSpec()).getSelector() != null)
                .filter(it -> !it.getSpec().getSelector().isEmpty())
                .filter(it -> {
                    final Map<String, String> labels = pod.get().getMetadata().getLabels();
                    final Map<String, String> selector = it.getSpec().getSelector();
                    return hasIntersection(selector.entrySet(), labels.entrySet());
                })
                .findFirst();
            if (!service.isPresent()) {
                return null;
            }
            return ObjectID
                .builder()
                .name(service.get().getMetadata().getName())
                .namespace(service.get().getMetadata().getNamespace())
                .build();
        }, K8sMetadataReadSettings::getMalReadQueueSize,
            K8sMetadataReadSettings::getAlsReadQueueSize);
    }

    public static K8sInfoRegistry getInstance() {
        return INSTANCE;
    }

    public String findServiceName(String namespace, String podName) {
        return findService(namespace, podName).toString();
    }

    public ObjectID findService(String namespace, String podName) {
        return podServiceMap.find(podObjectID(namespace, podName), K8sMetadataReadSettings.getMalReadTimeoutMillis())
                            .orElse(ObjectID.EMPTY);
    }

    public ObjectID findPodByIP(String ip) {
        return ipPodMap.find(ip, K8sMetadataReadSettings.getMalReadTimeoutMillis()).orElse(ObjectID.EMPTY);
    }

    public ObjectID findServiceByIP(String ip) {
        return ipServiceMap.find(ip, K8sMetadataReadSettings.getMalReadTimeoutMillis()).orElse(ObjectID.EMPTY);
    }

    public ObjectID findServiceWithoutWait(String namespace, String podName) {
        return podServiceMap.find(podObjectID(namespace, podName)).orElse(ObjectID.EMPTY);
    }

    public ObjectID findPodByIPWithoutWait(String ip) {
        return ipPodMap.find(ip).orElse(ObjectID.EMPTY);
    }

    public ObjectID findServiceByIPWithoutWait(String ip) {
        return ipServiceMap.find(ip).orElse(ObjectID.EMPTY);
    }

    private static ObjectID podObjectID(final String namespace, final String podName) {
        return ObjectID
            .builder()
            .name(podName)
            .namespace(namespace)
            .build();
    }

    private boolean hasIntersection(Collection<?> o, Collection<?> c) {
        Objects.requireNonNull(o);
        Objects.requireNonNull(c);
        for (final Object value : o) {
            if (!c.contains(value)) {
                return false;
            }
        }
        return true;
    }
}
