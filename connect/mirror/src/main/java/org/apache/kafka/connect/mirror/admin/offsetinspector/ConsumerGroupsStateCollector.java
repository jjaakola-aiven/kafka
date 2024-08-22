/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.mirror.admin.offsetinspector;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.GroupFilter;
import org.apache.kafka.connect.mirror.MirrorCheckpointConfig;
import org.apache.kafka.connect.mirror.TopicFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public final class ConsumerGroupsStateCollector implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupsStateCollector.class);

    private final Duration operationTimeout;
    private final Map<String, Object> configuration;
    private AdminClient adminClient;
    private final MirrorCheckpointConfig mirrorCheckpointConfig;

    private ConsumerGroupsStateCollector(final Duration operationTimeout,
                                         final Map<String, Object> configuration,
                                         final MirrorCheckpointConfig mirrorCheckpointConfig) {
        this.operationTimeout = Objects.requireNonNull(operationTimeout);
        this.configuration = Collections.unmodifiableMap(Objects.requireNonNull(configuration));
        this.mirrorCheckpointConfig = mirrorCheckpointConfig;
    }

    private AdminClient getAdminClient() {
        if (adminClient == null) {
            adminClient = KafkaAdminClient.create(configuration);
        }
        return adminClient;
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close(operationTimeout);
        }
    }

    public Map<String, Map<TopicPartition, OffsetAndMetadata>> collectConsumerGroupsState() {
        final Collection<String> groupIds;
        try (final GroupFilter groupFilter = mirrorCheckpointConfig.groupFilter()) {
            groupIds = getAdminClient().listConsumerGroups()
                    .all()
                    .get(timeoutMs(), TimeUnit.MILLISECONDS)
                    .stream()
                    .map(ConsumerGroupListing::groupId)
                    .filter(groupFilter::shouldReplicateGroup)
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return getCommittedOffsets(groupIds);
    }

    private Map<String, Map<TopicPartition, OffsetAndMetadata>> getCommittedOffsets(final Collection<String> groupIds) {
        final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = new HashMap<>();
        final ListConsumerGroupOffsetsSpec emptyConsumerGroupSpecs = new ListConsumerGroupOffsetsSpec();
        for (final String groupId : groupIds) {
            if (groupSpecs.put(groupId, emptyConsumerGroupSpecs) != null) {
                throw new IllegalStateException("Duplicate key");
            }
        }
        final ListConsumerGroupOffsetsResult result = getAdminClient().listConsumerGroupOffsets(groupSpecs,
                new ListConsumerGroupOffsetsOptions().timeoutMs(timeoutMs()));
        final Map<String, Map<TopicPartition, OffsetAndMetadata>> groupData = new HashMap<>();
        for (final String groupId : groupIds) {
            try (final TopicFilter topicFilter = mirrorCheckpointConfig.topicFilter()) {
                final Map<TopicPartition, OffsetAndMetadata> filteredTopicPartitionOffsetAndMetadataMap =
                        result.partitionsToOffsetAndMetadata(groupId).get(timeoutMs(), TimeUnit.MILLISECONDS)
                                .entrySet().stream()
                                .filter(entry -> topicFilter.shouldReplicateTopic(entry.getKey().topic()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                groupData.put(groupId, filteredTopicPartitionOffsetAndMetadataMap);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("Interrupted when getting topic partition offsets and metadata.", e);
            }
        }
        return groupData;
    }

    private int timeoutMs() {
        return Math.toIntExact(operationTimeout.toMillis());
    }

    public static ConsumerGroupsStateCollectorBuilder builder() {
        return new ConsumerGroupsStateCollectorBuilder();
    }

    public static final class ConsumerGroupsStateCollectorBuilder {
        private Duration operationTimeout = Duration.ofMinutes(5);
        private final Map<String, Object> configuration = new HashMap<>();
        private MirrorCheckpointConfig mirrorCheckpointConfig;

        private ConsumerGroupsStateCollectorBuilder() {
            /* hide constructor */
        }

        public ConsumerGroupsStateCollectorBuilder withOperationTimeout(final Duration operationTimeout) {
            this.operationTimeout = operationTimeout;
            return this;
        }

        public ConsumerGroupsStateCollectorBuilder withAdminClientConfiguration(final Map<String, Object> configuration) {
            this.configuration.putAll(configuration);
            return this;
        }

        public ConsumerGroupsStateCollectorBuilder withMirrorCheckpointConfig(
                final MirrorCheckpointConfig mirrorCheckpointConfig) {
            this.mirrorCheckpointConfig = mirrorCheckpointConfig;
            return this;
        }

        public ConsumerGroupsStateCollector build() {
            this.configuration.put("default.api.timeout.ms", Math.toIntExact(operationTimeout.toMillis()));
            return new ConsumerGroupsStateCollector(operationTimeout, configuration, mirrorCheckpointConfig);
        }

    }
}
