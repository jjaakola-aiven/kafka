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
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.GroupFilter;
import org.apache.kafka.connect.mirror.MirrorCheckpointConfig;
import org.apache.kafka.connect.mirror.TopicFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public final class ConsumerGroupsStateCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupsStateCollector.class);

    private final Duration adminTimeout;
    private final AdminClient adminClient;
    private final MirrorCheckpointConfig mirrorCheckpointConfig;
    private final boolean includeInactiveGroups;

    private ConsumerGroupsStateCollector(final Duration adminTimeout,
                                         final AdminClient adminClient,
                                         final MirrorCheckpointConfig mirrorCheckpointConfig,
                                         final boolean includeInactiveGroups
    ) {
        this.adminTimeout = Objects.requireNonNull(adminTimeout);
        this.adminClient = adminClient;
        this.mirrorCheckpointConfig = mirrorCheckpointConfig;
        this.includeInactiveGroups = includeInactiveGroups;
    }

    public Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> collectConsumerGroupsState() {
        final Collection<String> groupIds;
        try (final GroupFilter groupFilter = mirrorCheckpointConfig.groupFilter()) {
            groupIds = adminClient.listConsumerGroups()
                    .all()
                    .get(timeoutMs(), TimeUnit.MILLISECONDS)
                    .stream()
                    .map(ConsumerGroupListing::groupId)
                    .filter(groupId -> {
                        final boolean shouldReplicate = groupFilter.shouldReplicateGroup(groupId);
                        LOGGER.debug("Group filter, replicate {} -> {}", groupId, shouldReplicate);
                        return shouldReplicate;
                    })
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        final Collection<GroupAndState> selectedGroupIds = new HashSet<>();

        adminClient.describeConsumerGroups(groupIds).describedGroups().forEach((key, value) -> {
            try {
                LOGGER.debug("Describing group {}", key);
                final ConsumerGroupDescription consumerGroupDescription = value
                        .get(timeoutMs(), TimeUnit.MILLISECONDS);
                final ConsumerGroupState groupState = consumerGroupDescription.state();
                final GroupAndState groupAndState = new GroupAndState(key, groupState);
                if (includeInactiveGroups) {
                    selectedGroupIds.add(groupAndState);
                } else {
                    switch (groupState) {
                        case STABLE:
                        case ASSIGNING:
                        case RECONCILING:
                        case PREPARING_REBALANCE:
                        case COMPLETING_REBALANCE:
                            selectedGroupIds.add(groupAndState);
                            return;
                        default:
                            LOGGER.debug("Group {} is in state {} and not inspected.", key, groupState);
                    }
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error(String.format("Interrupted when filtering for groups to inspect on %s.", key), e);
            }
        });

        LOGGER.debug("Proposed groups: {}", groupIds);
        LOGGER.debug("Selected groups: {}", selectedGroupIds);

        return getCommittedOffsets(selectedGroupIds);
    }

    public Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> getCommittedOffsets(final Collection<GroupAndState> groupIdsAndStates) {
        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> groupData = new HashMap<>();
        try (final TopicFilter topicFilter = mirrorCheckpointConfig.topicFilter()) {
            for (GroupAndState groupAndState : groupIdsAndStates) {
                final Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = new HashMap<>();
                groupSpecs.put(groupAndState.id(), new ListConsumerGroupOffsetsSpec());
                final ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupSpecs,
                        new ListConsumerGroupOffsetsOptions().timeoutMs(timeoutMs()));
                final Map<TopicPartition, OffsetAndMetadata> filteredTopicPartitionOffsetAndMetadataMap =
                        result.partitionsToOffsetAndMetadata(groupAndState.id()).get(timeoutMs(), TimeUnit.MILLISECONDS)
                                .entrySet().stream()
                                .filter(entry -> {
                                    final String topicName = entry.getKey().topic();
                                    final boolean shouldReplicateTopic = topicFilter.shouldReplicateTopic(topicName);
                                    LOGGER.debug("Topic filter, replicate group {} with topic: {} -> {}",
                                            groupAndState.id(), topicName, shouldReplicateTopic);
                                    return shouldReplicateTopic;
                                })
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                groupData.put(groupAndState, filteredTopicPartitionOffsetAndMetadataMap);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.error(
                    String.format("Interrupted or timeout of %s when getting topic partition offsets and metadata.",
                            adminTimeout), e);
        }
        return groupData;
    }

    private int timeoutMs() {
        return Math.toIntExact(adminTimeout.toMillis());
    }

    public static ConsumerGroupsStateCollectorBuilder builder() {
        return new ConsumerGroupsStateCollectorBuilder();
    }

    public static final class ConsumerGroupsStateCollectorBuilder {
        private Duration adminTimeout = Duration.ofMinutes(1);
        private AdminClient adminClient;
        private MirrorCheckpointConfig mirrorCheckpointConfig;
        private boolean includeInactiveGroups = true;

        private ConsumerGroupsStateCollectorBuilder() {
            /* hide constructor */
        }

        public ConsumerGroupsStateCollectorBuilder withAdminTimeout(final Duration adminTimeout) {
            this.adminTimeout = adminTimeout;
            return this;
        }

        public ConsumerGroupsStateCollectorBuilder withAdminClient(final AdminClient adminClient) {
            this.adminClient = adminClient;
            return this;
        }

        public ConsumerGroupsStateCollectorBuilder withMirrorCheckpointConfig(
                final MirrorCheckpointConfig mirrorCheckpointConfig) {
            this.mirrorCheckpointConfig = mirrorCheckpointConfig;
            return this;
        }

        public ConsumerGroupsStateCollectorBuilder includeInactiveGroups(final boolean includeInactiveGroups) {
            this.includeInactiveGroups = includeInactiveGroups;
            return this;
        }

        public ConsumerGroupsStateCollector build() {
            return new ConsumerGroupsStateCollector(adminTimeout, adminClient, mirrorCheckpointConfig, includeInactiveGroups);
        }

    }
}
