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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class ConsumerGroupOffsetsComparer {

    private final Duration operationTimeout;
    private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets;
    private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets = new HashMap<>();
    private final TopicPartitionCache sourceTopicPartitionCache;
    private final boolean includeOkConsumerGroups;

    public ConsumerGroupOffsetsComparer(final Duration operationTimeout, final AdminClient sourceAdminClient,
                                        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets,
                                        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets,
                                        final boolean includeOkConsumerGroups) {
        this.operationTimeout = Objects.requireNonNull(operationTimeout);
        sourceTopicPartitionCache = new TopicPartitionCache(Objects.requireNonNull(sourceAdminClient));
        this.targetConsumerOffsets.putAll(Objects.requireNonNull(targetConsumerOffsets));
        this.sourceConsumerOffsets = Collections.unmodifiableMap(Objects.requireNonNull(sourceConsumerOffsets));
        this.includeOkConsumerGroups = includeOkConsumerGroups;
    }

    public ConsumerGroupsCompareResult compare() {
        final ConsumerGroupsCompareResult result = new ConsumerGroupsCompareResult();

        sourceConsumerOffsets.forEach((groupAndState, sourceOffsetData) -> {
            final Optional<Map<TopicPartition, OffsetAndMetadata>> maybeTargetGroupConsumerOffsets =
                    Optional.ofNullable(targetConsumerOffsets.remove(groupAndState));
            if (maybeTargetGroupConsumerOffsets.isPresent()) {
                final Map<TopicPartition, OffsetAndMetadata> targetOffsetData = maybeTargetGroupConsumerOffsets.get();
                sourceOffsetData.forEach((sourceTopicPartition, metadata) -> {
                    final Optional<OffsetAndMetadata> maybeTargetOffsetAndMetadata = Optional
                            .ofNullable(targetOffsetData.get(sourceTopicPartition));
                    final boolean targetOk;
                    final String message;
                    if (maybeTargetOffsetAndMetadata.isPresent()) {
                        final Long targetOffset = Optional.ofNullable(targetOffsetData.get(sourceTopicPartition))
                                .map(OffsetAndMetadata::offset)
                                .orElse(null);
                        if (targetOffset == null) {
                            // If no target offset, check if source partition is empty.
                            // If source partition is empty Mirrormaker does not sync offset.
                            final boolean isEmpty = sourceTopicPartitionCache.isEmpty(sourceTopicPartition,
                                    operationTimeout);
                            if (isEmpty) {
                                targetOk = true;
                                message = "Source partition is empty, offset not expected to be synced.";
                            } else {
                                targetOk = false;
                                message = "Source partition not empty, offset expected to be synced.";
                            }
                        } else {
                            // Target offset present, assume ok sync.
                            targetOk = true;
                            message = "Target has offset sync.";
                        }
                        if (includeOkConsumerGroups || !targetOk) {
                            result.addConsumerGroupCompareResult(new ConsumerGroupCompareResult(groupAndState,
                                    sourceTopicPartition, metadata.offset(), targetOffset, targetOk, message));
                        }
                    } else {
                        // If no target offset, check if source partition is empty.
                        // If source partition is empty Mirrormaker does not sync offset.
                        final boolean isEmpty = sourceTopicPartitionCache.isEmpty(sourceTopicPartition,
                                operationTimeout);
                        if (isEmpty) {
                            targetOk = true;
                            message = "Target consumer group missing the topic partition. Source partition is empty, offset not expected to be synced.";
                        } else {
                            targetOk = false;
                            message = "Target consumer group missing the topic partition. Source partition not empty, offset expected to be synced.";
                        }
                        if (includeOkConsumerGroups || !targetOk) {
                            result.addConsumerGroupCompareResult(
                                    new ConsumerGroupCompareResult(groupAndState,
                                            sourceTopicPartition, metadata.offset(), null, targetOk, message));
                        }
                    }
                });
            } else {
                // No group at target, add each partition to result without target offset.
                sourceOffsetData.forEach((sourceTopicPartition, metadata) -> result
                        .addConsumerGroupCompareResult(new ConsumerGroupCompareResult(groupAndState, sourceTopicPartition,
                                metadata.offset(), null, false, "Target missing the consumer group.")));
            }
        });

        // Target consumer offsets map is mutated by remove calls and leaves behind extra groups at target.
        result.setExtraAtTarget(targetConsumerOffsets);
        return result;
    }

    public static ConsumerGroupOffsetsComparerBuilder builder() {
        return new ConsumerGroupOffsetsComparerBuilder();
    }

    public static final class ConsumerGroupsCompareResult {

        private Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> extraAtTarget = Collections.emptyMap();
        private final Set<ConsumerGroupCompareResult> result = new HashSet<>();

        private ConsumerGroupsCompareResult() {
            /* hide constructor */
        }

        private void addConsumerGroupCompareResult(final ConsumerGroupCompareResult groupResult) {
            result.add(groupResult);
        }

        private void setExtraAtTarget(final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> extraAtTarget) {
            this.extraAtTarget = extraAtTarget;
        }

        public Set<ConsumerGroupCompareResult> getConsumerGroupsCompareResult() {
            return result;
        }

        public boolean isEmpty() {
            return result.isEmpty();
        }


        @Override
        public String toString() {
            return "ConsumerGroupsCompareResult{" + "extraAtTarget=" + extraAtTarget + ", result=" + result + '}';
        }
    }

    public static final class ConsumerGroupCompareResult {
        private final GroupAndState groupAndState;
        private final TopicPartition topicPartition;
        private final Long sourceOffset;
        private final Long targetOffset;
        private final boolean groupStateOk;
        private final String message;

        public ConsumerGroupCompareResult(final GroupAndState groupId, final TopicPartition topicPartition,
                                           final Long sourceOffset, final Long targetOffset, final boolean groupStateOk, final String message) {
            this.groupAndState = Objects.requireNonNull(groupId);
            this.topicPartition = topicPartition;
            this.sourceOffset = sourceOffset;
            this.targetOffset = targetOffset;
            this.groupStateOk = groupStateOk;
            this.message = message;
        }

        public String getGroupId() {
            return groupAndState.id();
        }

        public ConsumerGroupState getGroupState() {
            return groupAndState.state();
        }

        public TopicPartition getTopicPartition() {
            return this.topicPartition;
        }

        public Long getSourceOffset() {
            return sourceOffset;
        }

        public Long getTargetOffset() {
            return targetOffset;
        }

        public boolean isOk() {
            return groupStateOk;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConsumerGroupCompareResult result = (ConsumerGroupCompareResult) o;
            return groupStateOk == result.groupStateOk && Objects.equals(groupAndState, result.groupAndState) && Objects.equals(topicPartition, result.topicPartition) && Objects.equals(sourceOffset, result.sourceOffset) && Objects.equals(targetOffset, result.targetOffset) && Objects.equals(message, result.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupAndState, topicPartition, sourceOffset, targetOffset, groupStateOk, message);
        }

        @Override
        public String toString() {
            return "ConsumerGroupCompareResult{" +
                    "groupAndState=" + groupAndState +
                    ", topicPartition=" + topicPartition +
                    ", sourceOffset=" + sourceOffset +
                    ", targetOffset=" + targetOffset +
                    ", groupStateOk=" + groupStateOk +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

    public static final class ConsumerGroupOffsetsComparerBuilder {

        private Duration operationTimeout = Duration.ofMinutes(1);
        private AdminClient sourceAdminClient;
        private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets = new HashMap<>();
        private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets = new HashMap<>();
        private boolean withIncludeOkConsumerGroups = false;

        private ConsumerGroupOffsetsComparerBuilder() {
            /* hide constructor */
        }

        public ConsumerGroupOffsetsComparerBuilder withOperationTimeout(final Duration operationTimeout) {
            this.operationTimeout = operationTimeout;
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withSourceAdminClient(final AdminClient sourceAdminClient) {
            this.sourceAdminClient = sourceAdminClient;
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withSourceConsumerOffsets(
                final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets) {
            this.sourceConsumerOffsets.putAll(Objects.requireNonNull(sourceConsumerOffsets));
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withTargetConsumerOffsets(
                final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets) {
            this.targetConsumerOffsets.putAll(Objects.requireNonNull(targetConsumerOffsets));
            return this;
        }

        public ConsumerGroupOffsetsComparerBuilder withIncludeOkConsumerGroups(final boolean includeOkConsumerGroups) {
            this.withIncludeOkConsumerGroups = includeOkConsumerGroups;
            return this;
        }


        public ConsumerGroupOffsetsComparer build() {
            return new ConsumerGroupOffsetsComparer(operationTimeout, sourceAdminClient, sourceConsumerOffsets,
                    targetConsumerOffsets, withIncludeOkConsumerGroups);
        }

    }
}
