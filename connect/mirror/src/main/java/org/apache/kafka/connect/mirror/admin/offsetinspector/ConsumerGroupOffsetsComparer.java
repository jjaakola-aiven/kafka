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
import org.apache.kafka.connect.mirror.OffsetSync;
import org.apache.kafka.connect.mirror.OffsetSyncStore;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
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
    private final TopicPartitionCache targetTopicPartitionCache;
    private final boolean includeOkConsumerGroups;
    private final OffsetSyncStore offsetSyncStore;

    public ConsumerGroupOffsetsComparer(final Duration operationTimeout,
                                        final AdminClient sourceAdminClient, final AdminClient targetAdminClient,
                                        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets,
                                        final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets,
                                        final boolean includeOkConsumerGroups, final OffsetSyncStore offsetSyncStore) {
        this.operationTimeout = Objects.requireNonNull(operationTimeout);
        sourceTopicPartitionCache = new TopicPartitionCache(Objects.requireNonNull(sourceAdminClient));
        targetTopicPartitionCache = new TopicPartitionCache(Objects.requireNonNull(targetAdminClient));
        this.targetConsumerOffsets.putAll(Objects.requireNonNull(targetConsumerOffsets));
        this.sourceConsumerOffsets = Collections.unmodifiableMap(Objects.requireNonNull(sourceConsumerOffsets));
        this.includeOkConsumerGroups = includeOkConsumerGroups;
        this.offsetSyncStore = offsetSyncStore;
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
                        final Long targetLag;
                        final Long lagAtTargetToSource;
                        if (targetOffset == null) {
                            // No target offset, cannot determine target lag.
                            targetLag = null;
                            lagAtTargetToSource = null;
                            // If no target offset, check if source partition is empty.
                            // If source partition is empty Mirrormaker does not sync offset.
                            final boolean isEmpty = sourceTopicPartitionCache.isEmpty(sourceTopicPartition,
                                    operationTimeout);
                            if (isEmpty) {
                                targetOk = true;
                                message = "Source partition is empty therefore offset not expected to be synced.";
                            } else {
                                targetOk = false;
                                message = "Source partition not empty therefore offset expected to be synced.";
                            }
                        } else {
                            // If target lag is negative, round back to 0.
                            final long targetLatestOffset = targetTopicPartitionCache.latestOffset(sourceTopicPartition, operationTimeout);
                            targetLag =  targetLatestOffset >= targetOffset
                                    ? targetLatestOffset - targetOffset
                                    : 0;

                            final long sourceLatestOffset = sourceTopicPartitionCache.latestOffset(sourceTopicPartition, operationTimeout);
                            final Optional<OffsetSync> maybeOffsetSync = offsetSyncStore.latestOffsetSync(sourceTopicPartition, sourceLatestOffset);
                            if (maybeOffsetSync.isPresent()) {
                                final OffsetSync offsetSync = maybeOffsetSync.get();
                                final long sourceAndTargetOffsetDifference = offsetSync.upstreamOffset() - offsetSync.downstreamOffset();
                                lagAtTargetToSource = sourceLatestOffset - (targetOffset + sourceAndTargetOffsetDifference);
                            } else {
                                // no offset for downstream
                                lagAtTargetToSource = null;
                            }

                            // Target offset present, assume ok sync.
                            targetOk = true;
                            message = "Target has offset sync.";
                        }
                        if (includeOkConsumerGroups || !targetOk) {
                            result.addConsumerGroupCompareResult(new ConsumerGroupCompareResult(groupAndState,
                                    sourceTopicPartition, metadata.offset(), lagAtTargetToSource, targetOffset, targetLag, targetOk, message));
                        }
                    } else {
                        // If no target offset, check if source partition is empty.
                        // If source partition is empty Mirrormaker does not sync offset.
                        final boolean isEmpty = sourceTopicPartitionCache.isEmpty(sourceTopicPartition,
                                operationTimeout);
                        if (isEmpty) {
                            targetOk = true;
                            message = "Target consumer group missing the topic partition. Source partition is empty therefore offset not expected to be synced.";
                        } else {
                            targetOk = false;
                            message = "Target consumer group missing the topic partition. Source partition not empty therefore offset expected to be synced.";
                        }
                        if (includeOkConsumerGroups || !targetOk) {
                            result.addConsumerGroupCompareResult(
                                    new ConsumerGroupCompareResult(groupAndState,
                                            sourceTopicPartition, metadata.offset(), null, null, null, targetOk, message));
                        }
                    }
                });
            } else {
                // No group at target, add each partition to result without target offset.
                sourceOffsetData.forEach((sourceTopicPartition, metadata) -> result
                        .addConsumerGroupCompareResult(new ConsumerGroupCompareResult(groupAndState, sourceTopicPartition,
                                metadata.offset(), null, null, null, false, "Target missing the consumer group.")));
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

    public static final class ConsumerGroupCompareResult implements Comparable<ConsumerGroupCompareResult> {
        private static final Comparator<ConsumerGroupCompareResult> COMPARATOR = Comparator
                .comparing(ConsumerGroupCompareResult::getGroupId, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(ConsumerGroupCompareResult::getTopic, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(ConsumerGroupCompareResult::getPartition);
        private final GroupAndState groupAndState;
        private final TopicPartition topicPartition;
        private final Long sourceOffset;
        private final Long lagAtTargetToSource;
        private final Long targetOffset;
        private final Long targetLag;
        private final boolean groupStateOk;
        private final String message;

        public ConsumerGroupCompareResult(final GroupAndState groupId, final TopicPartition topicPartition,
                                          final Long sourceOffset, final Long lagAtTargetToSource,
                                          final Long targetOffset, final Long targetLag,
                                          final boolean groupStateOk, final String message) {
            this.groupAndState = Objects.requireNonNull(groupId);
            this.topicPartition = topicPartition;
            this.sourceOffset = sourceOffset;
            this.lagAtTargetToSource = lagAtTargetToSource;
            this.targetOffset = targetOffset;
            this.targetLag = targetLag;
            this.groupStateOk = groupStateOk;
            this.message = message;
        }

        public String getGroupId() {
            return groupAndState.id();
        }

        public ConsumerGroupState getGroupState() {
            return groupAndState.state();
        }

        public String getTopic() {
            return this.topicPartition.topic();
        }

        public int getPartition() {
            return this.topicPartition.partition();
        }

        public Long getSourceOffset() {
            return sourceOffset;
        }

        public Long getLagAtTargetToSource() {
            return lagAtTargetToSource;
        }

        public Long getTargetOffset() {
            return targetOffset;
        }

        public Long getTargetLag() {
            return targetLag;
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
            return groupStateOk == result.groupStateOk
                    && Objects.equals(groupAndState, result.groupAndState)
                    && Objects.equals(topicPartition, result.topicPartition)
                    && Objects.equals(sourceOffset, result.sourceOffset)
                    && Objects.equals(lagAtTargetToSource, result.lagAtTargetToSource)
                    && Objects.equals(targetOffset, result.targetOffset)
                    && Objects.equals(targetLag, result.targetLag)
                    && Objects.equals(message, result.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupAndState, topicPartition, sourceOffset, lagAtTargetToSource,
                    targetOffset, targetLag, groupStateOk, message);
        }

        @Override
        public String toString() {
            return "ConsumerGroupCompareResult{" +
                    "groupAndState=" + groupAndState +
                    ", topicPartition=" + topicPartition +
                    ", sourceOffset=" + sourceOffset +
                    ", lagAtTargetToSource=" + lagAtTargetToSource +
                    ", targetOffset=" + targetOffset +
                    ", targetLag=" + targetLag +
                    ", groupStateOk=" + groupStateOk +
                    ", message='" + message + '\'' +
                    '}';
        }

        @Override
        public int compareTo(final ConsumerGroupCompareResult other) {
            return COMPARATOR.compare(this, other);
        }
    }

    public static final class ConsumerGroupOffsetsComparerBuilder {

        private Duration operationTimeout = Duration.ofMinutes(1);
        private AdminClient sourceAdminClient;
        private AdminClient targetAdminClient;
        private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets = new HashMap<>();
        private final Map<GroupAndState, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets = new HashMap<>();
        private boolean withIncludeOkConsumerGroups = false;
        private OffsetSyncStore offsetSyncStore;

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

        public ConsumerGroupOffsetsComparerBuilder withTargetAdminClient(final AdminClient targetAdminClient) {
            this.targetAdminClient = targetAdminClient;
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

        public ConsumerGroupOffsetsComparerBuilder withOffsetSyncStore(final OffsetSyncStore offsetSyncStore) {
            this.offsetSyncStore = offsetSyncStore;
            return this;
        }

        public ConsumerGroupOffsetsComparer build() {
            return new ConsumerGroupOffsetsComparer(operationTimeout, sourceAdminClient, targetAdminClient,
                    sourceConsumerOffsets, targetConsumerOffsets, withIncludeOkConsumerGroups, offsetSyncStore);
        }

    }
}
