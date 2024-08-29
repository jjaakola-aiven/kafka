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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.IdentityReplicationPolicy;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.admin.ConsumerGroupOffsetSyncInspector;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupOffsetsComparer;
import org.apache.kafka.connect.mirror.admin.offsetinspector.GroupAndState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests MM2 offset sync inspector tool.
 *
 * <p>MM2 is configured with active/passive replication between two Kafka clusters with {@link IdentityReplicationPolicy}.
 */
@Tag("integration")
public class OffsetSyncInspectorIntegrationTest extends MirrorConnectorsIntegrationSetupBase {
    @BeforeEach
    public void startClusters() throws Exception {
        super.startClusters(new HashMap<String, String>() {{
                put("replication.policy.class", IdentityReplicationPolicy.class.getName());
                // one way replication from primary to backup, add topic filter
                put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
                put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".enabled", "true");
                // enable automated consumer group offset sync
                put("sync.group.offsets.enabled", "true");
                put("sync.group.offsets.interval.seconds", "1");
            }});
    }

    @Test
    public void testOffsetSyncInspectionForFullySyncedGroup() throws Exception {
        // test-topic-1 is created by the base test class.
        final String testTopic1Name = "test-topic-1";
        final String consumerGroupTopic1 = "consumer-group-topic-1";
        final Map<String, Object> consumerGroupTopic1Props = new HashMap<String, Object>() {{
                put("group.id", consumerGroupTopic1);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopic1Consumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupTopic1Props, testTopic1Name)) {
            testTopic1Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic1Consumer.commitSync();

            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", testTopic1Name);
            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class),
                    mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is auto-created in the other cluster
            waitForTopicCreated(primary, testTopic1Name);
            waitForTopicCreated(backup, testTopic1Name);

            // Fill all partitions in the test-topic-1
            produceMessages(primaryProducer, testTopic1Name, NUM_PARTITIONS);

            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, testTopic1Name).count(),
                    "Records were not produced to primary cluster.");
            assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, testTopic1Name).count(),
                    "Records were not replicated to backup cluster.");

            testTopic1Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic1Consumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30), true);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult primaryToBackupResult =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

            final Collection<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> consumerGroupCompareResult
                    = primaryToBackupResult.getConsumerGroupsCompareResult().stream()
                    .filter(element -> element.getGroupId().equals(consumerGroupTopic1)).collect(Collectors.toList());
            assertEquals(NUM_PARTITIONS, consumerGroupCompareResult.size());
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                final ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result =
                        new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                            new GroupAndState(consumerGroupTopic1, ConsumerGroupState.STABLE),
                                new TopicPartition(testTopic1Name, partition),
                                ((Integer) NUM_RECORDS_PER_PARTITION).longValue(),
                                ((Integer) NUM_RECORDS_PER_PARTITION).longValue(),
                            true, "Target has offset sync.");
                assertTrue(consumerGroupCompareResult.contains(result),
                        String.format("Result %s not contained in %s", result, consumerGroupCompareResult));
            }

            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult backupToPrimaryResult =
                    clusterResults.get(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
            assertNull(backupToPrimaryResult);
        }
    }

    @Test
    public void testOffsetSyncInspectionForTopicHavingAnEmptyPartition() throws Exception {
        final String testTopic2Name = "test-topic-2-partition-0-filled";
        primary.kafka().createTopic(testTopic2Name, 2, 1, Collections.emptyMap(),
                Collections.singletonMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS));

        final String consumerGroupTopicPartition0Filled = "consumer-group-test-topic-2-partition-0-filled";
        final Map<String, Object> consumerGroupTopic2Props = new HashMap<String, Object>() {{
                put("group.id", consumerGroupTopicPartition0Filled);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopic2Consumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupTopic2Props, testTopic2Name)) {
            testTopic2Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic2Consumer.commitSync();

            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", testTopic2Name);
            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class),
                    mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is auto-created in the other cluster
            waitForTopicCreated(primary, testTopic2Name);
            waitForTopicCreated(backup, testTopic2Name);

            // Fill first partition of the test topic.
            produceMessages(primaryProducer, testTopic2Name, 1);

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, testTopic2Name).count(),
                    "Records were not produced to primary cluster.");
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, testTopic2Name).count(),
                    "Records were not replicated to backup cluster.");

            // Move the consumer group to end of the topic.
            testTopic2Consumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopic2Consumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30), false);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult primaryToBackupResult =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));

            final Collection<ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult> consumerGroupCompareResult
                    = primaryToBackupResult.getConsumerGroupsCompareResult();
            assertEquals(2, consumerGroupCompareResult.size());
            for (int partition = 0; partition < 2; partition++) {
                final Long expectedSourceOffset = partition == 0 ? ((Integer) NUM_RECORDS_PER_PARTITION).longValue() : 0L;
                final Long expectedTargetOffset = partition == 0 ? ((Integer) NUM_RECORDS_PER_PARTITION).longValue() : null;
                final String message = partition == 0 ? "Target has offset sync." : "Target consumer group missing the topic partition. Source partition is empty, offset not expected to be synced.";
                final ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult result =
                        new ConsumerGroupOffsetsComparer.ConsumerGroupCompareResult(
                                new GroupAndState(consumerGroupTopicPartition0Filled, ConsumerGroupState.STABLE),
                                new TopicPartition(testTopic2Name, partition),
                                expectedSourceOffset, expectedTargetOffset,
                                true, message);
                assertTrue(consumerGroupCompareResult.contains(result),
                        String.format("Result '%s' does not contain '%s'", consumerGroupCompareResult, result));
            }

            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult backupToPrimaryResult =
                    clusterResults.get(new SourceAndTarget(BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS));
            assertNull(backupToPrimaryResult);
        }
    }

    @Test
    public void testOffsetSyncInspectionNoResultForExcludedTopic() throws Exception {
        final String testTopic1Name = "test-topic-1";
        final String notMirroredTestTopicName = "not-mirrored-test-topic";
        primary.kafka().createTopic(notMirroredTestTopicName, 2, 1, Collections.emptyMap(),
                Collections.singletonMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_DURATION_MS));

        // warm up consumers before starting the connectors so we don't need to wait for discovery
        final String consumerGroupNotMirroredTopic = "consumer-group-not-mirrored-test-topic";
        final Map<String, Object> consumerGroupNotMirroredTopicProps = new HashMap<String, Object>() {{
                put("group.id", consumerGroupNotMirroredTopic);
                put("auto.offset.reset", "latest");
            }};

        try (final Consumer<byte[], byte[]> testTopicNotMirroredConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerGroupNotMirroredTopicProps, notMirroredTestTopicName)) {
            testTopicNotMirroredConsumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopicNotMirroredConsumer.commitSync();

            // one way replication from primary to backup, add topic filter
            mm2Props.put("offset.lag.max", "0");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics", ".*");
            mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".topics.blacklist", notMirroredTestTopicName);

            mm2Config = new MirrorMakerConfig(mm2Props);

            waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
            waitUntilMirrorMakerIsRunning(primary, Collections.singletonList(MirrorHeartbeatConnector.class), mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

            // make sure the topic is not auto-created in the other cluster
            waitForTopicCreated(primary, testTopic1Name);
            waitForTopicCreated(backup, testTopic1Name);
            waitForTopicCreated(primary, notMirroredTestTopicName);
            topicShouldNotBeCreated(backup, notMirroredTestTopicName);

            produceMessages(primaryProducer, notMirroredTestTopicName);

            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, notMirroredTestTopicName).count(),
                    "Records were not produced to primary cluster.");

            testTopicNotMirroredConsumer.poll(CONSUMER_POLL_TIMEOUT);
            testTopicNotMirroredConsumer.commitSync();

            // sleep a bit to have MM2 finish offset syncing
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            final ConsumerGroupOffsetSyncInspector consumerGroupOffsetSyncInspector = new ConsumerGroupOffsetSyncInspector();
            final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults =
                    consumerGroupOffsetSyncInspector.inspect(mm2Props, Duration.ofMinutes(1), Duration.ofSeconds(30), false);
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult result =
                    clusterResults.get(new SourceAndTarget(PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS));
            assertEquals(0, result.getConsumerGroupsCompareResult().stream().filter(element -> element.getGroupId().equals(consumerGroupNotMirroredTopic)).count());
        }
    }
}
