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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.mirror.Checkpoint;
import org.apache.kafka.connect.mirror.DefaultConfigPropertyFilter;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorConnectorConfig;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.test.TestCondition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.OFFSET_SYNCS_CLIENT_ROLE_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.OFFSET_SYNCS_TOPIC_CONFIG_PREFIX;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests MM2 replication and failover/failback logic.
 *
 * MM2 is configured with active/active replication between two Kafka clusters. Tests validate that
 * records sent to either cluster arrive at the other cluster. Then, a consumer group is migrated from
 * one cluster to the other and back. Tests validate that consumer offsets are translated and replicated
 * between clusters during this failover and failback.
 */
@Tag("integration")
public class MirrorConnectorsIntegrationBaseTest extends MirrorConnectorsIntegrationSetupBase {
    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationBaseTest.class);

    @Test
    public void testReplication() throws Exception {
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        if (replicateBackupToPrimary) {
            produceMessages(backupProducer, "test-topic-1");
        }
        String reverseTopic1 = remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS);
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);
        
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        List<Class<? extends Connector>> primaryConnectors = replicateBackupToPrimary ? CONNECTOR_LIST : Collections.singletonList(MirrorHeartbeatConnector.class);
        waitUntilMirrorMakerIsRunning(primary, primaryConnectors, mm2Config, BACKUP_CLUSTER_ALIAS, PRIMARY_CLUSTER_ALIAS);

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig(PRIMARY_CLUSTER_ALIAS));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));

        waitForTopicCreated(primary, "mm2-offset-syncs.backup.internal");

        TestCondition assertBackupTopicConfig = () -> {
            String compactPolicy = getTopicConfig(backup.kafka(), backupTopic1, TopicConfig.CLEANUP_POLICY_CONFIG);
            assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, compactPolicy, "topic config was not synced");
            return true;
        };

        if (replicateBackupToPrimary) {
            // make sure the topics are auto-created in the other cluster
            waitForTopicCreated(primary, reverseTopic1);
            waitForTopicCreated(backup, backupTopic1);
            assertBackupTopicConfig.conditionMet();
        } else {
            // The backup and reverse topics are identical to the topics we created while setting up the test;
            // we don't have to wait for them to be created, but there might be a small delay between
            // now and when MM2 is able to sync the config for the backup topic
            waitForCondition(
                    assertBackupTopicConfig,
                    10_000, // Topic config sync interval is one second; this should be plenty of time
                    "topic config was not synced in time"
            );
        }

        createAndTestNewTopicWithConfigFilter();

        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, backupTopic1).count(),
            "Records were not replicated to backup cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
            "Records were not produced to backup cluster.");
        if (replicateBackupToPrimary) {
            assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, reverseTopic1).count(),
                    "Records were not replicated to primary cluster.");
            assertEquals(NUM_RECORDS_PRODUCED * 2, primary.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, reverseTopic1, "test-topic-1").count(),
                "Primary cluster doesn't have all records from both clusters.");
            assertEquals(NUM_RECORDS_PRODUCED * 2, backup.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, backupTopic1, "test-topic-1").count(),
                "Backup cluster doesn't have all records from both clusters.");
        }

        assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to primary cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0,
            "Heartbeats were not emitted to backup cluster.");
        assertTrue(backup.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "primary.heartbeats").count() > 0,
            "Heartbeats were not replicated downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primary.kafka().consume(1, RECORD_TRANSFER_DURATION_MS, "backup.heartbeats").count() > 0,
                    "Heartbeats were not replicated downstream to primary cluster.");
        }
        
        assertTrue(backupClient.upstreamClusters().contains(PRIMARY_CLUSTER_ALIAS), "Did not find upstream primary cluster.");
        assertEquals(1, backupClient.replicationHops(PRIMARY_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
        assertTrue(backup.kafka().consume(1, CHECKPOINT_DURATION_MS, "primary.checkpoints.internal").count() > 0,
            "Checkpoints were not emitted downstream to backup cluster.");
        if (replicateBackupToPrimary) {
            assertTrue(primaryClient.upstreamClusters().contains(BACKUP_CLUSTER_ALIAS), "Did not find upstream backup cluster.");
            assertEquals(1, primaryClient.replicationHops(BACKUP_CLUSTER_ALIAS), "Did not calculate replication hops correctly.");
            assertTrue(primary.kafka().consume(1, CHECKPOINT_DURATION_MS, "backup.checkpoints.internal").count() > 0,
                    "Checkpoints were not emitted upstream to primary cluster.");
        }

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = waitForCheckpointOnAllPartitions(
                backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, backupTopic1);

        // Failover consumer group to backup cluster.
        try (Consumer<byte[], byte[]> primaryConsumer = backup.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
            primaryConsumer.assign(backupOffsets.keySet());
            backupOffsets.forEach(primaryConsumer::seek);
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT);
            primaryConsumer.commitAsync();

            assertTrue(primaryConsumer.position(new TopicPartition(backupTopic1, 0)) > 0, "Consumer failedover to zero offset.");
            assertTrue(primaryConsumer.position(
                new TopicPartition(backupTopic1, 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedover beyond expected offset.");
        }

        assertMonotonicCheckpoints(backup, "primary.checkpoints.internal");
 
        primaryClient.close();
        backupClient.close();

        if (replicateBackupToPrimary) {
            Map<TopicPartition, OffsetAndMetadata> primaryOffsets = waitForCheckpointOnAllPartitions(
                    primaryClient, consumerGroupName, BACKUP_CLUSTER_ALIAS, reverseTopic1);

            // Failback consumer group to primary cluster
            try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumer(Collections.singletonMap("group.id", consumerGroupName))) {
                primaryConsumer.assign(primaryOffsets.keySet());
                primaryOffsets.forEach(primaryConsumer::seek);
                primaryConsumer.poll(CONSUMER_POLL_TIMEOUT);
                primaryConsumer.commitAsync();

                assertTrue(primaryConsumer.position(new TopicPartition(reverseTopic1, 0)) > 0, "Consumer failedback to zero downstream offset.");
                assertTrue(primaryConsumer.position(
                        new TopicPartition(reverseTopic1, 0)) <= NUM_RECORDS_PRODUCED, "Consumer failedback beyond expected downstream offset.");
            }
        }

        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        String backupTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS);

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(backup, backupTopic2);

        // only produce messages to the first partition
        produceMessages(primaryProducer, "test-topic-2", 1);

        // expect total consumed messages equals to NUM_RECORDS_PER_PARTITION
        assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count(),
            "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, backupTopic2).count(),
            "New topic was not replicated to backup cluster.");

        if (replicateBackupToPrimary) {
            backup.kafka().createTopic("test-topic-3", NUM_PARTITIONS);
            String reverseTopic3 = remoteTopicName("test-topic-3", BACKUP_CLUSTER_ALIAS);
            waitForTopicCreated(primary, reverseTopic3);
            produceMessages(backupProducer, "test-topic-3", 1);
            assertEquals(NUM_RECORDS_PER_PARTITION, backup.kafka().consume(NUM_RECORDS_PER_PARTITION, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count(),
                    "Records were not produced to backup cluster.");

            assertEquals(NUM_RECORDS_PER_PARTITION, primary.kafka().consume(NUM_RECORDS_PER_PARTITION, 2 * RECORD_TRANSFER_DURATION_MS, reverseTopic3).count(),
                    "New topic was not replicated to primary cluster.");
        }
    }

    @Test
    public void testReplicationWithEmptyPartition() throws Exception {
        String consumerGroupName = "consumer-group-testReplicationWithEmptyPartition";
        Map<String, Object> consumerProps  = Collections.singletonMap("group.id", consumerGroupName);

        // create topic
        String topic = "test-topic-with-empty-partition";
        primary.kafka().createTopic(topic, NUM_PARTITIONS);

        // produce to all test-topic-empty's partitions, except the last partition
        produceMessages(primaryProducer, topic, NUM_PARTITIONS - 1);

        // consume before starting the connectors, so we don't need to wait for discovery
        int expectedRecords = NUM_RECORDS_PER_PARTITION * (NUM_PARTITIONS - 1);
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, topic)) {
            waitForConsumingAllRecords(primaryConsumer, expectedRecords);
        }

        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // sleep few seconds to have MM2 finish replication so that "end" consumer will consume some record
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        // consume all records from backup cluster
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(consumerProps,
                backupTopic)) {
            waitForConsumingAllRecords(backupConsumer, expectedRecords);
        }

        try (Admin backupClient = backup.kafka().createAdminClient()) {
            // retrieve the consumer group offset from backup cluster
            Map<TopicPartition, OffsetAndMetadata> remoteOffsets =
                backupClient.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata().get();

            // pinpoint the offset of the last partition which does not receive records
            OffsetAndMetadata offset = remoteOffsets.get(new TopicPartition(backupTopic, NUM_PARTITIONS - 1));
            // offset of the last partition should exist, but its value should be 0
            assertNotNull(offset, "Offset of last partition was not replicated");
            assertEquals(0, offset.offset(), "Offset of last partition is not zero");
        }
    }

    @Test
    public void testOneWayReplicationWithAutoOffsetSync() throws InterruptedException {
        testOneWayReplicationWithOffsetSyncs(OFFSET_LAG_MAX);
    }

    @Test
    public void testOneWayReplicationWithFrequentOffsetSyncs() throws InterruptedException {
        testOneWayReplicationWithOffsetSyncs(0);
    }

    private void testOneWayReplicationWithOffsetSyncs(int offsetLagMax) throws InterruptedException {
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        String consumerGroupName = "consumer-group-testOneWayReplicationWithAutoOffsetSync";
        Map<String, Object> consumerProps  = new HashMap<String, Object>() {{
                put("group.id", consumerGroupName);
                put("auto.offset.reset", "earliest");
            }};
        // create consumers before starting the connectors, so we don't need to wait for discovery
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, 
                "test-topic-1")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }

        // enable automated consumer group offset sync
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        mm2Props.put("offset.lag.max", Integer.toString(offsetLagMax));
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");


        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topic is created in the primary cluster only
        String reverseTopic1 = remoteTopicName("test-topic-1", BACKUP_CLUSTER_ALIAS);
        if (!"test-topic-1".equals(reverseTopic1)) {
            topicShouldNotBeCreated(primary, reverseTopic1);
        }
        waitForTopicCreated(backup, backupTopic1);
        // create a consumer at backup cluster with same consumer group Id to consume 1 topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(
            consumerProps, backupTopic1)) {

            waitForConsumerGroupFullSync(backup, Collections.singletonList(backupTopic1),
                    consumerGroupName, NUM_RECORDS_PRODUCED, offsetLagMax);
            assertDownstreamRedeliveriesBoundedByMaxLag(backupConsumer, offsetLagMax);
        }

        // now create a new topic in primary cluster
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        // make sure the topic is created in backup cluster
        String remoteTopic2 = remoteTopicName("test-topic-2", PRIMARY_CLUSTER_ALIAS);
        waitForTopicCreated(backup, remoteTopic2);

        // produce some records to the new topic in primary cluster
        produceMessages(primaryProducer, "test-topic-2");

        // create a consumer at primary cluster to consume the new topic
        try (Consumer<byte[], byte[]> consumer1 = primary.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", consumerGroupName), "test-topic-2")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(consumer1, NUM_RECORDS_PRODUCED);
        }

        // create a consumer at backup cluster with same consumer group ID to consume old and new topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
            "group.id", consumerGroupName), backupTopic1, remoteTopic2)) {

            waitForConsumerGroupFullSync(backup, Arrays.asList(backupTopic1, remoteTopic2),
                    consumerGroupName, NUM_RECORDS_PRODUCED, offsetLagMax);
            assertDownstreamRedeliveriesBoundedByMaxLag(backupConsumer, offsetLagMax);
        }

        assertMonotonicCheckpoints(backup, PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal");
    }

    @Test
    public void testReplicationWithoutOffsetSyncWillNotCreateOffsetSyncsTopic() throws Exception {
        produceMessages(primaryProducer, "test-topic-1");
        String backupTopic1 = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        if (replicateBackupToPrimary) {
            produceMessages(backupProducer, "test-topic-1");
        }
        String consumerGroupName = "consumer-group-testReplication";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        mm2Props.keySet().removeIf(prop -> prop.startsWith(OFFSET_SYNCS_CLIENT_ROLE_PREFIX) || prop.startsWith(OFFSET_SYNCS_TOPIC_CONFIG_PREFIX));
        mm2Props.put(MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED, "false");

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, Arrays.asList(MirrorSourceConnector.class, MirrorHeartbeatConnector.class), mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig(PRIMARY_CLUSTER_ALIAS));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));

        // make sure the topic is auto-created in the other cluster
        waitForTopicCreated(backup, backupTopic1);

        assertEquals(NUM_RECORDS_PRODUCED, primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
                "Records were not produced to primary cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, backupTopic1).count(),
                "Records were not replicated to backup cluster.");
        assertEquals(NUM_RECORDS_PRODUCED, backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count(),
                "Records were not produced to backup cluster.");

        List<TopicDescription> offsetSyncTopic = primary.kafka().describeTopics("mm2-offset-syncs.backup.internal").values()
                .stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        assertTrue(offsetSyncTopic.isEmpty());

        primaryClient.close();
        backupClient.close();
    }

    @Test
    public void testOffsetSyncsTopicsOnTarget() throws Exception {
        // move offset-syncs topics to target
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + "->" + BACKUP_CLUSTER_ALIAS + ".offset-syncs.topic.location", "target");
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // Ensure the offset syncs topic is created in the target cluster
        waitForTopicCreated(backup, "mm2-offset-syncs." + PRIMARY_CLUSTER_ALIAS + ".internal");

        String consumerGroupName = "consumer-group-syncs-on-target";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);

        produceMessages(primaryProducer, "test-topic-1");

        warmUpConsumer(consumerProps);

        String remoteTopic = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);

        // Check offsets are pushed to the checkpoint topic
        try (Consumer<byte[], byte[]> backupConsumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "auto.offset.reset", "earliest"), PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal")) {
            waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = backupConsumer.poll(Duration.ofSeconds(1L));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    Checkpoint checkpoint = Checkpoint.deserializeRecord(record);
                    if (remoteTopic.equals(checkpoint.topicPartition().topic())) {
                        return true;
                    }
                }
                return false;
            }, 30_000,
                "Unable to find checkpoints for " + PRIMARY_CLUSTER_ALIAS + ".test-topic-1"
            );
        }

        assertMonotonicCheckpoints(backup, PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal");

        // Ensure no offset-syncs topics have been created on the primary cluster
        try (Admin adminClient = primary.kafka().createAdminClient()) {
            Set<String> primaryTopics = adminClient.listTopics().names().get();
            assertFalse(primaryTopics.contains("mm2-offset-syncs." + PRIMARY_CLUSTER_ALIAS + ".internal"));
            assertFalse(primaryTopics.contains("mm2-offset-syncs." + BACKUP_CLUSTER_ALIAS + ".internal"));
        }
    }

    @Test
    public void testNoCheckpointsIfNoRecordsAreMirrored() throws InterruptedException {
        String consumerGroupName = "consumer-group-no-checkpoints";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);

        // ensure there are some records in the topic on the source cluster
        produceMessages(primaryProducer, "test-topic-1");

        // warm up consumers before starting the connectors, so we don't need to wait for discovery
        warmUpConsumer(consumerProps);

        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // make sure the topics  are created in the backup cluster
        waitForTopicCreated(backup, remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS));
        waitForTopicCreated(backup, remoteTopicName("test-topic-no-checkpoints", PRIMARY_CLUSTER_ALIAS));

        // commit some offsets for both topics in the source cluster
        TopicPartition tp1 = new TopicPartition("test-topic-1", 0);
        TopicPartition tp2 = new TopicPartition("test-topic-no-checkpoints", 0);
        try (Consumer<byte[], byte[]> consumer = primary.kafka().createConsumer(consumerProps)) {
            Collection<TopicPartition> tps = Arrays.asList(tp1, tp2);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = endOffsets.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> new OffsetAndMetadata(e.getValue())
                            ));
            consumer.commitSync(offsetsToCommit);
        }

        // Only test-topic-1 should have translated offsets because we've not yet mirrored any records for test-topic-no-checkpoints
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));
        waitForCondition(() -> {
            Map<TopicPartition, OffsetAndMetadata> translatedOffsets = backupClient.remoteConsumerOffsets(
                    consumerGroupName, PRIMARY_CLUSTER_ALIAS, Duration.ofSeconds(30L));
            return translatedOffsets.containsKey(remoteTopicPartition(tp1, PRIMARY_CLUSTER_ALIAS)) &&
                   !translatedOffsets.containsKey(remoteTopicPartition(tp2, PRIMARY_CLUSTER_ALIAS));
        }, OFFSET_SYNC_DURATION_MS, "Checkpoints were not emitted correctly to backup cluster");

        // Send some records to test-topic-no-checkpoints in the source cluster
        produceMessages(primaryProducer, "test-topic-no-checkpoints");

        try (Consumer<byte[], byte[]> consumer = primary.kafka().createConsumer(consumerProps)) {
            Collection<TopicPartition> tps = Arrays.asList(tp1, tp2);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = endOffsets.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> new OffsetAndMetadata(e.getValue())
                    ));
            consumer.commitSync(offsetsToCommit);
        }

        waitForCondition(() -> {
            Map<TopicPartition, OffsetAndMetadata> translatedOffsets = backupClient.remoteConsumerOffsets(
                    consumerGroupName, PRIMARY_CLUSTER_ALIAS, Duration.ofSeconds(30L));
            return translatedOffsets.containsKey(remoteTopicPartition(tp1, PRIMARY_CLUSTER_ALIAS)) &&
                   translatedOffsets.containsKey(remoteTopicPartition(tp2, PRIMARY_CLUSTER_ALIAS));
        }, OFFSET_SYNC_DURATION_MS, "Checkpoints were not emitted correctly to backup cluster");

        backupClient.close();
    }

    @Test
    public void testRestartReplication() throws InterruptedException {
        String consumerGroupName = "consumer-group-restart";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        String remoteTopic = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        warmUpConsumer(consumerProps);
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        mm2Props.put("offset.lag.max", Integer.toString(OFFSET_LAG_MAX));
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        produceMessages(primaryProducer, "test-topic-1");
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }
        waitForConsumerGroupFullSync(backup, Collections.singletonList(remoteTopic), consumerGroupName, NUM_RECORDS_PRODUCED, OFFSET_LAG_MAX);
        restartMirrorMakerConnectors(backup, CONNECTOR_LIST);
        assertMonotonicCheckpoints(backup, "primary.checkpoints.internal");
        Thread.sleep(5000);
        produceMessages(primaryProducer, "test-topic-1");
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            waitForConsumingAllRecords(primaryConsumer, NUM_RECORDS_PRODUCED);
        }
        waitForConsumerGroupFullSync(backup, Collections.singletonList(remoteTopic), consumerGroupName, 2 * NUM_RECORDS_PRODUCED, OFFSET_LAG_MAX);
        assertMonotonicCheckpoints(backup, "primary.checkpoints.internal");
    }

    @Test
    public void testOffsetTranslationBehindReplicationFlow() throws InterruptedException {
        String consumerGroupName = "consumer-group-lagging-behind";
        Map<String, Object> consumerProps = Collections.singletonMap("group.id", consumerGroupName);
        String remoteTopic = remoteTopicName("test-topic-1", PRIMARY_CLUSTER_ALIAS);
        warmUpConsumer(consumerProps);
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        mm2Props.put("offset.lag.max", Integer.toString(OFFSET_LAG_MAX));
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);
        // Produce and consume an initial batch of records to establish an initial checkpoint
        produceMessages(primaryProducer, "test-topic-1");
        warmUpConsumer(consumerProps);
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig(BACKUP_CLUSTER_ALIAS));
        Map<TopicPartition, OffsetAndMetadata> initialCheckpoints = waitForCheckpointOnAllPartitions(
                backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, remoteTopic);
        // Produce a large number of records to the topic, all replicated within one MM2 lifetime.
        int iterations = 100;
        for (int i = 1; i < iterations; i++) {
            produceMessages(primaryProducer, "test-topic-1");
        }
        waitForTopicCreated(backup, remoteTopic);
        assertEquals(iterations * NUM_RECORDS_PRODUCED, backup.kafka().consume(iterations * NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, remoteTopic).count(),
                "Records were not replicated to backup cluster.");
        // Once the replication has finished, we spin up the upstream consumer again and start consuming records
        ConsumerRecords<byte[], byte[]> allRecords = primary.kafka().consume(iterations * NUM_RECORDS_PRODUCED, RECORD_CONSUME_DURATION_MS, "test-topic-1");
        Map<TopicPartition, OffsetAndMetadata> partialCheckpoints;
        log.info("Initial checkpoints: {}", initialCheckpoints);
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT);
            primaryConsumer.commitSync(partialOffsets(allRecords, 0.9f));
            partialCheckpoints = waitForNewCheckpointOnAllPartitions(
                    backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, remoteTopic, initialCheckpoints);
            log.info("Partial checkpoints: {}", partialCheckpoints);
        }

        for (TopicPartition tp : initialCheckpoints.keySet()) {
            assertTrue(initialCheckpoints.get(tp).offset() < partialCheckpoints.get(tp).offset(),
                    "Checkpoints should advance when the upstream consumer group advances");
        }

        assertMonotonicCheckpoints(backup, PRIMARY_CLUSTER_ALIAS + ".checkpoints.internal");

        Map<TopicPartition, OffsetAndMetadata> finalCheckpoints;
        try (Consumer<byte[], byte[]> primaryConsumer = primary.kafka().createConsumerAndSubscribeTo(consumerProps, "test-topic-1")) {
            primaryConsumer.poll(CONSUMER_POLL_TIMEOUT);
            primaryConsumer.commitSync(partialOffsets(allRecords, 0.1f));
            finalCheckpoints = waitForNewCheckpointOnAllPartitions(
                    backupClient, consumerGroupName, PRIMARY_CLUSTER_ALIAS, remoteTopic, partialCheckpoints);
            log.info("Final checkpoints: {}", finalCheckpoints);
        }

        backupClient.close();

        for (TopicPartition tp : partialCheckpoints.keySet()) {
            assertTrue(finalCheckpoints.get(tp).offset() < partialCheckpoints.get(tp).offset(),
                    "Checkpoints should rewind when the upstream consumer group rewinds");
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> partialOffsets(ConsumerRecords<byte[], byte[]> allRecords, double fraction) {
        return allRecords.partitions()
                .stream()
                .collect(Collectors.toMap(Function.identity(), partition -> {
                    List<ConsumerRecord<byte[], byte[]>> records = allRecords.records(partition);
                    int index = (int) (records.size() * fraction);
                    return new OffsetAndMetadata(records.get(index).offset());
                }));
    }

    @Test
    public void testSyncTopicConfigs() throws InterruptedException {
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // create topic with configuration to test:
        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("delete.retention.ms", "1000"); // should be excluded (default value is 86400000)
        topicConfig.put("retention.bytes", "1000"); // should be included, default value is -1

        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, topicConfig);
        waitForTopicCreated(backup, backupTopic);

        // alter configs on the target topic
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, backupTopic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry("delete.retention.ms", "2000"), AlterConfigOp.OpType.SET));
        ops.add(new AlterConfigOp(new ConfigEntry("retention.bytes", "2000"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        // alter configs on target cluster
        backup.kafka().incrementalAlterConfigs(configOps);

        waitForCondition(() -> {
            String primaryConfig, backupConfig;
            primaryConfig = getTopicConfig(primary.kafka(), topic, "delete.retention.ms");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "delete.retention.ms");
            assertNotEquals(primaryConfig, backupConfig,
                    "`delete.retention.ms` should be different, because it's in exclude filter! ");
            assertEquals("2000", backupConfig, "`delete.retention.ms` should be 2000, because it's explicitly defined on the target topic! ");

            // regression test for the config that are still supposed to be replicated
            primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals(primaryConfig, backupConfig,
                    "`retention.bytes` should be the same, because it isn't in exclude filter! ");
            return true;
        }, 30000, "Topic configurations were not synced");
    }

    @Test
    public void testReplicateSourceDefault() throws Exception {
        mm2Props.put(DefaultConfigPropertyFilter.USE_DEFAULTS_FROM, "source");
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // create topic with default configurations to test
        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, new HashMap<>());
        waitForTopicCreated(backup, backupTopic);

        // alter target topic configurations
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, backupTopic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry("delete.retention.ms", "2000"), AlterConfigOp.OpType.SET));
        ops.add(new AlterConfigOp(new ConfigEntry("retention.bytes", "2000"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        backup.kafka().incrementalAlterConfigs(configOps);

        waitForCondition(() -> {
            String primaryConfig, backupConfig;
            // altered configuration of the target topic should be synced with the source cluster's default
            primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals(primaryConfig, backupConfig,
                    "`retention.bytes` should be the same, because the source cluster default is being used! ");
            assertEquals("-1", backupConfig,
                    "`retention.bytes` should be synced with default value!");

            // when using the source cluster's default, the excluded configuration should still not be changed
            primaryConfig = getTopicConfig(primary.kafka(), topic, "delete.retention.ms");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "delete.retention.ms");
            assertNotEquals(primaryConfig, backupConfig,
                    "`delete.retention.ms` should be different, because it's in exclude filter! ");
            assertEquals("2000", backupConfig, "`delete.retention.ms` should be 2000, because it's explicitly defined on the target topic! ");
            return true;
        }, 30000, "Topic configurations were not synced");
    }

    @Test
    public void testReplicateTargetDefault() throws Exception {
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        // create topic with configuration to test:
        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("retention.bytes", "1000");

        final String topic = "test-topic-with-config";
        final String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);

        primary.kafka().createTopic(topic, NUM_PARTITIONS, 1, topicConfig);
        waitForTopicCreated(backup, backupTopic);

        waitForCondition(() -> {
            String primaryConfig, backupConfig;
            primaryConfig = getTopicConfig(primary.kafka(), topic, "retention.bytes");
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals(primaryConfig, backupConfig,
                    "`retention.bytes` should be the same");
            assertEquals("1000", backupConfig,
                    "`retention.bytes` should be synced with default value!");
            return true;
        }, 30000, "Topic configurations were not synced");

        // delete the previously altered configuration of the source topic
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry("retention.bytes", "1000"), AlterConfigOp.OpType.DELETE));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Collections.singletonMap(configResource, ops);
        primary.kafka().incrementalAlterConfigs(configOps);

        waitForCondition(() -> {
            String backupConfig;
            // the configuration on the target topic should be changed to the target cluster's default
            backupConfig = getTopicConfig(backup.kafka(), backupTopic, "retention.bytes");
            assertEquals("-1", backupConfig,
                    "`retention.bytes` should be synced with target's default value!");
            return true;
        }, 30000, "Topic configurations were not synced");
    }

    @Test
    public void testReplicateFromLatest() throws Exception {
        // populate topic with records that should not be replicated
        String topic = "test-topic-1";
        produceMessages(primaryProducer, topic, NUM_PARTITIONS);

        String sentinelTopic = "test-topic-sentinel";
        primary.kafka().createTopic(sentinelTopic);

        // consume from the ends of topics when no committed offsets are found
        mm2Props.put(PRIMARY_CLUSTER_ALIAS + ".consumer." + AUTO_OFFSET_RESET_CONFIG, "latest");
        // one way replication from primary to backup
        mm2Props.put(BACKUP_CLUSTER_ALIAS + "->" + PRIMARY_CLUSTER_ALIAS + ".enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, PRIMARY_CLUSTER_ALIAS, BACKUP_CLUSTER_ALIAS);

        String backupSentinelTopic = remoteTopicName(sentinelTopic, PRIMARY_CLUSTER_ALIAS);
        waitForTopicCreated(backup, backupSentinelTopic);

        // wait for proof that the task has managed to poll its consumers at least once;
        // this should also mean that it knows the proper end offset of the other test topic,
        // and will consume exactly the expected number of records that we produce after
        // this assertion passes
        // NOTE: this assumes that there is a single MirrorSourceTask instance running;
        // if there are multiple tasks, the logic will need to be updated to ensure that each
        // task has managed to poll its consumer before continuing
        waitForCondition(
                () -> {
                    primary.kafka().produce(sentinelTopic, "sentinel-value");
                    int sentinelValues = backup.kafka().consumeAll(1_000, backupSentinelTopic).count();
                    return sentinelValues > 0;
                },
                RECORD_TRANSFER_DURATION_MS,
                "Records were not produced to sentinel topic in time"
        );

        // produce some more messages to the topic, now that MM2 is running and replication should be taking place
        produceMessages(primaryProducer, topic, NUM_PARTITIONS);

        String backupTopic = remoteTopicName(topic, PRIMARY_CLUSTER_ALIAS);
        // wait for at least the expected number of records to be replicated to the backup cluster
        backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, backupTopic);

        // consume all records from backup cluster
        ConsumerRecords<byte[], byte[]> replicatedRecords = backup.kafka().consumeAll(RECORD_TRANSFER_DURATION_MS, backupTopic);

        // ensure that we only replicated the records produced after startup
        replicatedRecords.partitions().forEach(topicPartition -> {
            int replicatedCount = replicatedRecords.records(topicPartition).size();
            assertEquals(
                    NUM_RECORDS_PER_PARTITION,
                    replicatedCount,
                    "Unexpected number of replicated records for partition " + topicPartition.partition()
            );
        });
    }
}
