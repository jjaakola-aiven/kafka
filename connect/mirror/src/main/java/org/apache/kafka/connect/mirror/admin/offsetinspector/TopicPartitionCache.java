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
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class TopicPartitionCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionCache.class);

    private final Map<TopicPartition, Boolean> topicPartitionMapCache = new HashMap<>();
    private final AdminClient adminClient;

    public TopicPartitionCache(final AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public boolean isEmpty(final TopicPartition topicPartition, final Duration operationTimeout) {
        return topicPartitionMapCache.computeIfAbsent(topicPartition, tp -> {
            final ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfoLatest;
            final ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfoEarliest;
            try {
                final HashMap<TopicPartition, OffsetSpec> earliestPartitionOffsets = new HashMap<>();
                earliestPartitionOffsets.put(tp, OffsetSpec.earliest());
                final HashMap<TopicPartition, OffsetSpec> latestPartitionOffsets = new HashMap<>();
                latestPartitionOffsets.put(tp, OffsetSpec.latest());
                listOffsetsResultInfoEarliest = adminClient.listOffsets(earliestPartitionOffsets)
                        .partitionResult(topicPartition)
                        .get(operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
                listOffsetsResultInfoLatest = adminClient.listOffsets(latestPartitionOffsets)
                        .partitionResult(topicPartition)
                        .get(operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Earliest offset {} / {} latest offsets", listOffsetsResultInfoEarliest.offset(), listOffsetsResultInfoLatest.offset());
            return listOffsetsResultInfoEarliest.offset() == listOffsetsResultInfoLatest.offset();
        });
    }
}
