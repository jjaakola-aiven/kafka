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
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class TopicPartitionCache {

    private final Map<TopicPartition, Boolean> topicPartitionMapCache = new HashMap<>();
    private final Map<String, Object> configuration;
    private AdminClient adminClient;

    public TopicPartitionCache(final Map<String, Object> configuration) {
        this.configuration = Collections.unmodifiableMap(Objects.requireNonNull(configuration));
    }

    private AdminClient getAdminClient() {
        if (adminClient == null) {
            adminClient = KafkaAdminClient.create(configuration);
        }
        return adminClient;
    }

    public boolean isEmpty(final TopicPartition topicPartition, final Duration operationTimeout) {
        return topicPartitionMapCache.computeIfAbsent(topicPartition, tp -> {
            final ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo;
            try {
                final HashMap<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
                topicPartitionOffsets.put(tp, OffsetSpec.latest());
                listOffsetsResultInfo = getAdminClient().listOffsets(topicPartitionOffsets)
                        .partitionResult(topicPartition)
                        .get(operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            return listOffsetsResultInfo.offset() < 1;
        });
    }
}
