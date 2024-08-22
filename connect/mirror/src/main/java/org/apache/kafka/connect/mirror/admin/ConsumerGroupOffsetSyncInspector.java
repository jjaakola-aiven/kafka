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

package org.apache.kafka.connect.mirror.admin;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.mirror.MirrorCheckpointConfig;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupOffsetsComparer;
import org.apache.kafka.connect.mirror.admin.offsetinspector.ConsumerGroupsStateCollector;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class ConsumerGroupOffsetSyncInspector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupOffsetSyncInspector.class);
    private static final String CSV_ROW_FORMAT = "%s,%s,%s,%s,%s,%s,%s,%s";
    private static final String CSV_HEADER_FORMAT = String.format(CSV_ROW_FORMAT, "CLUSTER PAIR", "GROUP", "TOPIC", "PARTITION",
            "SOURCE OFFSET", "TARGET OFFSET", "IS OK", "MESSAGE");

    public static void main(final String[] args) throws IOException {
        final ArgumentParser parser = ArgumentParsers.newArgumentParser("mirror-maker-consumer-group-offset-sync-inspector");
        parser.description("MirrorMaker 2.0 consumer group offset sync inspector");
        parser.addArgument("--mm2-config").type(Arguments.fileType().verifyCanRead())
                .metavar("mm2.properties").required(true)
                .help("MM2 configuration file.");

        parser.addArgument("--output-path").type(Arguments.fileType().verifyCanCreate())
                .required(false)
                .help("The result CSV file output path. If not given the result is printed to console.");

        final Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(-1);
            return;
        }

        final File mm2ConfigFile = ns.get("mm2_config");
        final Properties mm2Properties = Utils.loadProps(mm2ConfigFile.getPath());
        final File outputFile = ns.get("output_path");
        new ConsumerGroupOffsetSyncInspector().run(Utils.propsToStringMap(mm2Properties), outputFile);
    }

    public void run(final Map<String, String> mm2ConfigProps, final File outputFile) throws IOException {
        final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults = inspect(mm2ConfigProps);
        LOGGER.info("Writing result CSV to {}", outputFile != null ? outputFile.getPath() : "STDOUT");
        if (outputFile == null) {
            writeToOutputStream(System.out, clusterResults);
        } else {
            try (PrintStream out = new PrintStream(Files.newOutputStream(outputFile.toPath()),
                    false, StandardCharsets.UTF_8.name())) {
                writeToOutputStream(out, clusterResults);
            }
        }
        LOGGER.info("Done.");
    }

    public Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> inspect(final Map<String, String> mm2ConfigProps) {
        final MirrorMakerConfig mm2Config = new MirrorMakerConfig(mm2ConfigProps);
        final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults = new HashMap<>();

        mm2Config.clusterPairs().forEach(sourceAndTarget -> {
            if (!mm2Config.clusterPairEnabled(sourceAndTarget)) {
                // If not enabled, do not inspect.
                return;
            }
            final MirrorCheckpointConfig mirrorCheckpointConfig = new MirrorCheckpointConfig(
                    mm2Config.connectorBaseConfig(sourceAndTarget, MirrorCheckpointConnector.class)
            );
            if (!mirrorCheckpointConfig.getBoolean("enabled") ||
                    mirrorCheckpointConfig.emitCheckpointsInterval().isNegative()) {
                // If checkpoint connector is not enabled or emit of checkpoints is disabled (negative duration),
                // do not inspect.
                return;
            }

            final Map<String, Map<TopicPartition, OffsetAndMetadata>> sourceConsumerOffsets;
            final Map<String, Map<TopicPartition, OffsetAndMetadata>> targetConsumerOffsets;

            final String sourceClusterAlias = sourceAndTarget.source();
            final String targetClusterAlias = sourceAndTarget.target();

            final MirrorClientConfig sourceMirrorClientConfig = mm2Config.clientConfig(sourceClusterAlias);
            final MirrorClientConfig targetMirrorClientConfig = mm2Config.clientConfig(targetClusterAlias);

            try (ConsumerGroupsStateCollector collector = ConsumerGroupsStateCollector.builder()
                    .withOperationTimeout(Duration.ofMinutes(1))
                    .withAdminClientConfiguration(sourceMirrorClientConfig.adminConfig())
                    .withMirrorCheckpointConfig(mirrorCheckpointConfig)
                    .build()) {
                sourceConsumerOffsets = collector.collectConsumerGroupsState();
            }

            try (ConsumerGroupsStateCollector collector = ConsumerGroupsStateCollector.builder()
                    .withOperationTimeout(Duration.ofMinutes(1))
                    .withAdminClientConfiguration(targetMirrorClientConfig.adminConfig())
                    .withMirrorCheckpointConfig(mirrorCheckpointConfig)
                    .build()) {
                targetConsumerOffsets = collector.collectConsumerGroupsState();
            }

            final ConsumerGroupOffsetsComparer comparer = ConsumerGroupOffsetsComparer.builder()
                    .withOperationTimeout(Duration.ofMinutes(1))
                    .withSourceConfiguration(sourceMirrorClientConfig.adminConfig())
                    .withSourceConsumerOffsets(sourceConsumerOffsets)
                    .withTargetConsumerOffsets(targetConsumerOffsets)
                    .build();
            final ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult result = comparer.compare();
            clusterResults.put(sourceAndTarget, result);
        });
        return clusterResults;
    }

    private void writeToOutputStream(final PrintStream out,
                                     final Map<SourceAndTarget, ConsumerGroupOffsetsComparer.ConsumerGroupsCompareResult> clusterResults) {
        try {
            out.write(CSV_HEADER_FORMAT.getBytes(StandardCharsets.UTF_8));
            out.write("\n".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        clusterResults.forEach((sourceAndTarget, result) -> result.getConsumerGroupsCompareResult().forEach(groupResult -> {
            try {
                final String sourceOffset = groupResult.getSourceOffset() == null
                        ? "-"
                        : groupResult.getSourceOffset().toString();
                final String targetOffset = groupResult.getTargetOffset() == null
                        ? "-"
                        : groupResult.getTargetOffset().toString();
                out.write(
                        String.format(CSV_ROW_FORMAT, sourceAndTarget.toString(),
                                groupResult.getGroupId(), groupResult.getTopicPartition().topic(),
                                groupResult.getTopicPartition().partition(), sourceOffset, targetOffset,
                                groupResult.isOk(), groupResult.getMessage()).getBytes(StandardCharsets.UTF_8));
                out.write("\n".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        out.flush();
    }
}
