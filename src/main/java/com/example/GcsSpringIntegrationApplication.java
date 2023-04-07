/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;


import com.google.cloud.spring.storage.integration.outbound.GcsMessageHandler;
import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.messaging.PollableChannel;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.Executors;

import static org.springframework.integration.dsl.IntegrationFlow.from;
import static org.springframework.integration.file.FileReadingMessageSource.WatchEventType.*;

/** Storage Spring Integration sample app. */
@SpringBootApplication
public class GcsSpringIntegrationApplication {

  private static final String NEW_FILE_CHANNEL = "newFileChannel";
  private static final String THROTTLE_CHANNEL = "throttleChannel";
  private static final String UPLOAD_FILE_CHANNEL = "uploadFileChannel";

  @Value("${gcs-local-directory}")
  private String localDirectory;

  @Value("${gcs-write-bucket}")
  private String gcsWriteBucket;

  public static void main(String[] args) {
    SpringApplication.run(GcsSpringIntegrationApplication.class, args);
  }

  @Bean
  public PollableChannel throttleChannel() {
    return MessageChannels.queue(THROTTLE_CHANNEL).get();
  }

  @Bean
  public ExecutorChannel uploadFileChannel() {
    return MessageChannels.executor(UPLOAD_FILE_CHANNEL, Executors.newCachedThreadPool()).get();
  }

  @Bean
  @InboundChannelAdapter(value = NEW_FILE_CHANNEL, poller = @Poller(fixedDelay = "1"))
  public MessageSource<File> fileReadingMessageSource() {
    FileReadingMessageSource source = new FileReadingMessageSource();
    source.setDirectory(new File(localDirectory));
    source.setFilter(new AcceptOnceFileListFilter<>());
    source.setUseWatchService(true);
    source.setWatchEvents(CREATE, MODIFY);
    return source;
  }

  @Bean
  public IntegrationFlow newFileFlow() {
    return from(NEW_FILE_CHANNEL)
            .channel(THROTTLE_CHANNEL)
            .bridge(e -> e.poller(Pollers.fixedDelay(Duration.ofSeconds(1)).maxMessagesPerPoll(10)
                    .taskExecutor(Executors.newCachedThreadPool())))
            .channel(UPLOAD_FILE_CHANNEL)
            .get();
  }

  @Bean
  public IntegrationFlow uploadFileFlow(GcsMessageHandler handler) {
    return from(UPLOAD_FILE_CHANNEL)
            .log()
            .handle(handler)
            .get();
  }

  /**
   * A service activator that connects to a channel with messages containing {@code InputStream}
   * payloads and copies the file data to a remote directory on GCS.
   *
   * @param gcs a storage client to use
   * @return a message handler
   */
  @Bean
  public GcsMessageHandler outboundChannelAdapter(Storage gcs) {
    GcsMessageHandler outboundChannelAdapter = new GcsMessageHandler(new ImprovedGcsSessionFactory(gcs));
    outboundChannelAdapter.setRemoteDirectoryExpression(new ValueExpression<>(this.gcsWriteBucket+"/data-test-new"));
    outboundChannelAdapter.setFileNameGenerator(
            message -> message.getHeaders().get(FileHeaders.FILENAME, String.class));

    return outboundChannelAdapter;
  }

}
