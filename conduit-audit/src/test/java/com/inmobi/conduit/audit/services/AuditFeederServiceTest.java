package com.inmobi.conduit.audit.services;

/*
 * #%L
 * Conduit Audit
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.MessageConsumer;
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.MockConsumerWithReset;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.MessagingConsumerConfig;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MockInMemoryPublisher;
import com.inmobi.messaging.util.AuditUtil;

import java.io.IOException;

public class AuditFeederServiceTest extends AuditFeederService {

  private static final String CONSUMER_CLASS_NAME_KEY = "consumer.classname";
  private MessagePublisher publisher;
  private  boolean isSetSource = false;

  public AuditFeederServiceTest(String clusterName, String rootDir,
                                ClientConfig config,
                                MessagePublisher publisher) throws IOException {
    super(clusterName, rootDir, config);
    this.publisher = publisher;
    if(!isSetSource && this.publisher!= null) {
      ((MockConsumerWithReset) consumer)
          .setSource(((MockInMemoryPublisher) (publisher)).source);
      isSetSource = true;
    }
    stopIfMsgNull = true;
  }

  @Override
  MessageConsumer getConsumer(ClientConfig config) throws IOException {
    isSetSource = false;
    config.set(DatabusConsumerConfig.databusRootDirsConfig, getRootDir());
    String consumerName = getClusterName() + "_consumer";
    if (config.getString(MessageConsumerFactory.ABSOLUTE_START_TIME) == null)
      config.set(MessagingConsumerConfig.startOfStreamConfig, "true");
    MessageConsumer consumer = MessageConsumerFactory.create(config,
        config.getString(CONSUMER_CLASS_NAME_KEY),
        AuditUtil.AUDIT_STREAM_TOPIC_NAME, consumerName);
    if(publisher != null) {
      ((MockConsumerWithReset) consumer)
          .setSource(((MockInMemoryPublisher) (publisher)).source);
      isSetSource = true;
    }
    return consumer;
  }

  @Override
  public void execute() {
    isStop = false;
    super.execute();
  }
}
