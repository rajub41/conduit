package com.inmobi.conduit.visualization.server;

/*
 * #%L
 * Conduit Visualization
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

public class MessageStats {
  private String topic;
  private Long messages;
  private String hostname;

  public MessageStats(String topic, Long messages, String hostname) {
    this.topic = topic;
    this.messages = messages;
    this.hostname = hostname;
  }

  public MessageStats(MessageStats other) {
    this.topic = other.getTopic();
    this.messages = other.getMessages();
    this.hostname = other.getHostname();
  }

  public String getTopic() {
    return topic;
  }

  public Long getMessages() {
    return messages;
  }

  public void setMessages(Long messages) {
    this.messages = messages;
  }

  public String getHostname() {
    return hostname;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MessageStats that = (MessageStats) o;

    if (hostname != null ? !hostname.equals(that.hostname) :
        that.hostname != null) {
      return false;
    }
    if (!topic.equals(that.topic)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = topic.hashCode();
    result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MessageStats{" +
        "topic='" + topic + '\'' +
        ", messages=" + messages +
        ", hostname='" + hostname + '\'' +
        '}';
  }
}
