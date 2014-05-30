package com.inmobi.conduit.audit;

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

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class Tuple {
  final private String hostname;
  final private String tier;
  final private String cluster;
  final private Date timestamp;
  final private String topic;
  private Long sent = 0l, received = 0l, lost = 0l;
  private boolean isGroupBySet = false;
  private Map<LatencyColumns, Long> latencyCountMap;
  private GroupBy.Group group;

  public Tuple(String hostname, String tier, String cluster, Date timestamp,
      String topic) {
    this(hostname, tier, cluster, timestamp, topic,
        new HashMap<LatencyColumns, Long>(), 0l);
  }

  public Tuple(String hostname, String tier, String cluster, Date timestamp,
      String topic, Map<LatencyColumns, Long> latencyCountMap, Long sent) {
    this.hostname = hostname;
    this.tier = tier;
    this.topic = topic;
    this.cluster = cluster;
    this.timestamp = timestamp;
    this.latencyCountMap = latencyCountMap;
    this.sent = sent;
    setReceived();
  }
  
  
  public Tuple(String tier, String cluster, Date timestamp, String topic,
      Long sent, Long received) {
    this(null, tier, cluster, timestamp, topic,
        new HashMap<LatencyColumns, Long>(), sent);
    this.received = received;
  }
  

  private void setReceived() {
    received = 0l;
    if (latencyCountMap != null) {
      for (Map.Entry<LatencyColumns, Long> entry : latencyCountMap.entrySet()) {
        received += entry.getValue();
        if (entry.getKey() == LatencyColumns.C600)
          lost = entry.getValue();
      }
    }
  }

  @Override
  public int hashCode() {
    int result = hostname != null ? hostname.hashCode() : 0;
    result = 31 * result + (tier != null ? tier.hashCode() : 0);
    result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
    result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
    result = 31 * result + (topic != null ? topic.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Tuple tuple = (Tuple) o;

    if (cluster != null ? !cluster.equals(tuple.cluster)
        : tuple.cluster != null) {
      return false;
    }
    if (hostname != null ? !hostname.equals(tuple.hostname)
        : tuple.hostname != null) {
      return false;
    }
    if (tier != null ? !tier.equals(tuple.tier) : tuple.tier != null) {
      return false;
    }
    if (timestamp != null ? !timestamp.equals(tuple.timestamp)
        : tuple.timestamp != null) {
      return false;
    }
    if (topic != null ? !topic.equals(tuple.topic) : tuple.topic != null) {
      return false;
    }

    return true;
  }

  public String getTier() {
    return tier;
  }

  public long getSent() {
    return sent;
  }

  public void setSent(long sent) {
    this.sent = sent;
  }

  public long getReceived() {
    return received;
  }

  public String getTopic() {
    return topic;
  }

  public String getCluster() {
    return cluster;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public String getHostname() {
    return hostname;
  }

  public Map<LatencyColumns, Long> getLatencyCountMap() {
    if (latencyCountMap == null)
      return null;
    return Collections.unmodifiableMap(latencyCountMap);
  }

  public void setLatencyCountMap(Map<LatencyColumns, Long> latencyCountMap) {
    this.latencyCountMap = latencyCountMap;
    setReceived();
  }

  public Map<Column, String> getTupleKey() {
    Map<Column, String> values = new HashMap<Column, String>();
    values.put(Column.HOSTNAME, hostname);
    values.put(Column.TIER, tier);
    values.put(Column.TOPIC, topic);
    values.put(Column.CLUSTER, cluster);
    if (timestamp != null) {
      values.put(Column.TIMEINTERVAL, String.valueOf(timestamp.getTime()));
    } else {
      values.put(Column.TIMEINTERVAL, null);
    }
    return values;
  }

  public void setGroupBy(GroupBy groupBy) {
    Map<Column, String> values = getTupleKey();
    this.group = groupBy.getGroup(values);
    isGroupBySet = true;
  }

  public boolean isGroupBySet() {
    return isGroupBySet;
  }

  public GroupBy.Group getGroup() {
    return group;
  }

  public Long getLostCount() {
    return lost;
  }

  @Override
  public String toString() {
    return "Tuple{" + "tier='" + tier + '\'' + ", hostname='" + hostname + '\''
        + ", latencyCountMap=" + latencyCountMap + ", received=" + received
        + ", sent=" + sent + ", topic='" + topic + '\'' + ", timestamp="
        + timestamp + ", cluster='" + cluster + '\'' + '}';
  }
}
