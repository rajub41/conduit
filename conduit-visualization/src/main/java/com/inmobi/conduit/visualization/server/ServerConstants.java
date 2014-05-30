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

public class ServerConstants {
  public static final String FEEDER_PROPERTIES_DEFAULT_PATH =
      "/usr/local/conduit-visualization/conf/audit-feeder.properties";
  public static final String LOG4J_PROPERTIES_DEFAULT_PATH =
      "/usr/local/conduit-visualization/conf/log4j.properties";

  public static final String GROUPBY_STRING = "CLUSTER,TIER,HOSTNAME,TOPIC";
  public static final String GROUPBY_TIMELINE_STRING ="TIER,TOPIC,CLUSTER,TIMEINTERVAL";
  public static final String GROUPBY_CLUSTER_AGG_TIMELINE_STR ="TIER,CLUSTER,TIMEINTERVAL";
  public static final String GROUPBY_ALL_AGG_TIMELINE_STR ="TIER,TIMEINTERVAL";

  public static final String TIMEZONE = "GMT";
  public static final String AUDIT_STREAM = "_audit";
  public static final String STREAM_FILTER = "stream";
  public static final String CLUSTER_FILTER = "cluster";
  public static final String START_TIME_FILTER = "startTime";
  public static final String END_TIME_FILTER = "endTime";
  public static final String VISUALIZATION_PROPERTIES_DEFAULT_PATH =
      "/usr/local/conduit-visualization/conf/visualization.properties";

  public static final String CONDUIT_XML_PATH = "xmlPath";
  public static final String LOG4J_PROPERTIES_PATH = "log4jPath";
  public static final String PERCENTILE_STRING = "percentileString";
  public static final String PUBLISHER_SLA = "publisherSla";
  public static final String AGENT_SLA = "agentSla";
  public static final String VIP_SLA = "vipSla";
  public static final String COLLECTOR_SLA = "collectorSla";
  public static final String HDFS_SLA = "hdfsSla";
  public static final String LOCAL_SLA  = "localSla";
  public static final String MERGE_SLA = "mergeSla";
  public static final String MIRROR_SLA = "mirrorSla";
  public static final String PERCENTILE_FOR_SLA = "slaPercentile";
  public static final String PERCENTAGE_FOR_LOSS = "lossPercentage";
  public static final String PERCENTAGE_FOR_WARN = "warnPercentage";
  public static final String MAX_START_TIME = "maxStartTime";
  public static final String MAX_TIME_RANGE_INTERVAL_IN_HOURS = "timeRange";
  public static final String LOSS_WARN_THRESHOLD_DIFF_IN_MINS = "threshold";
  public static final String ROLLEDUP_TILL_DAYS = "rollup.tilldays";
}
