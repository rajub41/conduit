package com.inmobi.conduit;

/*
 * #%L
 * Conduit Core
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

public interface ConduitConfigParserTags {

  public static final String DEFAULTS = "defaults";
  public static final String ROOTDIR = "rootdir";
  public static final String RETENTION_IN_HOURS = "retentioninhours";
  public static final String TRASH_RETENTION_IN_HOURS = "trashretentioninhours";

  public static final String NAME = "name";
  public static final String STREAM = "stream";
  public static final String SOURCE = "source";
  public static final String DESTINATION = "destination";
  public static final String PRIMARY = "primary";

  public static final String CLUSTER = "cluster";
  public static final String JOB_QUEUE_NAME = "jobqueuename";
  public static final String HDFS_URL = "hdfsurl";
  public static final String JT_URL = "jturl";
  public static final String COPYMAPPER_IMPL="copyMapperClass";
  String CLUSTER_READ_URL="readUrl";
}
