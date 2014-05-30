package com.inmobi.conduit.utils;

/*
 * #%L
 * Conduit Worker
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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.inmobi.conduit.ConduitConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.conduit.AbstractService;
import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;

public class CheckPointCreator {

  private final ConduitConfig config;
  private final String srcCluster;
  private final String destnCluster;
  private final String stream;
  private static final Log LOG = LogFactory.getLog(CheckPointCreator.class);
  private final Set<String> sourceClusters = new HashSet<String>();
  private Date date;

  public CheckPointCreator(ConduitConfig config, String sourceCluster,
      String destinationCluster, String stream, Date date) {
    this.config = config;
    srcCluster = sourceCluster;
    destnCluster = destinationCluster;
    this.stream = stream;
    this.date = date;

  }

  String getCheckPointKey(String stream, String srcCluster, boolean isMerge) {
    if (isMerge)
      return AbstractService.getCheckPointKey("MergedStreamService", stream,
          srcCluster);
    else
      return AbstractService.getCheckPointKey("MirrorStreamService", stream,
          srcCluster);
  }

  public void createCheckPoint() throws Exception {
    Cluster destinationCluster = config.getClusters().get(destnCluster);
    CheckpointProvider provider = new FSCheckpointProvider(
        destinationCluster.getCheckpointDir());
    boolean isMerge = false;
    Set<String> mergingStream = destinationCluster
        .getPrimaryDestinationStreams();
    if (mergingStream.contains(stream)) {
      // stream is getting merged here
      if (srcCluster == null) {// no src clusters provided;create checkpoint for
        // all src clusters
        SourceStream srcStream = config.getSourceStreams().get(stream);
        sourceClusters.addAll(srcStream.getSourceClusters());
      }
      isMerge = true;
    } else if (destinationCluster.getDestinationStreams().containsKey(stream)) {
      // stream is getting mirrored since its a destination stream and not
      // primary destination
      if (srcCluster == null) {// no src clusters provided;create checkpoint for
        // all src clusters
        sourceClusters.add(config.getPrimaryClusterForDestinationStream(stream)
            .getName());
      }
      isMerge = false;
    } else {
      LOG.error("Stream " + stream + " is not destination stream of cluster "
          + destnCluster);
    }
    if (srcCluster != null) {
      sourceClusters.add(srcCluster);
    }
    for (String source : sourceClusters) {
      Cluster srcCluster = config.getClusters().get(source);
      FileSystem srcFS = FileSystem.get(srcCluster.getHadoopConf());
      String checkPointValue;
      if (isMerge) {
        checkPointValue = srcCluster.getLocalDestDir(stream, date);
      } else {
        checkPointValue = srcCluster.getFinalDestDir(stream, date.getTime());
      }
      Path checkPoinPath = new Path(checkPointValue);
      if (!srcFS.exists(checkPoinPath))
        throw new Exception("Path " + checkPointValue
            + " doesn't exist,hence checkpoint can't be created for source "
            + source);
      provider.checkpoint(getCheckPointKey(stream, source, isMerge),
          checkPointValue.getBytes());
    }
  }
}
