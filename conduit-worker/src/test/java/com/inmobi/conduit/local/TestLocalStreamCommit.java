package com.inmobi.conduit.local;

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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.inmobi.conduit.ConduitConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.FSCheckpointProvider;

public class TestLocalStreamCommit {

  static Path rootDir = new Path("/tmp/test-conduit/conduit/");

  static FileSystem localFs;
  private Set<String> streamsToProcess = new HashSet<String>();
   
  private void createData(Cluster cluster) throws IOException {
    Path tmpPath = new Path(cluster.getTmpPath(),
        LocalStreamService.class.getName());
    Path tmpJobOutputPath = new Path(tmpPath, "jobOut");
    Path path1 = new Path(tmpJobOutputPath, "stream1");
    Path path2 = new Path(tmpJobOutputPath, "stream2");
    streamsToProcess.add("stream1");
    streamsToProcess.add("stream2");
    localFs.mkdirs(path1);
    localFs.mkdirs(path2);
    localFs.create(new Path(path1, "file1"));
    localFs.create(new Path(path2, "file2"));
  }

  @BeforeTest
  public void setUP() throws Exception {
    localFs = FileSystem.getLocal(new Configuration());
    //clean up the test data if any thing is left in the previous runs
    cleanup();
  }

  @AfterTest
  public void cleanup() throws Exception {
    localFs.delete(rootDir, true);

  }

  @Test
  public void testPrepareForCommit() throws Exception {
    ConduitConfigParser parser = new ConduitConfigParser(
        "src/test/resources/test-merge-mirror-conduit1.xml");

    Cluster cluster1 = parser.getConfig().getClusters().get("testcluster1");
    LocalStreamService service = new LocalStreamService(parser.getConfig(),
        cluster1, null, new FSCheckpointProvider(
            "/tmp/test-conduit/conduit/checkpoint"), streamsToProcess);
    createData(cluster1);
    service.prepareForCommit(System.currentTimeMillis());
    Path tmpPath = new Path(cluster1.getTmpPath(),
        LocalStreamService.class.getName());
    Path tmpConsumerPath = new Path(tmpPath, "testcluster2");
    FileStatus[] status = null;
    try {
      status = localFs.listStatus(tmpConsumerPath);
    } catch (FileNotFoundException e) {
      status = new FileStatus[0];
    }
    for (FileStatus tmpStatus : status) {
      // opening the consumer file written for testcluster2
      // it should not have any entry for stream 1 as testcluster2 is primary
      // destination only for stream2
      FSDataInputStream inStream = localFs.open(tmpStatus.getPath());
      String line;
      while ((line = inStream.readLine()) != null) {
        assert (!line.contains("stream1"));
      }

    }
  }
}
