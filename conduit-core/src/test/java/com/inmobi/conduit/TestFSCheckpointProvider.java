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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class TestFSCheckpointProvider extends TestMiniClusterUtil {
  private static final Path testDir = new Path("/tmp/fscheckpoint");
  private String cpKey = "mykey";
  private String checkpoint = "my Checkpoint";
  FileSystem localFs;

  @BeforeSuite
  public void setup() throws Exception {
    super.setup(-1, 0, 0);
    localFs = FileSystem.getLocal(new Configuration());
  }

  @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  @Test
  public void testWithLocal() throws IOException {
    testWithFS(localFs);
    localFs.delete(testDir, true);
  }

  @Test
  public void testWithHDFS() throws IOException {
    FileSystem dfs = GetFileSystem();
    testWithFS(dfs);
    dfs.delete(testDir, true);
  }

  public void testWithFS(FileSystem fs) throws IOException {
    FSCheckpointProvider cpProvider = new FSCheckpointProvider(testDir
        .makeQualified(fs).toString());
    Assert.assertTrue(fs.exists(testDir));
    Assert.assertNull(cpProvider.read(cpKey));
    cpProvider.checkpoint(cpKey, checkpoint.getBytes());
    Assert.assertEquals(new String(cpProvider.read(cpKey)), checkpoint);
    cpProvider.close();
  }

}
