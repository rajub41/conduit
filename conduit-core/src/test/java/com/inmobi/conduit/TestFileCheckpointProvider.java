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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestFileCheckpointProvider {

  @Test
  public void test() {
    CheckpointProvider provider = new FSCheckpointProvider("target/");
    Assert.assertNull(provider.read("notpresent"));

    String key = "t1";
    byte[] ck = "test".getBytes();
    provider.checkpoint("t1", ck);
    Assert.assertEquals(provider.read(key), ck);

    byte[] ck1 = "test1".getBytes();
    provider.checkpoint("t1", ck1);
    Assert.assertEquals(provider.read(key), ck1);
    provider.close();
  }

}
