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

public class DataServiceManagerTest extends DataServiceManager {
  private static DataServiceManagerTest instance = null;

  private DataServiceManagerTest(String xmlPath,
                                 String visualizationPropPath,
                                 String feederPropPath) {
    super(false, visualizationPropPath, feederPropPath);
    initConfig(xmlPath);
  }

  public static DataServiceManagerTest get(String xmlpath,
                                           String visualizationPropPath,
                                           String feederPropPath) {
    instance = new DataServiceManagerTest(xmlpath, visualizationPropPath,
        feederPropPath);
    return instance;
  }
}
