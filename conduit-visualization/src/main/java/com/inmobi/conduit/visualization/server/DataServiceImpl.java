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

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.inmobi.conduit.visualization.client.DataService;

/**
 * The server side implementation of the RPC service.
 */
public class DataServiceImpl extends RemoteServiceServlet
    implements DataService {
  private static final long serialVersionUID = 1L;
  DataServiceManager serviceManager = DataServiceManager.get(true);

  public String getTopologyData(String filterValues) {
    return serviceManager.getTopologyData(filterValues);
  }

  public String getStreamAndClusterList() {
    return serviceManager.getStreamAndClusterList();
  }

  public String getTierLatencyData(String filterValues) {
    return serviceManager.getTierLatencyData(filterValues);
  }

  public String getTimeLineData(String filterValues) {
    return serviceManager.getTimeLineData(filterValues);
  }

}
