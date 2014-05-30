package com.inmobi.conduit.visualization.client;

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

import com.google.gwt.http.client.Request;
import com.google.gwt.user.client.rpc.AsyncCallback;

public interface DataServiceAsync {
  public void getStreamAndClusterList(AsyncCallback<String> callback);

  public Request getTopologyData(String filterValues, AsyncCallback<String>
      callback);

  public Request getTierLatencyData(String filterValues, AsyncCallback<String>
      callback);

  public Request getTimeLineData(String filterValues, AsyncCallback<String>
      callback);
}
