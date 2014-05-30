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

import com.google.gwt.core.client.GWT;
import com.google.gwt.http.client.Request;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

public class DataServiceWrapper {
  private final DataServiceAsync service =
      (DataServiceAsync) GWT.create(DataService.class);
  private final String moduleRelativeURL = GWT.getModuleBaseURL() + "graph";
  private final ServiceDefTarget target = (ServiceDefTarget) service;

  public DataServiceWrapper() {
    target.setServiceEntryPoint(moduleRelativeURL);
  }

  public Request getTopologyData(String clientJson, final AsyncCallback<String>
      callback) {
    Request request = service.getTopologyData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
    return request;
  }

  public Request getTierLatencyData(String clientJson, final AsyncCallback<String>
      callback) {
    Request request = service.getTierLatencyData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
    return request;
  }

  public void getStreamAndClusterList(final AsyncCallback<String> callback) {
    service.getStreamAndClusterList(new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
  }

  public Request getTimeLineData(String clientJson, final AsyncCallback<String>
      callback) {
    Request request = service.getTimeLineData(clientJson, new AsyncCallback<String>() {

      public void onFailure(Throwable caught) {
        callback.onFailure(caught);
      }

      public void onSuccess(String result) {
        callback.onSuccess(result);
      }
    });
    return request;
  }
}
