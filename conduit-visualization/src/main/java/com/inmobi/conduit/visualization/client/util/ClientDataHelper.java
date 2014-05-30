package com.inmobi.conduit.visualization.client.util;

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

import com.google.protobuf.gwt.client.ClientJsonStreamFactory;
import com.inmobi.conduit.visualization.client.ClientConstants;
import com.inmobi.conduit.visualization.shared.RequestResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientDataHelper {
  private static ClientDataHelper dataHelperInstance;

  public static ClientDataHelper getInstance() {
    if (dataHelperInstance == null) {
      dataHelperInstance = new ClientDataHelper();
    }
    return dataHelperInstance;
  }

  public String setGraphDataRequest(String startTime, String endTime,
                                    String stream, String colo) {
    RequestResponse.Request request;
    request = RequestResponse.Request.newBuilder().setGraphDataRequest(
        RequestResponse.GraphDataRequest.newBuilder().setStartTime(startTime)
            .setEndTime(endTime).setStream(stream).setColo(colo).build())
        .build();
    return ClientJsonStreamFactory.getInstance().serializeMessage(request);
  }

  public String getJsonFromTopologyDataResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getTopologyDataResponse().getJsonString();
  }

  public List<String> getStreamsListFromLoadMainPanelResponse(
      String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getLoadMainPanelResponse().getStreamList();
  }

  public List<String> getClusterListFromLoadMainPanelResponse(
      String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getLoadMainPanelResponse().getClusterList();
  }

  public Map<String, String> getClientConfigLoadMainPanelResponse(String
                                                                 serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Map<String, String> configMap = new HashMap<String, String>();
    configMap.put(ClientConstants.PUBLISHER, response.getLoadMainPanelResponse()
        .getClientConfig().getPublisherSla());
    configMap.put(ClientConstants.AGENT, response.getLoadMainPanelResponse()
        .getClientConfig().getAgentSla());
    configMap.put(ClientConstants.VIP, response.getLoadMainPanelResponse()
        .getClientConfig().getVipSla());
    configMap.put(ClientConstants.COLLECTOR,
        response.getLoadMainPanelResponse().getClientConfig().getCollectorSla());
    configMap.put(ClientConstants.HDFS, response.getLoadMainPanelResponse()
        .getClientConfig().getHdfsSla());
    configMap.put(ClientConstants.LOCAL, response.getLoadMainPanelResponse()
        .getClientConfig().getLocalSla());
    configMap.put(ClientConstants.MERGE, response.getLoadMainPanelResponse()
        .getClientConfig().getMergeSla());
    configMap.put(ClientConstants.MIRROR, response.getLoadMainPanelResponse()
        .getClientConfig().getMirrorSla());
    configMap.put(ClientConstants.PERCENTILE_FOR_SLA, response.getLoadMainPanelResponse()
        .getClientConfig().getPercentileForSla());
    configMap.put(ClientConstants.PERCENTAGE_FOR_LOSS, response.getLoadMainPanelResponse()
        .getClientConfig().getPercentageForLoss());
    configMap.put(ClientConstants.PERCENTAGE_FOR_WARN, response.getLoadMainPanelResponse()
        .getClientConfig().getPercentageForWarn());
    configMap.put(ClientConstants.MAX_START_TIME, response.getLoadMainPanelResponse()
        .getClientConfig().getMaxStartTime());
    configMap.put(ClientConstants.MAX_TIME_INT_IN_HRS, response.getLoadMainPanelResponse()
        .getClientConfig().getMaxTimeRangeInt());
    configMap.put(ClientConstants.LOSS_WARN_THRESHOLD_DIFF, response.getLoadMainPanelResponse()
        .getClientConfig().getWarnLossThresholdDiff());
    configMap.put(ClientConstants.ROLLEDUP_TILL_DAYS, response.getLoadMainPanelResponse()
        .getClientConfig().getRolleduptilldays());
    return configMap;
  }

  public Map<String, Integer> getTierLatencyObjListFromResponse(
      String serverJson) {
    Map<String, Integer> tierLatencyMap = new HashMap<String, Integer>();
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (RequestResponse.TierLatencyObj tierLatencyObj : response
        .getTierLatencyResponse().getTierLatencyObjListList()) {
       tierLatencyMap.put(tierLatencyObj.getTier().toLowerCase(),
           tierLatencyObj.getLatency());
    }
    return tierLatencyMap;
  }

  public String getTimeLineJSONFromResponse(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getTimeLineGraphResponse().getJsonString();
  }

  public int getTimeBucketForTrend(String serverJson) {
    RequestResponse.Response response = null;
    try {
      response = RequestResponse.Response.newBuilder().readFrom(
          ClientJsonStreamFactory.getInstance()
              .createNewStreamFromJson(serverJson)).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response.getTimeLineGraphResponse().getTimebucket();
  }
}
