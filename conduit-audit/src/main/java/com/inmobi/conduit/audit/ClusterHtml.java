package com.inmobi.conduit.audit;

public class ClusterHtml {

  private String cluster;
  private StringBuilder localHtmlBody;
  private StringBuilder mergeHtmlBody;
  protected static boolean isStartedProcessing = false;

  public ClusterHtml(String cluster) {
    this.cluster = cluster;
    localHtmlBody = new StringBuilder();
    mergeHtmlBody = new StringBuilder();
  }
  public boolean isProcessing() {
    return isStartedProcessing;
  }
  public void setProcessing(boolean isProcessing) {
    this.isStartedProcessing = isProcessing;
  }
  public String getCluster() {
    return cluster;
  }
  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public StringBuilder getLocalHtmlBody() {
    return localHtmlBody;
  }
  public void setLocalHtmlBody(StringBuilder localHtmlBody) {
    this.localHtmlBody = localHtmlBody;
  }

  public StringBuilder getMergeHtmlBody() {
    return mergeHtmlBody;
  }
  public void setMergeHtmlBody(StringBuilder mergeHtmlBody) {
    this.mergeHtmlBody = mergeHtmlBody;
  }

  void updateLocalMergeHtmlWithRowSpan(String cluster,
      int localRowCount, int mergeRowCount) {
    int total = localRowCount + mergeRowCount;
    localHtmlBody.insert(0," <th rowspan=" + localRowCount + ">" + "LOCAL </th>");
    localHtmlBody.insert(0,"<tr> <th rowspan=" + total + ">" +  cluster +" </th>");
    localHtmlBody.append("</tr>").append("\n");
    mergeHtmlBody.insert(0,"<tr> <th rowspan=" + mergeRowCount + ">" + "MERGE </th>");
    mergeHtmlBody.append("</tr>").append("\n");
  }

  void updateMergeHtmlBodyWithRows(String value) {
    mergeHtmlBody.append("</tr> \n <tr>").append("\n");
    prepareMergeTableRowData(value);
  }

  void updateLocalHtmlBodyWithRows(String value) {
    localHtmlBody.append("</tr> \n <tr>").append("\n");
    prepareLocalTableRowData(value);
  }

  void prepareMergeTableRowData(String value) {
    mergeHtmlBody.append("<td>" + value + "</td>").append("\n");
  }

  void prepareLocalTableRowData(String value) {
    localHtmlBody.append("<td>" + value + "</td>").append("\n");
  }
}
