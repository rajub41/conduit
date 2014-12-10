package com.inmobi.conduit.audit;

public class StreamLatency {
	private String stream;
	private int cluster;
	private String url;
	private String percentileStr;

	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public int getCluster() {
		return cluster;
	}

	public void setCluster(int cluster) {
		this.cluster = cluster;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public StreamLatency(String stream, String clutser, String url, String percentile) {
		this.stream = stream;
		this.cluster = cluster;
		this.url = url;
		this.percentileStr = percentile;
	}
  }
