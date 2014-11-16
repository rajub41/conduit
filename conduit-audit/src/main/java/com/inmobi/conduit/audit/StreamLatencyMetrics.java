package com.inmobi.conduit.audit;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.json.JSONException;

import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.messaging.util.AuditUtil;

public class StreamLatencyMetrics {
/*
  private List<String> streamList = new ArrayList<String>();
  private List<String> clusterList = new ArrayList<String>();
  
  private void evaluateLatencies(String streams, float percentile, int startTime,
      int endTime, String clusters) {
    SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil.DATE_FORMAT);
    String [] streamSplits = streams.split(",");
    for (String stream : streamSplits) {
      streamList.add(stream);
    }
    String [] clusterSplits = clusters.split(",");
    for (String cluster : clusterSplits) {
      clusterList.add(cluster);
    }
    //TODO run audit query for each stream
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -startTime);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    Date startDate = cal.getTime();
    Calendar cal1 = Calendar.getInstance();
    cal1.add(Calendar.DAY_OF_MONTH, -endTime);
    cal1.set(Calendar.HOUR_OF_DAY, 0);
    cal1.set(Calendar.MINUTE, 0);
    cal1.set(Calendar.SECOND, 0);
    cal1.set(Calendar.MILLISECOND, 0);
    Date endDate = cal1.getTime();
    String startTimeStr = formatter.format(startDate);
    String endTimeStr = formatter.format(endDate);
    for (String topic : streamList) {
      AuditDbQuery auditQuery = new AuditDbQuery(endTimeStr, startTimeStr, "TOPIC="
          + topic, "TIER", null);
      try {
        auditQuery.execute();
      } catch (Exception e) {
        System.out.println("Audit Query execute failed with exception: "
            + e.getMessage());
        e.printStackTrace();
        return;
      }

      System.out.println("Displaying results for Audit Query: " + auditQuery);
      // display audit query results
      try {
        auditQuery.displayResults();
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  private static void printUsage() {
    System.out.println("Usage: ");
    System.out.println(
        "[-streams (comma separated stream names)]" +
        "[-percentile ({90, 95,99.99})]" +
        "[-time (relative time (hours from now))]");
  }

  public static void main(String[] args) {
    
    String streams = null;
    String clusters = null;
    float percentile = -1;
    int time = -1;
    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }
    for (int i = 1; i < args.length - 1;) {
      if (args[i].equalsIgnoreCase("-streams")) {
         streams = args[i+1];
         i += 2;
      }
      if (args[i].equalsIgnoreCase("-percentile")) {
        percentile = Float.parseFloat(args[i+1]);
        i += 2;
      }
      if (args[i].equalsIgnoreCase("-time")) {
        time = Integer.parseInt(args[i+1]);
        i += 2;
      }
      if (args[i].equalsIgnoreCase("-clusters")) {
        clusters = args[i+1];
        i += 2;
     }
    }
    //TODO Validate parameters


    StreamLatencyMetrics latencyMetrics = new StreamLatencyMetrics();
    latencyMetrics.evaluateLatencies(streams, percentile, time, clusters);
  }*/
}
