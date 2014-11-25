package com.inmobi.conduit.audit;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.inmobi.conduit.audit.query.AuditDbQuery;
import com.inmobi.messaging.util.AuditUtil;

/*
 * Post metrics to track.corp.inmobi.com/bender/dash/117
 * 
 * 
 *Inputs: ListOfStreams, ListOfClusters, ListOfUrls(order of url is in the order of clusters),
 * percentileString(default 90 and 95), tier(local and merge)
 * 
 *Steps: 
 * Parse the inputs
 * for each cluster stream url create a object called streamLatency 
 * for each object run two queries.. one for LOCAL tier and another for MERGE tier.
 * 
 * How to post metrics from the query results..
 * Query results returns set of tuples.. here we will be having only one tuple
 * 
 * 
 */
public class StreamLatencyMetrics {

	private List<String> streamList = new ArrayList<String>();
	private List<String> clusterList = new ArrayList<String>();

	private Map<String, String> clusterUrlMap = new HashMap<String, String>();

	public static final String METRIC_DATE_FORMAT = "yyyy-MM-dd-HH";

	private static final Log LOG = LogFactory.getLog(StreamLatencyMetrics.class);

	public static final ThreadLocal<SimpleDateFormat> metric_formatter =
			new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat(METRIC_DATE_FORMAT);
		}
	};

	public static final ThreadLocal<SimpleDateFormat> formatter =
			new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat(AuditUtil.DATE_FORMAT);
		}
	};

	private void evaluateLatencies(String streamStr, String clusterStr, String urlStr,
			String percentileStr, int days, int hours) {
		if (percentileStr == null || percentileStr.isEmpty()) {
			percentileStr = "90,95";
		}
		LOG.info("AAAAAAAAAA percentile str " + percentileStr);

		String [] streamSplits = streamStr.split(",");
		String [] clusterSplits = clusterStr.split(",");
		String [] urlSplits = urlStr.split(",");

		for (int i = 0; i < clusterSplits.length; i++) {	
			clusterUrlMap.put(clusterSplits[i], urlSplits[i]);
		}
		LOG.info("AAAAAA clusterUrl map " + clusterUrlMap);
		for (String stream : streamSplits) {
			streamList.add(stream);
		}
		LOG.info("AAAAAAAAAAAAA streams list " + streamList);
		for (String cluster : clusterSplits) {
			clusterList.add(cluster);
		}
		LOG.info("AAAAAAAAAAAAA clusters list " + clusterList);

		Calendar cal = getTimeToHour(days, hours);
		Date startDate = cal.getTime();

		Date endDate = getTimeToHour(0, 0).getTime();

		String startTimeStr = formatter.get().format(startDate);
		String endTimeStr = formatter.get().format(endDate);
		String metricValueStr = metric_formatter.get().format(startDate);

		/*
		 * rajub@tzns4003:~$ /usr/local/conduit-audit/bin/audit-client audit -group TIER,TOPIC -filter topic=beacon_rr_uj1_cpm_render,TIER=LOCAL -percentile 95 19-11-2014-08:00 19-11-2014-09:00  --conf /usr/local/conduit-audit/prod/conf/
		 * 
		 * Displaying results for AuditStatsQuery [fromTime=19-11 08:00, toTime=19-11 09:00, groupBy=GroupBy[TIER, TOPIC], filter=Filter{TOPIC=[beacon_rr_uj1_cpm_render], TIER=[LOCAL]}, timeZone=null, percentiles=95]
[{"TOPIC":"beacon_rr_uj1_cpm_render","CLUSTER":null,"Received":698899,"HOSTNAME":null,"TIMEINTERVAL":null,"Latencies":{"95.0":3},"TIER":"LOCAL"}]
		 */

		// query the results
		// post the results
		for (String cluster : clusterList) {
			LOG.info("AAAAAAAAAAAAAAA prepare the  query ");
			AuditDbQuery auditQuery = new AuditDbQuery(endTimeStr, startTimeStr,
					"TIER='LOCAL|MERGE', CLUSTER=" + cluster,
					"TIER,TOPIC", "GMT", percentileStr);  // percentileString .. in 95 and 99
			try {
				//	LOG.info("AAAAAAAAAAA execute the query " + auditQuery.toString());
				auditQuery.execute();
				LOG.info("AAAAAAAAAAAAAA displaying the results : ");
				auditQuery.displayResults();
				LOG.info("AAAAAAAAAAAAA post the latency metrics " + metricValueStr);
				postLatencyMetrics(auditQuery, metricValueStr, clusterUrlMap.get(cluster));
			} catch (Exception e) {
				System.out.println("Audit Query execute failed with exception: "
						+ e.getMessage());
				e.printStackTrace();
				return;
			}
		}
	}

	private void postLatencyMetrics(AuditDbQuery auditQuery,
			String startTimeStr, String url) throws JSONException {
		Set<Tuple> tupleSet = auditQuery.getTupleSet();
		Map<Tuple, Map<Float, Integer>> percentileTupleMap = auditQuery.getPercentile();
		JSONObject resultJson = new JSONObject();
		resultJson.put("x", startTimeStr);
		for (Tuple tuple : tupleSet) {
			if (streamList.contains(tuple.getTopic())) {
				Map<Float, Integer> percentileMap = percentileTupleMap.get(tuple);
				Set<Float> percentileStr = auditQuery.getPercentileSet();
				for (Float percentileVal : percentileStr) {
					Integer latencyValue = percentileMap.get(percentileVal);
					// post metrics to the curl url
					String percentile = String.valueOf(percentileVal).substring(0, String.valueOf(percentileVal).indexOf('.'));

					resultJson.put(tuple.getTier() + " " + percentile + " " + tuple.getTopic() , latencyValue);
				}
			}
		}
		try {
			postLatencies(resultJson, url);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void postLatencies(JSONObject resultJson, String url) throws IOException {
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) ((new URL(url).openConnection()));

			//con.setRequestMethod("POST");

			//con.setRequestProperty("Content-Type","application/x-www-form-urlencoded"); 
			//con.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0;Windows98;DigExt)");
			//scon.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
			//con.setRequestProperty("Content-Length", String.valueOf(resultJson.toString().getBytes().length));
			con.setDoOutput(true);
			con.setDoInput(true); 
			con.setUseCaches(false);

			con.connect();
			LOG.info("AAAAAAAAAAA resultJson    : " + resultJson + "    connection " + con);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes("log=" + resultJson.toString());
			wr.flush();
			wr.close();
	 
			
			LOG.info("AAAAAAAAAAAA reading from the url using input stream ");
			BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                    con.getInputStream()));
String decodedString;
while ((decodedString = in.readLine()) != null) {
LOG.info("AAAAAAAAAAAdecoded : " + decodedString);
}
in.close();
	/*		OutputStream os = null;
			OutputStreamWriter osw = null;
			try {
				os = con.getOutputStream();
				osw = new OutputStreamWriter(os, "UTF-8");
				LOG.info("AAAAAAAAAAAA posting this value " + "'" + resultJson.toString() + "'");
				osw.write(resultJson.toString());
			} finally {
				try {
					if (osw != null) {
						LOG.info("AAAAAAAAAAAAA closing oswriter " );
						osw.close();
					}
				} finally {
					if (os != null) {
						LOG.info("AAAAAAAAAAAAA closing os " );
						os.close();
					}
				}
			}*/
		} finally {
			if (con != null) {
				LOG.info("AAAAAAAAAAAAA disconnecting connection to bedner " );
				con.disconnect();
			}
		}
	}

	private Calendar getTimeToHour(int days, int hours) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -days);
		cal.add(Calendar.HOUR_OF_DAY, -hours);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal;
	}

	private static void printUsage() {
		System.out.println("Usage: ");
		System.out.println(
				"[-streams (comma separated stream names)]" + "[-clusters (comma seaprated list) ]"
						+"[-percentile (comma separated)]" + "[-days (relative time from current time (days from now))]"
						+ "[-hours (number of hours beyond from now)]");
	}

	public static void main(String[] args) {
		String streamStr = null;
		String clusterStr = null;
		String percentileStr = null;
		String urlStr = null;
		int relativeTimeInHours = -1;
		int relativeTimeInDays = 0;
		if (args.length < 3) {
			printUsage();
			System.exit(-1);
		}
		for (int i = 0; i < args.length;) {
			LOG.info("AAAAAAAAAA parsing inputs args " + args);
			if (args[i].equalsIgnoreCase("-streams")) {
				streamStr = args[i+1];
				i += 2;
			}
			if (args[i].equalsIgnoreCase("-clusters")) {
				clusterStr = args[i+1];
				i += 2;
			}
			if (args[i].equalsIgnoreCase("-urls")) {
				urlStr = args[i + 1];
				i += 2;
			}
			if (args[i].equalsIgnoreCase("-percentile")) {
				percentileStr = args[i + 1];
				i += 2;
			}
			if (args[i].equalsIgnoreCase("-hours")) {
				relativeTimeInHours = Integer.parseInt(args[i+1]);
				i += 2;
			}
			if (args[i].equalsIgnoreCase("-days")) {
				relativeTimeInDays = Integer.parseInt(args[i+1]);
				i += 2;
			}
		}
		//TODO Validate parameters
		StreamLatencyMetrics latencyMetrics = new StreamLatencyMetrics();
		latencyMetrics.evaluateLatencies(streamStr, clusterStr, urlStr,
				percentileStr, relativeTimeInDays, relativeTimeInHours);
	}
}