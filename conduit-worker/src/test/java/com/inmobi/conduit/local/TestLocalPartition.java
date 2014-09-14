package com.inmobi.conduit.local;

import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import com.inmobi.conduit.CheckpointProvider;
import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.HCatClientUtil;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.distcp.MergeMirrorStreamPartitionsTest;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.HCatPartitionComparator;

public class TestLocalPartition extends TestLocalStreamService {

  private static final Log LOG = LogFactory.getLog(TestLocalPartition.class);
   HCatClient hcatClient = null;
  static String dbName;
  Cluster srcCluster;
  //static String tableName;
  private Set<String> streamsToProcess = new HashSet<String>();
  public TestLocalPartition(ConduitConfig config, Cluster srcCluster,
      Cluster currentCluster, CheckpointProvider provider,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil) throws IOException {
    super(config, srcCluster, currentCluster, provider, streamsToProcess, hcatUtil);
    this.streamsToProcess = streamsToProcess;
    this.srcCluster = srcCluster;
  }
  /*
  @BeforeTest
  public void setup() throws Exception {
    //Conduit.setHCatEnabled(true);
   // Conduit.setHcatDBName("test_conduit");
  }
  
  @AfterTest
  public void cleanup() throws Exception  {
    
  }
  */
  @Override
  protected void postExecute() throws InterruptedException {
    
    LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA post execute in TestLocalStreamPartition");
    try {
      hcatClient = getHCatClient();
      for (String stream : streamsToProcess) {
        String tableName = "conduit_local_" + stream;
        List<HCatPartition> list = hcatClient.getPartitions(dbName, tableName);
        Collections.sort(list, new HCatPartitionComparator());
        Date lastAddedTime = MergeMirrorStreamPartitionsTest.getLastAddedPartTime();
        Calendar cal = Calendar.getInstance();
        /*cal.setTime(lastAddedTime);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.add(Calendar.MINUTE, 35);
      */  Date endTime = cal.getTime();
       Path localStreamPath = new Path(srcCluster.getLocalFinalDestDirRoot(), stream);
       Path startPath = CalendarHelper.getPathFromDate(lastAddedTime, localStreamPath);
       Path endPath = CalendarHelper.getPathFromDate(endTime, localStreamPath);
       //Path startPath = new Path(localStreamPath);
       // Date endTime = 
        LOG.info("AAAAAAAAAAAAAAAAAAAA get partitions : " + list.size());
        for (HCatPartition part : list) {
          LOG.info("AAAAaA : " + part.getLocation());
          
          Path path = new Path(part.getLocation());
          if (path.compareTo(startPath) >=0 && path.compareTo(endPath) <= 0) {
            LOG.info("AAAAAAAAA part location :   " + path);
            continue;
          } else {
            LOG.error("AAA EEEEEEEEEEEEEEEEEEEEErrorrrrrrrrrrrrrrr " + path + "    " + startPath + "    " + endPath);
          }
          
        }
      }
    } catch (HCatException e) {
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAa while trying to get the partitions " + e.getCause());
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      submitBack(hcatClient);
    }
  }

  public static void setHCatClient(String db) {
    // TODO Auto-generated method stub
    //hcatClient = client;
    dbName = db;
   // Conduit.setHcatDBName("dbName");
    //Conduit.
  }

}
