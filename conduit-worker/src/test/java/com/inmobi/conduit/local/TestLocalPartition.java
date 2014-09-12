package com.inmobi.conduit.local;

import java.io.IOException;
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

public class TestLocalPartition extends TestLocalStreamService {

  private static final Log LOG = LogFactory.getLog(TestLocalPartition.class);
   HCatClient hcatClient = null;
  static String dbName;
  //static String tableName;
  private Set<String> streamsToProcess = new HashSet<String>();
  public TestLocalPartition(ConduitConfig config, Cluster srcCluster,
      Cluster currentCluster, CheckpointProvider provider,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil) throws IOException {
    super(config, srcCluster, currentCluster, provider, streamsToProcess, hcatUtil);
    this.streamsToProcess = streamsToProcess;
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
        LOG.info("AAAAAAAAAAAAAAAAAAAA get partitions : " + list.size());
        for (HCatPartition part : list) {
          LOG.info("AAAAaA : " + part.getLocation());
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
