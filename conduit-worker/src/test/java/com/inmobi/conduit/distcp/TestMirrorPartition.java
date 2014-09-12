package com.inmobi.conduit.distcp;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.HCatClientUtil;

public class TestMirrorPartition extends TestMirrorStreamService {


  private static final Log LOG = LogFactory.getLog(TestMirrorPartition.class);
   HCatClient hcatClient = null;
  static String dbName;
  Set<String> streamsToProcess = new HashSet<String>();


  public TestMirrorPartition(ConduitConfig config, Cluster srcCluster,
      Cluster destinationCluster, Cluster currentCluster,
      Set<String> streamsToProcess, HCatClientUtil hcatUtil) throws Exception {
    super(config, srcCluster, destinationCluster, currentCluster, streamsToProcess, hcatUtil);
    this.streamsToProcess = streamsToProcess;
    // TODO Auto-generated constructor stub
  }

  @BeforeTest
  public void setup() {
    //Conduit.setHCatEnabled(true);
   // Conduit.setHcatDBName("test_conduit");
  }
  
  @AfterTest
  public void close() {
    
  }
  @Override
  protected void postExecute() throws InterruptedException {
    
    LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA post execute in mirror  stream parititons");
    try {
      hcatClient = getHCatClient();
      for (String stream : streamsToProcess) {
        String tableName = "conduit_" + stream;
        List<HCatPartition> list = hcatClient.getPartitions(dbName, tableName);
        LOG.info("AAAAAAAAAAAAAAAAAAAA get mirror  partitions : " + list.size());
        for (HCatPartition part : list) {
          LOG.info("AAAAaA : " + part.getLocation());
        }
      }
    } catch (HCatException e) {
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAa while trying to get the partitionsAAAAAAAA " + e.getCause());
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
